"""
Resource watchdog.

Samples memory and CPU once a second while a scan runs and:

  * pauses new work when free system memory drops below the low-water mark,
    and resumes once it recovers past a higher mark (hysteresis, so it does
    not flap);
  * aborts the scan if free memory stays below the critical mark for several
    samples in a row, before the OOM killer picks something at random;
  * tracks which file each worker is on, so a file that hangs a decoder shows
    up by name instead of the scan just looking frozen.

It never kills a worker. A thread stuck in a native decoder cannot be
interrupted from Python; the watchdog reports it and the scan continues on the
remaining threads.

psutil is optional. Without it the watchdog still tracks stalled files but
cannot see memory, and says so in its snapshot.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass

try:
    import psutil
except ImportError:  # pragma: no cover - exercised only on broken installs
    psutil = None

log = logging.getLogger(__name__)

MB = 1024 * 1024


@dataclass(frozen=True)
class Snapshot:
    rss: int = 0                 # this process, bytes
    available: int = 0           # system, bytes
    total: int = 0               # system, bytes
    cpu_percent: float = 0.0     # this process, across all cores
    paused: bool = False
    reason: str = ""
    stalled_path: str = ""
    stalled_seconds: float = 0.0
    in_flight: int = 0
    have_psutil: bool = psutil is not None


def default_thresholds(total_bytes):
    """(low, resume, critical) in bytes for a machine with `total_bytes` RAM."""
    low = max(512 * MB, int(total_bytes * 0.08))
    critical = max(256 * MB, int(total_bytes * 0.04))
    resume = int(low * 1.5)
    return low, resume, critical


class ResourceWatchdog:
    def __init__(self, interval=1.0, stall_seconds=120.0, critical_samples=5,
                 thresholds=None, memory_source=None):
        self.interval = interval
        self.stall_seconds = stall_seconds
        self.critical_samples = critical_samples
        self._thresholds = thresholds
        # Injected in tests; returns (available, total) in bytes.
        self._memory_source = memory_source

        self._gate = threading.Event()
        self._gate.set()
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._in_flight = {}
        self._reported_stalls = set()
        self._critical_run = 0
        self._thread = None
        self._process = psutil.Process(os.getpid()) if psutil else None
        self._snapshot = Snapshot()

        self.abort_reason = ""
        self.stalled_files = []      # (path, seconds) reported once each

    # -- lifecycle ----------------------------------------------------------

    def start(self):
        if self._thread is not None:
            return self
        if self._process is not None:
            try:
                self._process.cpu_percent(None)   # prime the counter
            except Exception:
                pass
        self._thread = threading.Thread(target=self._run, name="fdf-watchdog", daemon=True)
        self._thread.start()
        return self

    def stop(self):
        self._stop.set()
        self._gate.set()
        if self._thread is not None:
            self._thread.join(timeout=self.interval * 3)
            self._thread = None

    def __enter__(self):
        return self.start()

    def __exit__(self, *_exc):
        self.stop()

    # -- worker side --------------------------------------------------------

    @property
    def aborted(self):
        return bool(self.abort_reason)

    @property
    def paused(self):
        return not self._gate.is_set()

    def wait_if_paused(self, stop_signal=None, poll=0.25):
        """
        Block while paused. Returns False if the caller should stop instead of
        doing more work, either because the scan was cancelled or because the
        watchdog aborted it.
        """
        while not self._gate.wait(poll):
            if self.aborted or (stop_signal and stop_signal()):
                return False
        return not self.aborted

    @contextmanager
    def track(self, path):
        key = threading.get_ident()
        with self._lock:
            self._in_flight[key] = (path, time.monotonic())
        try:
            yield
        finally:
            with self._lock:
                self._in_flight.pop(key, None)

    # -- sampling -----------------------------------------------------------

    def snapshot(self):
        return self._snapshot

    def _memory(self):
        if self._memory_source is not None:
            return self._memory_source()
        if psutil is None:
            return 0, 0
        vm = psutil.virtual_memory()
        return vm.available, vm.total

    def sample(self):
        """Take one sample and apply the policy. Called by the thread; public
        so tests can drive it without timing."""
        available, total = self._memory()
        rss = cpu = 0
        if self._process is not None:
            try:
                rss = self._process.memory_info().rss
                cpu = self._process.cpu_percent(None)
            except Exception:
                pass

        reason = self._snapshot.reason if self.paused else ""
        if total:
            low, resume, critical = self._thresholds or default_thresholds(total)
            if available < critical:
                self._critical_run += 1
                if self._critical_run >= self.critical_samples and not self.abort_reason:
                    self.abort_reason = (
                        f"Stopped: free memory stayed below {critical // MB} MB "
                        f"({available // MB} MB left)."
                    )
                    log.warning(self.abort_reason)
                    self._gate.set()        # release waiters so they see the abort
            else:
                self._critical_run = 0

            if not self.abort_reason:
                if self._gate.is_set() and available < low:
                    reason = f"Paused: low memory ({available // MB} MB free)"
                    log.info(reason)
                    self._gate.clear()
                elif not self._gate.is_set() and available >= resume:
                    log.info("Resumed: %d MB free", available // MB)
                    reason = ""
                    self._gate.set()

        stalled_path, stalled_for = "", 0.0
        now = time.monotonic()
        with self._lock:
            in_flight = len(self._in_flight)
            for path, started in self._in_flight.values():
                age = now - started
                if age > stalled_for:
                    stalled_path, stalled_for = path, age
        if stalled_for < self.stall_seconds:
            stalled_path, stalled_for = "", 0.0
        elif stalled_path not in self._reported_stalls:
            self._reported_stalls.add(stalled_path)
            self.stalled_files.append((stalled_path, stalled_for))
            log.warning("Slow file (%.0fs so far): %s", stalled_for, stalled_path)

        self._snapshot = Snapshot(
            rss=rss, available=available, total=total, cpu_percent=cpu,
            paused=self.paused, reason=reason, stalled_path=stalled_path,
            stalled_seconds=stalled_for, in_flight=in_flight,
        )
        return self._snapshot

    def _run(self):
        while not self._stop.wait(self.interval):
            try:
                self.sample()
            except Exception:
                log.exception("Watchdog sample failed")


def lower_priority():
    """
    Drop this process to background priority so a scan does not make the
    desktop sluggish. On Linux and macOS this cannot be undone without root
    for the life of the process, which is fine for a scan.
    """
    try:
        if psutil is not None and os.name == 'nt':
            psutil.Process().nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)
        elif hasattr(os, 'nice'):
            current = os.nice(0)
            if current < 10:
                os.nice(10 - current)
        return True
    except Exception:
        log.debug("Could not lower process priority", exc_info=True)
        return False


def format_bytes(count):
    value = float(count)
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if value < 1024 or unit == "PB":
            return f"{value:.0f} {unit}" if unit in ("B", "KB") else f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} PB"

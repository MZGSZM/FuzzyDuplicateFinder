import threading
import time

from fdf.watchdog import MB, ResourceWatchdog, default_thresholds, format_bytes


class FakeMemory:
    def __init__(self, available, total=8000 * MB):
        self.available = available
        self.total = total

    def __call__(self):
        return self.available, self.total


def make(mem, **kw):
    return ResourceWatchdog(memory_source=mem, thresholds=(1000 * MB, 1500 * MB, 400 * MB), **kw)


def test_default_thresholds_scale_with_ram():
    low, resume, critical = default_thresholds(64 * 1024 * MB)
    assert critical < low < resume
    small_low, _, small_crit = default_thresholds(2 * 1024 * MB)
    assert small_low == 512 * MB and small_crit == 256 * MB


def test_pause_and_resume_with_hysteresis():
    mem = FakeMemory(4000 * MB)
    dog = make(mem)
    dog.sample()
    assert not dog.paused
    mem.available = 900 * MB
    snap = dog.sample()
    assert dog.paused and "low memory" in snap.reason
    mem.available = 1200 * MB          # above low, below resume: stay paused
    dog.sample()
    assert dog.paused
    mem.available = 1600 * MB
    dog.sample()
    assert not dog.paused


def test_abort_after_sustained_critical():
    mem = FakeMemory(300 * MB)
    dog = make(mem, critical_samples=3)
    dog.sample()
    dog.sample()
    assert not dog.aborted
    dog.sample()
    assert dog.aborted and "free memory" in dog.abort_reason
    # Waiters are released so they can see the abort.
    assert dog.wait_if_paused() is False


def test_brief_critical_dip_does_not_abort():
    mem = FakeMemory(300 * MB)
    dog = make(mem, critical_samples=3)
    dog.sample()
    dog.sample()
    mem.available = 4000 * MB
    dog.sample()
    mem.available = 300 * MB
    dog.sample()
    assert not dog.aborted


def test_wait_if_paused_honours_stop_signal():
    mem = FakeMemory(500 * MB)
    dog = make(mem)
    dog.sample()
    assert dog.paused
    started = time.monotonic()
    assert dog.wait_if_paused(stop_signal=lambda: True, poll=0.01) is False
    assert time.monotonic() - started < 1


def test_wait_if_paused_returns_on_resume():
    mem = FakeMemory(500 * MB)
    dog = make(mem)
    dog.sample()
    result = []
    waiter = threading.Thread(target=lambda: result.append(dog.wait_if_paused(poll=0.01)))
    waiter.start()
    time.sleep(0.05)
    mem.available = 4000 * MB
    dog.sample()
    waiter.join(2)
    assert result == [True]


def test_stall_detection_reports_once():
    dog = ResourceWatchdog(memory_source=lambda: (0, 0), stall_seconds=0.05)
    with dog.track("/slow/file.mkv"):
        time.sleep(0.1)
        snap = dog.sample()
        dog.sample()
    assert snap.stalled_path == "/slow/file.mkv"
    assert [p for p, _ in dog.stalled_files] == ["/slow/file.mkv"]
    assert dog.sample().stalled_path == ""


def test_thread_lifecycle():
    with ResourceWatchdog(interval=0.01) as dog:
        time.sleep(0.05)
        snap = dog.snapshot()
    assert snap.rss > 0 or not snap.have_psutil


def test_format_bytes():
    assert format_bytes(0) == "0 B"
    assert format_bytes(2048) == "2 KB"
    assert format_bytes(5 * 1024 * MB) == "5.0 GB"

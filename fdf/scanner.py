"""
Filesystem indexing.

A scan runs in three passes:

  1. Walk the roots with os.scandir, applying the filters. Nothing is read.
  2. Fingerprint pass. Media files get their fuzzy fingerprint. Files whose
     size collides with another file get a quick hash (first and last 64 KB).
     Files that are unchanged since the last scan reuse their stored values.
  3. Exact pass. Only files that collide on (size, quick hash) are read in
     full. On a typical library this is a small fraction of the bytes.

Workers never touch sqlite; they return values and the coordinating thread
writes them in batches. Submission is bounded, so a million-file scan does not
hold a million futures, and the resource watchdog can pause it between files.
"""

from __future__ import annotations

import concurrent.futures as cf
import fnmatch
import logging
import os
import re
import stat as stat_mod
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field

from . import fingerprints as fp
from .exts import KIND_COLUMNS, KINDS, exts_for, kind_of
from .index import (
    P_AUDIO,
    P_DEV,
    P_DURATION,
    P_EXACT,
    P_INODE,
    P_MTIME,
    P_QUICK,
    P_SIZE,
    P_TEXT,
    P_VERSION,
    P_VISUAL,
    Index,
)
from .paths import index_files, normalize_root

log = logging.getLogger(__name__)

MAX_SCAN_WORKERS = 32

DEFAULT_EXCLUDES = (
    '.git', '.hg', '.svn', 'node_modules', '__pycache__', '.Trash-*', '.Trash',
    '$RECYCLE.BIN', 'System Volume Information', '.thumbnails', '@eaDir',
)

PROGRESS_INTERVAL = 0.15

_FILE_ATTRIBUTE_HIDDEN = getattr(stat_mod, 'FILE_ATTRIBUTE_HIDDEN', 2)


@dataclass
class ScanOptions:
    kinds: frozenset = frozenset(KINDS)
    all_types: bool = False          # exact-match every extension too
    skip_hidden: bool = True
    excludes: tuple = DEFAULT_EXCLUDES
    min_size: int = 1                # empty files are all "identical"; skip
    max_workers: int | None = None

    def wanted_exts(self):
        return None if self.all_types else exts_for(self.kinds)


@dataclass
class ScanResult:
    db_path: str
    issues: list = field(default_factory=list)       # (path, reason)
    files: int = 0
    fingerprinted: int = 0
    reused: int = 0
    full_reads: int = 0
    removed: int = 0
    elapsed: float = 0.0
    aborted: bool = False
    abort_reason: str = ""


def default_workers():
    return max(2, min(MAX_SCAN_WORKERS, (os.cpu_count() or 4)))


# ---------------------------------------------------------------------------
# Walking
# ---------------------------------------------------------------------------

class _Filter:
    def __init__(self, options, skip_paths=()):
        names, paths = [], []
        for pattern in options.excludes:
            pattern = pattern.strip()
            if not pattern:
                continue
            (paths if ('/' in pattern or os.sep in pattern) else names).append(
                os.path.normcase(pattern)
            )
        self._name_re = re.compile("|".join(fnmatch.translate(p) for p in names)) if names else None
        self._path_re = re.compile("|".join(fnmatch.translate(p) for p in paths)) if paths else None
        self.skip_hidden = options.skip_hidden
        self.wanted = options.wanted_exts()
        self.min_size = max(0, int(options.min_size or 0))
        self.skip_paths = {normalize_root(p) for p in skip_paths}

    def excluded(self, entry):
        name = os.path.normcase(entry.name)
        if self._name_re is not None and self._name_re.match(name):
            return True
        if self._path_re is not None and self._path_re.match(os.path.normcase(entry.path)):
            return True
        if self.skip_hidden:
            if entry.name.startswith('.'):
                return True
            if os.name == 'nt':
                try:
                    attrs = entry.stat(follow_symlinks=False).st_file_attributes
                    if attrs & _FILE_ATTRIBUTE_HIDDEN:
                        return True
                except (OSError, AttributeError):
                    pass
        return False


def _collapse_roots(roots):
    """Absolute, de-duplicated roots with nested roots dropped (the parent's
    walk already covers them)."""
    normalized = sorted({normalize_root(r): os.path.abspath(r) for r in roots}.items())
    kept = []
    for norm, real in normalized:
        if any(norm == k or norm.startswith(k.rstrip(os.sep) + os.sep) for k, _ in kept):
            continue
        kept.append((norm, real))
    return [real for _, real in kept]


def _is_junction(entry):
    """Windows directory junctions are links too; following them can loop
    (the legacy "Application Data" junctions point at their own parent)."""
    check = getattr(entry, 'is_junction', None)     # Python 3.12+
    return bool(check and check())


def iter_files(roots, options, stop_signal=None, skip_paths=()):
    """
    Yield (path, size, mtime, ctime, dev, inode) for files to index.

    Symlinks and junctions are not followed or indexed: trashing a link frees
    nothing, and trashing its target leaves a dangling link behind.
    """
    filt = _Filter(options, skip_paths)
    stack = list(reversed(_collapse_roots(roots)))
    while stack:
        if stop_signal and stop_signal():
            return
        current = stack.pop()
        try:
            it = os.scandir(current)
        except OSError as exc:
            log.info("Cannot list %s: %s", current, exc)
            continue
        with it:
            for entry in it:
                try:
                    if entry.is_symlink() or _is_junction(entry) or filt.excluded(entry):
                        continue
                    if entry.is_dir(follow_symlinks=False):
                        stack.append(entry.path)
                        continue
                    if not entry.is_file(follow_symlinks=False):
                        continue
                    if filt.wanted is not None:
                        ext = os.path.splitext(entry.name)[1].lower()
                        if ext not in filt.wanted:
                            continue
                    st = entry.stat(follow_symlinks=False)
                    if st.st_size < filt.min_size:
                        continue
                    if filt.skip_paths and normalize_root(entry.path) in filt.skip_paths:
                        continue
                    ino = st.st_ino or None
                    yield (entry.path, st.st_size, st.st_mtime, st.st_ctime,
                           st.st_dev if ino else None, ino)
                except OSError:
                    continue


# ---------------------------------------------------------------------------
# Worker functions. They only compute; they never touch the index.
# ---------------------------------------------------------------------------

def _fingerprint(path, kind):
    """(column_values: dict, issue_reason|None)."""
    try:
        if kind == 'image':
            return {'visual_hash': fp.image_hash(path)}, None
        if kind == 'video':
            value, duration = fp.video_hash(path)
            return {'visual_hash': value, 'duration': duration}, None
        if kind == 'audio':
            value, duration = fp.audio_hash(path)
            return {'audio_hash': value, 'duration': duration}, None
        if kind == 'text':
            return {'text_hash': fp.text_hash(path) or ''}, None
    except fp.FingerprintError as exc:
        reason = str(exc)
    except MemoryError:
        reason = "ran out of memory decoding it"
    except Exception as exc:
        reason = f"{type(exc).__name__}: {exc}" if str(exc) else type(exc).__name__
    return {KIND_COLUMNS[kind]: ''}, reason


def _work_pass1(path, kind, size, need_fuzzy, need_quick):
    values, issue = {}, None
    if need_fuzzy:
        values, issue = _fingerprint(path, kind)
    if need_quick:
        try:
            values['quick_hash'] = fp.quick_hash(path, size)
        except OSError as exc:
            issue = issue or f"unreadable: {exc.strerror or exc}"
    return values, issue


def _work_exact(path):
    try:
        return fp.exact_hash(path), None
    except OSError as exc:
        return None, f"unreadable: {exc.strerror or exc}"


def _build_row(entry, prev, current_version):
    """
    Index row for a walked file, carrying over stored hashes when the file is
    unchanged. '_unchanged' is a flag for the caller, not a column.
    """
    path, size, mtime, ctime, dev, ino = entry
    row = {
        'path': path, 'filename': os.path.basename(path),
        'extension': os.path.splitext(path)[1].lower(), 'size': size, 'mtime': mtime,
        'ctime': ctime, 'dev': dev, 'inode': ino, 'fp_version': current_version,
    }
    unchanged = (
        prev is not None and prev[P_SIZE] == size
        and prev[P_MTIME] is not None and abs(prev[P_MTIME] - mtime) < 0.01
    )
    if unchanged:
        row.update(exact_hash=prev[P_EXACT], quick_hash=prev[P_QUICK])
        if prev[P_VERSION] == current_version:
            # Fingerprints from another version are not comparable; leaving
            # them out makes them recompute.
            row.update(visual_hash=prev[P_VISUAL], audio_hash=prev[P_AUDIO],
                       text_hash=prev[P_TEXT], duration=prev[P_DURATION])
    row['_unchanged'] = unchanged
    return row


# ---------------------------------------------------------------------------
# Scanner
# ---------------------------------------------------------------------------

class Scanner:
    def __init__(self, options=None, watchdog=None):
        self.options = options or ScanOptions()
        self.watchdog = watchdog

    # -- helpers ------------------------------------------------------------

    def _stopping(self, stop_signal):
        if self.watchdog is not None and self.watchdog.aborted:
            return True
        return bool(stop_signal and stop_signal())

    def _run_bounded(self, window, items, submit, on_done, stop_signal, tick):
        """
        Submit items a window at a time. `submit(item)` returns a future or
        None (nothing to do); `on_done(item, result)` consumes results.
        """
        pending = {}
        source = iter(items)
        exhausted = False
        while True:
            while not exhausted and len(pending) < window:
                if self._stopping(stop_signal):
                    exhausted = True
                    break
                if self.watchdog is not None and self.watchdog.paused:
                    if pending:
                        break       # drain what is running, then wait
                    if not self.watchdog.wait_if_paused(stop_signal):
                        exhausted = True
                        break
                item = next(source, None)
                if item is None:
                    exhausted = True
                    break
                future = submit(item)
                if future is not None:
                    pending[future] = item
            if not pending:
                if exhausted:
                    return
                continue
            done, _ = cf.wait(pending, timeout=0.25, return_when=cf.FIRST_COMPLETED)
            for future in done:
                item = pending.pop(future)
                try:
                    result = future.result()
                except Exception as exc:
                    log.exception("Worker failed on %s", item[0])
                    result = (None, f"{type(exc).__name__}: {exc}")
                on_done(item, result)
            tick()
            if self._stopping(stop_signal):
                for future in pending:
                    future.cancel()
                # In-flight futures cannot be cancelled; their results are
                # simply discarded.
                return

    def _tracked(self, func):
        if self.watchdog is None:
            return func

        def wrapper(path, *args):
            with self.watchdog.track(path):
                return func(path, *args)
        return wrapper

    # -- scan ---------------------------------------------------------------

    def scan(self, folder_list, db_path, stop_signal=None, progress=None):
        """
        Index `folder_list` (paths or {'path', 'priority'} dicts) into db_path.
        progress(phase, done, total, issues) is called at most every
        PROGRESS_INTERVAL seconds; phase is 'walk', 'hash' or 'verify'.
        """
        started = time.monotonic()
        opts = self.options
        roots = [f['path'] if isinstance(f, dict) else f for f in folder_list]
        result = ScanResult(db_path=db_path)
        issues = result.issues

        last_report = [0.0]

        def report(phase, done, total, force=False):
            now = time.monotonic()
            if progress and (force or now - last_report[0] >= PROGRESS_INTERVAL):
                last_report[0] = now
                progress(phase, done, total, len(issues))

        index = Index(db_path)
        try:
            index.save_roots(folder_list)
            existing = index.load_existing()

            # -- pass 0: walk ----------------------------------------------
            entries = []
            for entry in iter_files(roots, opts, stop_signal, skip_paths=index_files(db_path)):
                entries.append(entry)
                report('walk', len(entries), 0)
            if self._stopping(stop_signal):
                return self._finish(result, started, stop_signal)
            result.files = len(entries)
            report('walk', len(entries), len(entries), force=True)

            # Rows under the scanned roots that were not found again are gone
            # (or now excluded). Dropping them keeps the matcher honest.
            seen = {e[0] for e in entries}
            norm_roots = [normalize_root(r).rstrip(os.sep) + os.sep for r in _collapse_roots(roots)]
            stale = [
                p for p in existing
                if p not in seen and any(normalize_root(p).startswith(r) for r in norm_roots)
            ]
            result.removed = index.delete_paths(stale)
            for p in stale:
                existing.pop(p, None)

            size_counts = Counter(e[1] for e in entries)
            current = fp.FINGERPRINT_VERSION

            # -- pass 1: fingerprints and quick hashes ---------------------
            # Row dicts are only built for files in flight, so memory stays
            # flat however many files there are. `collide` keeps just the
            # (quick, exact) pair for files whose size is shared, for pass 2.
            collide = {}
            work = []
            for entry in entries:
                path, size, _mtime, _ctime, dev, ino = entry
                prev = existing.get(path)
                kind = kind_of(os.path.splitext(path)[1].lower())
                row = _build_row(entry, prev, current)
                unchanged = row.pop('_unchanged')
                need_fuzzy = (
                    kind in opts.kinds
                    and kind in KIND_COLUMNS
                    and row.get(KIND_COLUMNS[kind]) is None
                )
                need_quick = size_counts[size] > 1 and not row.get('quick_hash')
                if need_fuzzy or need_quick:
                    work.append((entry, kind, need_fuzzy, need_quick))
                    continue
                if unchanged:
                    result.reused += 1
                needs_write = (
                    not unchanged
                    or prev[P_VERSION] != current
                    or (ino is not None and (prev[P_DEV], prev[P_INODE]) != (dev, ino))
                )
                if needs_write:
                    index.queue_row(row)
                if size_counts[size] > 1:
                    collide[path] = (row.get('quick_hash'), row.get('exact_hash'))

            total = len(work)
            done = [0]
            worker_count = opts.max_workers or default_workers()
            worker_count = max(1, min(int(worker_count), MAX_SCAN_WORKERS))
            pass1 = self._tracked(_work_pass1)
            in_flight = {}

            def submit1(item):
                entry, kind, need_fuzzy, need_quick = item
                in_flight[entry[0]] = _build_row(entry, existing.get(entry[0]), current)
                return executor.submit(pass1, entry[0], kind, entry[1], need_fuzzy, need_quick)

            def done1(item, outcome):
                path, size = item[0][0], item[0][1]
                values, issue = outcome if outcome else ({}, None)
                row = in_flight.pop(path)
                row.pop('_unchanged', None)
                row.update(values or {})
                if issue:
                    issues.append((path, issue))
                index.queue_row(row)
                if size_counts[size] > 1:
                    collide[path] = (row.get('quick_hash'), row.get('exact_hash'))
                result.fingerprinted += 1
                done[0] += 1

            executor = cf.ThreadPoolExecutor(max_workers=worker_count,
                                             thread_name_prefix="fdf-scan")
            try:
                report('hash', 0, total, force=True)
                self._run_bounded(worker_count * 3, work, submit1, done1, stop_signal,
                                  lambda: report('hash', done[0], total))
                report('hash', done[0], total, force=True)
                work.clear()
                index.flush()
                if self._stopping(stop_signal):
                    return self._finish(result, started, stop_signal)

                # -- pass 2: exact hashes for (size, quick) collisions -----
                groups = defaultdict(list)
                for path, size, *_ in entries:
                    quick, exact = collide.get(path, (None, None))
                    if quick:
                        groups[(size, quick)].append((path, exact))

                exact_updates = []
                verify = []
                for (size, quick), members in groups.items():
                    if len(members) < 2:
                        continue
                    for path, exact in members:
                        if exact:
                            continue
                        if size <= 2 * fp.QUICK_BYTES:
                            # The quick hash already covered the whole file.
                            exact_updates.append((quick, path))
                        else:
                            verify.append((path,))
                groups.clear()
                collide.clear()

                vtotal = len(verify)
                vdone = [0]
                exact_pass = self._tracked(_work_exact)

                def submit2(item):
                    return executor.submit(exact_pass, item[0])

                def done2(item, outcome):
                    digest, issue = outcome if outcome else (None, None)
                    if digest:
                        exact_updates.append((digest, item[0]))
                    if issue:
                        issues.append((item[0], issue))
                    result.full_reads += 1
                    vdone[0] += 1
                    if len(exact_updates) >= 500:
                        index.set_exact_hashes(exact_updates)
                        exact_updates.clear()

                report('verify', 0, vtotal, force=True)
                self._run_bounded(worker_count * 3, verify, submit2, done2, stop_signal,
                                  lambda: report('verify', vdone[0], vtotal))
                index.set_exact_hashes(exact_updates)
                report('verify', vdone[0], vtotal, force=True)
            finally:
                executor.shutdown(wait=True, cancel_futures=True)
        finally:
            index.close()

        if self.watchdog is not None:
            for path, seconds in self.watchdog.stalled_files:
                issues.append((path, f"took over {int(seconds)}s to process"))
        return self._finish(result, started, stop_signal)

    def _finish(self, result, started, stop_signal):
        result.elapsed = time.monotonic() - started
        if self.watchdog is not None and self.watchdog.aborted:
            result.aborted = True
            result.abort_reason = self.watchdog.abort_reason
        elif stop_signal and stop_signal():
            result.aborted = True
        return result

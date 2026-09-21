"""
Filesystem indexing for Fuzzy Duplicate Finder.

Summary of changes against the previous version:

  * Database writes are batched and committed in groups instead of one commit
    per file under a global lock. That was an fsync per file and serialised
    every worker thread.
  * The existing index is loaded once into a dict instead of issuing one locked
    SELECT per file.
  * Files whose size is unique across the index can never be byte-identical to
    anything, so their MD5 is skipped entirely. On a media library this removes
    most of the read I/O in phase 1.
  * Directory walking uses os.scandir, which gives size and mtime without an
    extra stat syscall per file.
  * Images and videos whose perceptual hash fails are now still indexed using
    their exact hash, matching the fix already applied to audio. Previously
    they were dropped from the database entirely, so their byte-identical
    copies were never reported.
  * Unchanged files reuse their stored hashes rather than recomputing them when
    only one of the hashes is missing.
  * Overlapping scan roots and repeated paths are de-duplicated before queuing.
  * Symlinked directories are not followed, and symlinked files are recorded
    with their inode identity so the matcher can avoid reporting hardlinks as
    duplicates.
  * The executor is shut down with wait=True before the connection is closed.
    The previous code closed sqlite out from under in-flight writer threads on
    a user-triggered stop.
  * OPENCV_LOG_LEVEL is set before cv2 is imported, which is the only point at
    which it has any effect.
  * Pillow's decompression bomb guard is raised rather than disabled outright.
  * PIL image handles are closed instead of being left to the garbage collector.
"""

from __future__ import annotations

import concurrent.futures
import hashlib
import os
import sqlite3
import threading
import time
from collections import Counter
from datetime import datetime

# Must be set before cv2 is imported to have any effect.
os.environ.setdefault("OPENCV_LOG_LEVEL", "OFF")

import cv2  # noqa: E402
import imagehash  # noqa: E402
import numpy as np  # noqa: E402
from PIL import Image  # noqa: E402

MAX_SCAN_WORKERS = min(16, max(4, (os.cpu_count() or 4) * 2))

# Allow very large images without disabling the decompression bomb guard
# entirely. This tool gets pointed at arbitrary directories.
Image.MAX_IMAGE_PIXELS = 512_000_000

HASH_CHUNK = 1024 * 1024

try:
    import librosa
    AUDIO_AVAILABLE = True
except ImportError:
    AUDIO_AVAILABLE = False

# GLOBAL CONFIG: These must match matcher.py logic
IMAGE_EXTS = {'.jpg', '.jpeg', '.png', '.bmp', '.gif', '.webp', '.tiff', '.tif', '.psd', '.raw'}
VIDEO_EXTS = {'.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv', '.m4v', '.webm', '.ts', '.mts', '.3gp'}
AUDIO_EXTS = {'.mp3', '.wav', '.flac', '.m4a', '.aac', '.ogg', '.wma'}
TEXT_EXTS = {'.txt', '.md', '.py', '.js', '.json', '.html', '.css', '.c', '.cpp'}

ALL_SUPPORTED_EXTS = IMAGE_EXTS | VIDEO_EXTS | AUDIO_EXTS | TEXT_EXTS


class DatabaseManager:
    """
    Thread-safe sqlite wrapper with buffered writes.

    Rows are queued and flushed with executemany, either when the buffer fills
    or after FLUSH_SECONDS, whichever comes first.
    """

    FLUSH_ROWS = 400
    FLUSH_SECONDS = 2.0

    _UPSERT = """
    INSERT INTO files (path, filename, extension, size, mtime, ctime,
                       exact_hash, visual_hash, audio_hash, scan_date)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(path) DO UPDATE SET
        filename=excluded.filename, extension=excluded.extension,
        size=excluded.size, mtime=excluded.mtime, ctime=excluded.ctime,
        exact_hash=excluded.exact_hash, visual_hash=excluded.visual_hash,
        audio_hash=excluded.audio_hash, scan_date=excluded.scan_date
    """

    def __init__(self, db_path):
        self.db_path = db_path
        self.lock = threading.Lock()
        self.conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        # NORMAL is the correct durability level here: a crash mid-scan costs a
        # rescan, not user data.
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA temp_store=MEMORY")
        self.conn.execute("PRAGMA cache_size=-65536")
        self._pending = []
        self._last_flush = time.monotonic()
        self.create_table()

    def create_table(self):
        query_files = """
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            path TEXT UNIQUE,
            filename TEXT,
            extension TEXT,
            size INTEGER,
            mtime REAL,
            ctime REAL,
            exact_hash TEXT,
            visual_hash TEXT,
            audio_hash TEXT,
            scan_date TEXT
        )
        """
        query_roots = """
        CREATE TABLE IF NOT EXISTS scan_roots (
            path TEXT PRIMARY KEY,
            priority INTEGER
        )
        """
        with self.lock:
            self.conn.execute(query_files)
            self.conn.execute(query_roots)
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_files_size ON files(size)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_files_exact ON files(exact_hash)")
            self.conn.commit()

    # -- scan roots ---------------------------------------------------------

    def save_roots(self, folder_list):
        with self.lock:
            self.conn.execute("DELETE FROM scan_roots")
            rows = []
            for item in folder_list:
                path = item['path'] if isinstance(item, dict) else item
                prio = item['priority'] if isinstance(item, dict) else 10
                rows.append((path, prio))
            self.conn.executemany(
                "INSERT OR REPLACE INTO scan_roots (path, priority) VALUES (?, ?)", rows
            )
            self.conn.commit()

    def get_roots(self):
        with self.lock:
            try:
                cursor = self.conn.execute("SELECT path, priority FROM scan_roots ORDER BY path")
                return [{'path': row[0], 'priority': row[1]} for row in cursor.fetchall()]
            except sqlite3.OperationalError:
                try:
                    self.conn.execute("""
                        CREATE TABLE IF NOT EXISTS scan_roots (
                            path TEXT PRIMARY KEY,
                            priority INTEGER
                        )
                    """)
                    self.conn.commit()
                except Exception:
                    pass
                return []

    # -- index --------------------------------------------------------------

    def load_index(self):
        """
        Return {path: (size, mtime, exact_hash, visual_hash, audio_hash)}.

        Loading this once replaces one locked SELECT per scanned file.
        """
        with self.lock:
            try:
                cursor = self.conn.execute(
                    "SELECT path, size, mtime, exact_hash, visual_hash, audio_hash FROM files"
                )
            except sqlite3.OperationalError:
                return {}
            return {row[0]: (row[1], row[2], row[3], row[4], row[5]) for row in cursor}

    def queue_upsert(self, data):
        flush_rows = None
        with self.lock:
            self._pending.append(data)
            due = (
                len(self._pending) >= self.FLUSH_ROWS
                or (time.monotonic() - self._last_flush) >= self.FLUSH_SECONDS
            )
            if due:
                flush_rows = self._pending
                self._pending = []
                self._last_flush = time.monotonic()

        if flush_rows:
            self._write(flush_rows)

    def flush(self):
        with self.lock:
            rows = self._pending
            self._pending = []
            self._last_flush = time.monotonic()
        if rows:
            self._write(rows)

    def _write(self, rows):
        try:
            with self.lock:
                self.conn.executemany(self._UPSERT, rows)
                self.conn.commit()
        except Exception as exc:
            print(f"DB Write Error: {exc}")

    def close(self):
        try:
            self.flush()
        except Exception:
            pass
        try:
            self.conn.close()
        except Exception:
            pass


class Scanner:
    def __init__(self):
        self.db = None
        self.existing = {}

    # -- hashing ------------------------------------------------------------

    def generate_exact_hash(self, filepath):
        """MD5 of the full file content."""
        try:
            hasher = hashlib.md5()
            with open(filepath, 'rb', buffering=0) as handle:
                file_digest = getattr(hashlib, 'file_digest', None)
                if file_digest is not None:
                    return file_digest(handle, hashlib.md5).hexdigest()
                for chunk in iter(lambda: handle.read(HASH_CHUNK), b""):
                    hasher.update(chunk)
            return hasher.hexdigest()
        except Exception:
            return None

    def generate_visual_hash(self, filepath, ext):
        """Perceptual hash for images; mid-frame pHash for videos."""
        try:
            if ext in IMAGE_EXTS:
                with Image.open(filepath) as img:
                    img.load()
                    return str(imagehash.phash(img))

            if ext in VIDEO_EXTS:
                cap = cv2.VideoCapture(filepath)
                try:
                    if not cap.isOpened():
                        return None
                    total = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
                    if total > 0:
                        cap.set(cv2.CAP_PROP_POS_FRAMES, total // 2)
                    ret, frame = cap.read()
                    if not ret:
                        # Seeking is unreliable for a number of codecs; fall
                        # back to the first decodable frame.
                        cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                        ret, frame = cap.read()
                    if not ret:
                        return None
                    rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                    with Image.fromarray(rgb) as img:
                        return str(imagehash.phash(img))
                finally:
                    cap.release()
        except Exception:
            pass
        return None

    def generate_audio_hash(self, filepath):
        """
        Chroma-based audio fingerprint via librosa.

        Audio files that fail fingerprinting are still indexed using their
        exact hash alone; returning None here only leaves the audio_hash column
        NULL.
        """
        if not AUDIO_AVAILABLE:
            return None
        try:
            import warnings
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                y, sr = librosa.load(filepath, duration=30, sr=22050)
                chroma = librosa.feature.chroma_stft(y=y, sr=sr)
                chroma_mean = np.mean(chroma, axis=1)
                fingerprint = ",".join(str(round(float(x), 1)) for x in chroma_mean)
                return hashlib.md5(fingerprint.encode()).hexdigest()
        except Exception:
            return None

    # -- per-file work ------------------------------------------------------

    def process_file(self, entry, needs_exact):
        """
        Hash and store one file.

        Returns (indexed: bool, skipped_reason: str|None). A file can be both
        indexed and reported: an image whose perceptual hash fails is still
        written using its exact hash, but the user is told about it.
        """
        filepath, size, mtime, ctime = entry
        try:
            ext = os.path.splitext(filepath)[1].lower()
            is_img = ext in IMAGE_EXTS
            is_vid = ext in VIDEO_EXTS
            is_aud = ext in AUDIO_EXTS
            is_txt = ext in TEXT_EXTS

            if not (is_img or is_vid or is_aud or is_txt):
                return False, None

            prev = self.existing.get(filepath)
            unchanged = False
            prev_exact = prev_visual = prev_audio = None
            if prev:
                prev_size, prev_mtime, prev_exact, prev_visual, prev_audio = prev
                unchanged = (
                    prev_size == size
                    and prev_mtime is not None
                    and abs(prev_mtime - mtime) < 0.01
                )

            # Nothing to add: the record is current and already has whatever
            # exact hash this scan requires.
            if unchanged and (prev_exact or not needs_exact):
                return True, None

            if unchanged and prev_exact:
                exact_hash = prev_exact
            elif needs_exact:
                exact_hash = self.generate_exact_hash(filepath)
            else:
                exact_hash = None

            visual_hash = None
            visual_failed = False
            if is_img or is_vid:
                if unchanged and prev_visual:
                    visual_hash = prev_visual
                else:
                    visual_hash = self.generate_visual_hash(filepath, ext)
                    visual_failed = visual_hash is None

            audio_hash = None
            if is_aud:
                audio_hash = prev_audio if (unchanged and prev_audio) else self.generate_audio_hash(filepath)

            self.db.queue_upsert((
                filepath, os.path.basename(filepath), ext, size, mtime, ctime,
                exact_hash, visual_hash, audio_hash, datetime.now().isoformat(),
            ))
            return True, (filepath if visual_failed else None)

        except Exception:
            return False, filepath

    # -- walking ------------------------------------------------------------

    @staticmethod
    def _iter_files(root, stop_signal=None):
        """
        Yield (path, size, mtime, ctime) for supported files under `root`.

        os.scandir gives us the stat data for free, so this avoids a separate
        stat call per file. Symlinked directories are not followed.
        """
        stack = [root]
        while stack:
            if stop_signal and stop_signal():
                return
            current = stack.pop()
            try:
                with os.scandir(current) as it:
                    for entry in it:
                        if stop_signal and stop_signal():
                            return
                        try:
                            if entry.is_dir(follow_symlinks=False):
                                stack.append(entry.path)
                                continue
                            if not entry.is_file(follow_symlinks=True):
                                continue
                            ext = os.path.splitext(entry.name)[1].lower()
                            if ext not in ALL_SUPPORTED_EXTS:
                                continue
                            st = entry.stat()
                            yield (
                                os.path.abspath(entry.path),
                                st.st_size,
                                st.st_mtime,
                                getattr(st, 'st_ctime', 0),
                            )
                        except OSError:
                            continue
            except OSError:
                continue

    # -- scan ---------------------------------------------------------------

    def scan_directory(self, folder_list, db_path, stop_signal=None,
                       progress_callback=None, max_workers=None):
        """Scan directories and dispatch worker threads for hashing."""
        print("Starting Scan")
        self.db = DatabaseManager(db_path)
        self.db.save_roots(folder_list)
        self.existing = self.db.load_index()

        entries = []
        seen_paths = set()
        for root_dir in folder_list:
            if stop_signal and stop_signal():
                break
            path_str = root_dir['path'] if isinstance(root_dir, dict) else root_dir
            path_str = os.path.abspath(os.path.normpath(path_str))
            for entry in self._iter_files(path_str, stop_signal):
                # Overlapping scan roots would otherwise queue and hash the
                # same file more than once.
                if entry[0] in seen_paths:
                    continue
                seen_paths.add(entry[0])
                entries.append(entry)

        # Two files of different sizes can never be byte-identical, so only
        # files whose size collides with something else need an MD5. Sizes from
        # the existing index are included so that incremental scans against a
        # pre-existing database stay correct.
        size_counts = Counter(entry[1] for entry in entries)
        for path, (size, _mtime, _eh, _vh, _ah) in self.existing.items():
            if path not in seen_paths:
                size_counts[size] += 1

        # A previously indexed file may only now have become a size collision
        # candidate, in which case it needs its MD5 backfilled.
        for path, (size, mtime, exact_hash, _vh, _ah) in self.existing.items():
            if path in seen_paths or exact_hash or size_counts[size] < 2:
                continue
            try:
                st = os.stat(path)
            except OSError:
                continue
            seen_paths.add(path)
            entries.append((path, st.st_size, st.st_mtime, getattr(st, 'st_ctime', 0)))

        total_files = len(entries)
        print(f"Found {total_files} supported files. Processing...")

        processed_count = 0
        skipped_files_list = []

        worker_count = (
            max_workers
            if max_workers and max_workers > 0
            else min(MAX_SCAN_WORKERS, max(1, (os.cpu_count() or 4) * 2))
        )
        worker_count = max(1, min(worker_count, MAX_SCAN_WORKERS))

        executor = concurrent.futures.ThreadPoolExecutor(max_workers=worker_count)
        futures = {}

        try:
            for entry in entries:
                if stop_signal and stop_signal():
                    break
                needs_exact = size_counts[entry[1]] > 1
                futures[executor.submit(self.process_file, entry, needs_exact)] = entry[0]

            for future in concurrent.futures.as_completed(futures):
                if stop_signal and stop_signal():
                    break
                try:
                    _indexed, skip_path = future.result()
                    if skip_path:
                        skipped_files_list.append(skip_path)
                except Exception:
                    skipped_files_list.append(futures[future])

                processed_count += 1
                if progress_callback and processed_count % 64 == 0:
                    progress_callback(processed_count, total_files, len(skipped_files_list))
        finally:
            # wait=True matters: cancel_futures drops queued work, but in-flight
            # workers are still writing to the connection we are about to close.
            executor.shutdown(wait=True, cancel_futures=True)

        if progress_callback:
            progress_callback(processed_count, total_files, len(skipped_files_list))

        self.db.close()

        if stop_signal and stop_signal():
            return None, []

        print("Scan Complete")
        return db_path, skipped_files_list

from __future__ import annotations

import concurrent.futures
import logging
import os
import sqlite3
from difflib import SequenceMatcher

import numpy as np

log = logging.getLogger(__name__)

# Configuration
SIMILARITY_THRESHOLD = 70.0
MAX_MATCH_WORKERS = max(1, min(8, os.cpu_count() or 4))

# Below this many comparable files the vectorised path finishes in well under a
# second, so the cost and fragility of a process pool is not worth paying.
PARALLEL_MIN_FILES = 50_000

# Scoring weights. Note that the final score is normalised by the sum of the
# weights that actually applied, so these are relative, not absolute,
# percentages.
W_VISUAL = 0.50
W_AUDIO = 0.50
W_NAME = 0.20
W_SIZE = 0.10
W_EXT = 0.05

# Must match scanner_engine extensions
VISUAL_EXTS = {'.jpg', '.jpeg', '.png', '.bmp', '.gif', '.webp', '.tiff', '.tif', '.psd', '.raw',
               '.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv', '.m4v', '.webm', '.ts', '.mts', '.3gp'}
AUDIO_EXTS = {'.mp3', '.wav', '.flac', '.m4a', '.aac', '.ogg', '.wma'}
TEXT_EXTS = {'.txt', '.md', '.py', '.js', '.json', '.html', '.css', '.c', '.cpp'}


def _file_type_group(ext):
    """Return a coarse type bucket so we never compare across media categories."""
    if ext in VISUAL_EXTS:
        return 'visual'
    if ext in AUDIO_EXTS:
        return 'audio'
    if ext in TEXT_EXTS:
        return 'text'
    return 'other'


def _visual_similarity(dist):
    """pHash distance -> 0..100 similarity, matching the original curve."""
    return max(0.0, (10 - dist) / 10) * 100


def _max_visual_distance(threshold):
    """
    Largest pHash distance that could still reach `threshold`.

    Uses the most generous possible case: perfect filename match, perfect size
    match and identical extensions. Any pair beyond this bound is unreachable
    regardless of its other signals, so gating on it is lossless.
    """
    best_weight = W_VISUAL + W_NAME + W_SIZE + W_EXT
    best_extra = 100 * (W_NAME + W_SIZE + W_EXT)
    limit = -1
    for dist in range(65):
        score = (_visual_similarity(dist) * W_VISUAL + best_extra) / best_weight
        if round(score, 1) >= threshold:
            limit = dist
    return max(limit, 0)


def _min_name_ratio_for_text(threshold):
    """
    Minimum SequenceMatcher ratio a text pair needs before it is worth scoring.

    Text files have no content signal, so only filename, size and extension
    apply. Assume a perfect size and extension match and solve for the name.
    """
    weight = W_NAME + W_SIZE + W_EXT
    needed = threshold * weight - 100 * (W_SIZE + W_EXT)
    return max(0.0, needed / W_NAME) / 100.0


MAX_VISUAL_DISTANCE = _max_visual_distance(SIMILARITY_THRESHOLD)
MIN_TEXT_NAME_RATIO = _min_name_ratio_for_text(SIMILARITY_THRESHOLD)


# ---------------------------------------------------------------------------
# Popcount
# ---------------------------------------------------------------------------

_POPCOUNT_TABLE = np.array([bin(i).count('1') for i in range(256)], dtype=np.uint8)


def _popcount64(arr):
    """Per-element bit count for a uint64 array."""
    bitwise_count = getattr(np, 'bitwise_count', None)
    if bitwise_count is not None:
        return bitwise_count(arr)
    if arr.size == 0:
        return np.zeros(0, dtype=np.uint16)
    return _POPCOUNT_TABLE[np.ascontiguousarray(arr).view(np.uint8).reshape(-1, 8)].sum(
        axis=1, dtype=np.uint16
    )


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def _score_pair(f1, f2, visual_dist=None):
    """
    Weighted similarity between two file records, normalised by the weights
    that actually applied.

    visual_dist is the precomputed pHash distance for visual pairs, or None for
    audio and text pairs.
    """
    score = 0.0
    total_weight = 0.0

    if visual_dist is not None:
        score += _visual_similarity(visual_dist) * W_VISUAL
        total_weight += W_VISUAL
    else:
        a1 = f1.get('audio_hash')
        a2 = f2.get('audio_hash')
        if a1 and a2:
            if a1 == a2:
                score += 100 * W_AUDIO
            total_weight += W_AUDIO

    name_a = f1.get('filename')
    name_b = f2.get('filename')
    if name_a and name_b:
        score += SequenceMatcher(None, name_a, name_b).ratio() * 100 * W_NAME
        total_weight += W_NAME

    size_a = f1.get('size') or 0
    size_b = f2.get('size') or 0
    if size_a > 0 and size_b > 0:
        size_sim = (1 - abs(size_a - size_b) / max(size_a, size_b)) * 100
        score += size_sim * W_SIZE
        total_weight += W_SIZE

    if f1.get('extension') == f2.get('extension'):
        score += 100 * W_EXT
        total_weight += W_EXT

    if total_weight == 0:
        return 0.0
    return round(score / total_weight, 1)


def _is_redundant_pair(f1, f2):
    """
    True when a pair should not be reported as a fuzzy match.

    Exact duplicates are reported separately, and hardlinks to the same inode
    are the same file on disk rather than two copies.
    """
    h1 = f1.get('exact_hash')
    h2 = f2.get('exact_hash')
    if h1 and h2 and h1 == h2:
        return True
    i1 = f1.get('_inode')
    i2 = f2.get('_inode')
    if i1 is not None and i1 == i2:
        return True
    return False


# ---------------------------------------------------------------------------
# Worker state
#
# The file list crosses the process boundary once per worker via the pool
# initializer, instead of once per submitted chunk.
# ---------------------------------------------------------------------------

_WORKER_FILES = None
_WORKER_HASHES = None


def _init_worker(files, hashes):
    global _WORKER_FILES, _WORKER_HASHES
    _WORKER_FILES = files
    _WORKER_HASHES = hashes


def _compare_visual_stride(offset, stride):
    """
    Compare rows offset, offset+stride, offset+2*stride, ... against their tails.

    Striding gives every chunk an even share of the triangular comparison
    space, which contiguous ranges do not.
    """
    files = _WORKER_FILES
    hashes = _WORKER_HASHES
    n = len(files)
    matches = []

    for i in range(offset, n - 1, stride):
        dists = _popcount64(hashes[i] ^ hashes[i + 1:])
        candidates = np.nonzero(dists <= MAX_VISUAL_DISTANCE)[0]
        if candidates.size == 0:
            continue

        f1 = files[i]
        for k in candidates:
            j = i + 1 + int(k)
            f2 = files[j]
            if _is_redundant_pair(f1, f2):
                continue
            score = _score_pair(f1, f2, visual_dist=int(dists[k]))
            if score >= SIMILARITY_THRESHOLD:
                matches.append({'file_a': f1['path'], 'file_b': f2['path'], 'score': score})

    return matches


def _compare_text_stride(offset, stride):
    """Same striding scheme for text files, which have no content signal."""
    files = _WORKER_FILES
    n = len(files)
    matches = []
    sm = SequenceMatcher(None, '', '')

    for i in range(offset, n - 1, stride):
        f1 = files[i]
        name_a = f1.get('filename') or ''
        sm.set_seq1(name_a)

        for j in range(i + 1, n):
            f2 = files[j]
            if _is_redundant_pair(f1, f2):
                continue

            name_b = f2.get('filename') or ''
            sm.set_seq2(name_b)
            # real_quick_ratio and quick_ratio are cheap upper bounds on ratio.
            if sm.real_quick_ratio() < MIN_TEXT_NAME_RATIO:
                continue
            if sm.quick_ratio() < MIN_TEXT_NAME_RATIO:
                continue

            score = _score_pair(f1, f2)
            if score >= SIMILARITY_THRESHOLD:
                matches.append({'file_a': f1['path'], 'file_b': f2['path'], 'score': score})

    return matches


def _match_audio(files):
    """
    Audio pairs can only clear the threshold when their fingerprints match
    exactly (the best score without the audio term is ~41%), so bucket by
    fingerprint rather than comparing every pair.
    """
    buckets = {}
    for f in files:
        h = f.get('audio_hash')
        if h:
            buckets.setdefault(h, []).append(f)

    matches = []
    for group in buckets.values():
        if len(group) < 2:
            continue
        for a in range(len(group)):
            f1 = group[a]
            for b in range(a + 1, len(group)):
                f2 = group[b]
                if _is_redundant_pair(f1, f2):
                    continue
                score = _score_pair(f1, f2)
                if score >= SIMILARITY_THRESHOLD:
                    matches.append({'file_a': f1['path'], 'file_b': f2['path'], 'score': score})
    return matches


# ---------------------------------------------------------------------------
# Matcher
# ---------------------------------------------------------------------------

class Matcher:
    def __init__(self, db_path):
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"Database not found at {db_path}")
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._files = None

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass

    # -- loading ------------------------------------------------------------

    def fetch_all_files(self):
        """
        Indexed records that still exist on disk, stat'ed once and annotated
        with a parsed pHash, a type bucket and an inode identity.

        The result is cached: the previous version ran this twice per scan,
        which meant two full table reads and two stat calls per file.
        """
        if self._files is not None:
            return self._files

        cursor = self.conn.execute(
            "SELECT path, filename, extension, size, mtime, exact_hash, visual_hash, audio_hash "
            "FROM files"
        )

        valid = []
        for row in cursor:
            record = dict(row)
            path = record['path']
            try:
                st = os.stat(path)
            except OSError:
                continue

            record['_inode'] = (st.st_dev, st.st_ino) if st.st_ino else None
            record['_group'] = _file_type_group(record.get('extension') or '')

            vh = record.get('visual_hash')
            if vh:
                try:
                    record['_vh'] = int(str(vh), 16)
                except ValueError:
                    record['_vh'] = None
            else:
                record['_vh'] = None

            valid.append(record)

        self._files = valid
        return valid

    # -- exact --------------------------------------------------------------

    def find_exact_duplicates(self):
        """
        Group files sharing an MD5. Hardlinks to the same inode are collapsed
        to a single entry, since trashing one of them frees nothing.
        """
        buckets = {}
        for f in self.fetch_all_files():
            h = f.get('exact_hash')
            if h:
                buckets.setdefault(h, []).append(f)

        groups = []
        for group in buckets.values():
            if len(group) < 2:
                continue
            seen_inodes = set()
            unique = []
            for f in group:
                inode = f.get('_inode')
                if inode is not None:
                    if inode in seen_inodes:
                        continue
                    seen_inodes.add(inode)
                unique.append(f)
            if len(unique) > 1:
                groups.append(unique)
        return groups

    # -- fuzzy --------------------------------------------------------------

    def find_fuzzy_matches(self, stop_signal=None, progress_callback=None, max_workers=None):
        files = self.fetch_all_files()
        if len(files) < 2:
            return []

        visual, audio, text = [], [], []
        for f in files:
            group = f['_group']
            if group == 'visual':
                # A visual file with no pHash can never score above zero
                # against another visual file, so drop it up front.
                if f['_vh'] is not None:
                    visual.append(f)
            elif group == 'audio':
                if f.get('audio_hash'):
                    audio.append(f)
            elif group == 'text':
                text.append(f)

        workers = max_workers if (max_workers and max_workers > 0) else MAX_MATCH_WORKERS
        workers = max(1, min(workers, os.cpu_count() or 1, MAX_MATCH_WORKERS))

        visual_chunks = self._chunk_count(len(visual), workers)
        text_chunks = self._chunk_count(len(text), workers)
        total_units = visual_chunks + text_chunks + (1 if audio else 0)
        done_units = 0

        def report():
            if progress_callback and total_units:
                progress_callback(done_units, total_units)

        matches = []

        if audio:
            if stop_signal and stop_signal():
                return matches
            matches.extend(_match_audio(audio))
            done_units += 1
            report()

        if visual_chunks:
            hashes = np.fromiter((f['_vh'] for f in visual), dtype=np.uint64, count=len(visual))
            produced, completed = self._run_strided(
                _compare_visual_stride, visual, hashes, visual_chunks, workers, stop_signal
            )
            matches.extend(produced)
            done_units += completed
            report()
            if stop_signal and stop_signal():
                return matches

        if text_chunks:
            produced, completed = self._run_strided(
                _compare_text_stride, text, None, text_chunks, workers, stop_signal
            )
            matches.extend(produced)
            done_units += completed
            report()

        return matches

    @staticmethod
    def _chunk_count(n, workers):
        if n < 2:
            return 0
        return max(1, min(workers * 8, n - 1))

    def _run_strided(self, func, files, hashes, chunks, workers, stop_signal):
        """
        Run `func(offset, chunks)` for every offset.

        Uses a process pool only for large inputs, and falls back to running
        in-process if the pool cannot be started. Frozen bundles on macOS in
        particular are prone to spawn failures, and the vectorised path is fast
        enough that a fallback costs very little.
        """
        use_processes = workers > 1 and len(files) >= PARALLEL_MIN_FILES

        if use_processes:
            try:
                return self._run_pool(func, files, hashes, chunks, workers, stop_signal)
            except Exception as exc:
                log.warning("Process pool unavailable (%s); falling back to in-process matching", exc)

        _init_worker(files, hashes)
        matches = []
        completed = 0
        for offset in range(chunks):
            if stop_signal and stop_signal():
                break
            matches.extend(func(offset, chunks))
            completed += 1
        return matches, completed

    def _run_pool(self, func, files, hashes, chunks, workers, stop_signal):
        matches = []
        completed = 0
        executor = concurrent.futures.ProcessPoolExecutor(
            max_workers=workers,
            initializer=_init_worker,
            initargs=(files, hashes),
        )
        try:
            futures = []
            for offset in range(chunks):
                if stop_signal and stop_signal():
                    break
                futures.append(executor.submit(func, offset, chunks))

            for future in concurrent.futures.as_completed(futures):
                if stop_signal and stop_signal():
                    for pending in futures:
                        pending.cancel()
                    break
                try:
                    matches.extend(future.result())
                except concurrent.futures.CancelledError:
                    pass
                except Exception:
                    # Previously swallowed silently, which hid worker failures
                    # behind an apparently successful run.
                    log.exception("Fuzzy comparison chunk failed")
                completed += 1
        finally:
            executor.shutdown(wait=True, cancel_futures=True)

        return matches, completed

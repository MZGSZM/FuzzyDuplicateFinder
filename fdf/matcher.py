"""
Duplicate matching.

Exact duplicates come straight out of sqlite (rows sharing an MD5).

Fuzzy matching works per kind (image, video, audio, text) and never across
kinds. Each kind has a fixed-width binary fingerprint, so finding candidates is
a Hamming-distance search done with vectorised XOR and popcount. That runs in
threads: numpy releases the GIL for the heavy part, so there is no process pool
to spawn, pickle into, or break inside a frozen bundle.

Candidates within the distance gate are then scored in Python:

    score = sum(similarity_i * weight_i) / sum(weight_i over signals present)

The gate is derived from the threshold: it is the largest distance at which a
pair could still reach the threshold if every other signal were perfect, so
gating loses nothing.
"""

from __future__ import annotations

import concurrent.futures as cf
import logging
import os
import sqlite3
from dataclasses import dataclass
from difflib import SequenceMatcher

import numpy as np

from .exts import KIND_COLUMNS, KINDS, exts_for
from .fingerprints import AUDIO_WORDS, FINGERPRINT_VERSION
from .paths import normalize_root

log = logging.getLogger(__name__)

DEFAULT_THRESHOLD = 70.0

W_CONTENT = 0.50
W_NAME = 0.20
W_SIZE = 0.10
W_DURATION = 0.15
W_EXT = 0.05

# Distance at which content similarity reaches zero, per kind, out of
# 64 bits (image, video, text) or 192 bits (audio).
ZERO_DISTANCE = {'image': 10, 'video': 10, 'audio': 64, 'text': 20}
HASH_WORDS = {'image': 1, 'video': 1, 'audio': AUDIO_WORDS, 'text': 1}
USES_DURATION = {'image': False, 'video': True, 'audio': True, 'text': False}

# Memory guard for pathological inputs (thousands of near-identical files):
# keep at most this many candidate pairs, preferring the closest.
CANDIDATE_CAP = 2_000_000
DEFAULT_MAX_PAIRS = 250_000


@dataclass(slots=True)
class FileRec:
    path: str
    name: str
    ext: str
    size: int
    exact: str | None
    duration: float | None
    inode: tuple | None


@dataclass(slots=True)
class Match:
    file_a: str
    file_b: str
    score: float
    type: str          # 'EXACT' or 'FUZZY'
    kind: str          # 'image', 'video', 'audio', 'text' or 'other'
    size_a: int = 0
    size_b: int = 0


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def content_similarity(kind, distance):
    return max(0.0, 1.0 - distance / ZERO_DISTANCE[kind]) * 100.0


def _ratio_similarity(a, b):
    if not a or not b:
        return None
    return (1.0 - abs(a - b) / max(a, b)) * 100.0


def score_pair(kind, distance, a, b):
    score = content_similarity(kind, distance) * W_CONTENT
    weight = W_CONTENT

    if a.name and b.name:
        score += SequenceMatcher(None, a.name, b.name).ratio() * 100.0 * W_NAME
        weight += W_NAME

    # For video and audio, duration replaces size: a re-encode changes the
    # size a lot and the duration not at all.
    dur_sim = _ratio_similarity(a.duration, b.duration) if USES_DURATION[kind] else None
    if dur_sim is not None:
        score += dur_sim * W_DURATION
        weight += W_DURATION
    else:
        size_sim = _ratio_similarity(a.size, b.size)
        if size_sim is not None:
            score += size_sim * W_SIZE
            weight += W_SIZE

    if a.ext == b.ext:
        score += 100.0 * W_EXT
        weight += W_EXT

    return round(score / weight, 1)


def max_distance(kind, threshold):
    """Largest distance that can still reach `threshold` with every other
    signal perfect. -1 means nothing can."""
    extras = [W_NAME + W_SIZE + W_EXT]
    if USES_DURATION[kind]:
        extras.append(W_NAME + W_DURATION + W_EXT)
    limit = -1
    for extra in extras:
        total = W_CONTENT + extra
        for dist in range(ZERO_DISTANCE[kind] + 1):
            best = (content_similarity(kind, dist) * W_CONTENT + 100.0 * extra) / total
            if round(best, 1) >= threshold:
                limit = max(limit, dist)
    return limit


# ---------------------------------------------------------------------------
# Hamming search
# ---------------------------------------------------------------------------

_POPCOUNT_TABLE = np.array([bin(i).count('1') for i in range(256)], dtype=np.uint8)


def popcount64(arr):
    """Per-element bit count for a uint64 array of any shape."""
    bitwise_count = getattr(np, 'bitwise_count', None)
    if bitwise_count is not None:
        return bitwise_count(arr)
    arr = np.ascontiguousarray(arr)
    out = _POPCOUNT_TABLE[arr.view(np.uint8)].reshape(arr.shape + (8,))
    return out.sum(axis=-1, dtype=np.uint16)


def parse_hashes(values, words):
    """Hex strings -> (valid_mask, uint64 array of shape (n,) or (n, words))."""
    n = len(values)
    out = np.zeros((n, words), dtype=np.uint64)
    valid = np.zeros(n, dtype=bool)
    width = 16 * words
    for row, value in enumerate(values):
        if not value or len(value) != width:
            continue
        try:
            for w in range(words):
                out[row, w] = int(value[w * 16:(w + 1) * 16], 16)
        except ValueError:
            continue
        valid[row] = True
    return valid, (out[:, 0].copy() if words == 1 else out)


def _compact(chunks_i, chunks_j, chunks_d, cap):
    i = np.concatenate(chunks_i)
    j = np.concatenate(chunks_j)
    d = np.concatenate(chunks_d)
    if d.size > cap:
        keep = np.argpartition(d, cap - 1)[:cap]
        i, j, d = i[keep], j[keep], d[keep]
    return [i], [j], [d]


def hamming_candidates(hashes, gate, offset, stride, stop_signal=None, cap=CANDIDATE_CAP):
    """
    Pairs (i, j>i, distance<=gate) for rows offset, offset+stride, ...

    Striding gives every chunk an even share of the triangular comparison
    space. Returns (i, j, d, truncated).
    """
    n = hashes.shape[0]
    two_d = hashes.ndim == 2
    ci, cj, cd = [], [], []
    held = 0
    truncated = False
    for count, i in enumerate(range(offset, n - 1, stride)):
        if stop_signal is not None and count % 256 == 0 and stop_signal():
            break
        x = hashes[i + 1:] ^ hashes[i]
        dist = popcount64(x)
        if two_d:
            dist = dist.sum(axis=1, dtype=np.uint16)
        hits = np.flatnonzero(dist <= gate)
        if hits.size == 0:
            continue
        ci.append(np.full(hits.size, i, dtype=np.int64))
        cj.append(hits.astype(np.int64) + i + 1)
        cd.append(dist[hits].astype(np.uint16))
        held += hits.size
        if held > 2 * cap:
            ci, cj, cd = _compact(ci, cj, cd, cap)
            held = ci[0].size
            truncated = True
    if not ci:
        empty = np.zeros(0, dtype=np.int64)
        return empty, empty, np.zeros(0, dtype=np.uint16), truncated
    ci, cj, cd = _compact(ci, cj, cd, cap) if held > cap else (ci, cj, cd)
    truncated = truncated or held > cap
    return np.concatenate(ci), np.concatenate(cj), np.concatenate(cd), truncated


# ---------------------------------------------------------------------------
# Matcher
# ---------------------------------------------------------------------------

class Matcher:
    def __init__(self, db_path, roots=None, verify_exists=False):
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"Index not found at {db_path}")
        self.conn = sqlite3.connect(db_path)
        self.verify_exists = verify_exists
        self._roots = None
        if roots:
            self._roots = tuple(
                normalize_root(r['path'] if isinstance(r, dict) else r).rstrip(os.sep) + os.sep
                for r in roots
            )
        self.truncated = False

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        self.close()

    # -- loading ------------------------------------------------------------

    def _keep(self, path):
        if self._roots is not None:
            norm = normalize_root(path)
            if not any(norm.startswith(r) for r in self._roots):
                return False
        return not self.verify_exists or os.path.isfile(path)

    def _records(self, sql, params=()):
        """Yield (FileRec, extra_column) for rows passing the root filter."""
        for row in self.conn.execute(sql, params):
            path, name, ext, size, exact, duration, dev, inode, extra = row
            if not self._keep(path):
                continue
            yield FileRec(
                path=path, name=name or os.path.basename(path), ext=ext or '',
                size=size or 0, exact=exact or None, duration=duration,
                inode=(dev, inode) if inode else None,
            ), extra

    _BASE = "SELECT path, filename, extension, size, exact_hash, duration, dev, inode"

    # -- exact --------------------------------------------------------------

    def exact_groups(self):
        """
        Groups of files sharing an MD5, largest wasted space first. Hardlinks
        to one inode collapse to a single entry, since trashing one frees
        nothing.
        """
        sql = (
            f"{self._BASE}, NULL FROM files WHERE exact_hash IN ("
            "SELECT exact_hash FROM files WHERE exact_hash IS NOT NULL AND exact_hash != '' "
            "GROUP BY exact_hash HAVING COUNT(*) > 1) ORDER BY exact_hash, path"
        )
        buckets = {}
        for rec, _ in self._records(sql):
            buckets.setdefault(rec.exact, []).append(rec)

        groups = []
        for group in buckets.values():
            seen, unique = set(), []
            for rec in group:
                if rec.inode is not None:
                    if rec.inode in seen:
                        continue
                    seen.add(rec.inode)
                unique.append(rec)
            if len(unique) > 1:
                groups.append(unique)
        groups.sort(key=lambda g: g[0].size * (len(g) - 1), reverse=True)
        return groups

    # -- fuzzy --------------------------------------------------------------

    def _load_kind(self, kind):
        column = KIND_COLUMNS[kind]
        exts = sorted(exts_for({kind}))
        marks = ",".join("?" for _ in exts)
        # Image hashes kept the same format across versions; video and audio
        # fingerprints did not, so those need current-version rows.
        version_clause = "" if kind == 'image' else " AND fp_version = ?"
        params = list(exts) + ([] if kind == 'image' else [FINGERPRINT_VERSION])
        sql = (
            f"{self._BASE}, {column} FROM files WHERE {column} IS NOT NULL AND {column} != '' "
            f"AND extension IN ({marks}){version_clause} ORDER BY path"
        )
        records, values = [], []
        seen_exact, seen_inode = set(), set()
        for rec, value in self._records(sql, params):
            # One representative per exact group and per inode. Otherwise every
            # copy of a file pairs with every near-match of it, which repeats
            # the same finding once per copy.
            if rec.exact:
                if rec.exact in seen_exact:
                    continue
                seen_exact.add(rec.exact)
            if rec.inode is not None:
                if rec.inode in seen_inode:
                    continue
                seen_inode.add(rec.inode)
            records.append(rec)
            values.append(value)
        valid, hashes = parse_hashes(values, HASH_WORDS[kind])
        if not valid.all():
            keep = np.flatnonzero(valid)
            records = [records[k] for k in keep]
            hashes = hashes[keep]
        return records, hashes

    def stale_fingerprints(self):
        """Count of media rows whose fuzzy fingerprints predate this version.
        Shown to the user after loading an old index without rescanning."""
        exts = sorted(exts_for({'video', 'audio'}))
        marks = ",".join("?" for _ in exts)
        row = self.conn.execute(
            f"SELECT COUNT(*) FROM files WHERE extension IN ({marks}) "
            "AND (fp_version IS NULL OR fp_version != ?)",
            [*exts, FINGERPRINT_VERSION],
        ).fetchone()
        return row[0] if row else 0

    def fuzzy_matches(self, threshold=DEFAULT_THRESHOLD, kinds=KINDS, stop_signal=None,
                      progress=None, workers=None, max_pairs=DEFAULT_MAX_PAIRS):
        """
        Fuzzy matches across `kinds`, best first. progress(done, total) is
        called as chunks finish. Sets self.truncated when a cap was hit.
        """
        self.truncated = False
        workers = max(1, min(int(workers or os.cpu_count() or 4), 32))

        plans = []
        for kind in KINDS:
            if kind not in kinds:
                continue
            gate = max_distance(kind, threshold)
            if gate < 0:
                continue
            records, hashes = self._load_kind(kind)
            if len(records) < 2:
                continue
            chunks = max(1, min(workers * 8, len(records) - 1))
            plans.append((kind, gate, records, hashes, chunks))

        total = sum(p[4] for p in plans)
        done = 0
        matches = []

        for kind, gate, records, hashes, chunks in plans:
            if stop_signal and stop_signal():
                break
            parts = []
            with cf.ThreadPoolExecutor(max_workers=workers,
                                       thread_name_prefix="fdf-match") as pool:
                per_chunk_cap = max(1000, CANDIDATE_CAP // chunks)
                futures = [
                    pool.submit(hamming_candidates, hashes, gate, offset, chunks,
                                stop_signal, per_chunk_cap)
                    for offset in range(chunks)
                ]
                for future in cf.as_completed(futures):
                    try:
                        parts.append(future.result())
                    except Exception:
                        log.exception("Fuzzy comparison chunk failed")
                    done += 1
                    if progress:
                        progress(done, total)
            if stop_signal and stop_signal():
                break

            matches.extend(self._score(kind, records, parts, threshold))

        matches.sort(key=lambda m: m.score, reverse=True)
        if len(matches) > max_pairs:
            del matches[max_pairs:]
            self.truncated = True
        return matches

    def _score(self, kind, records, parts, threshold):
        if not parts:
            return []
        ci = np.concatenate([p[0] for p in parts])
        cj = np.concatenate([p[1] for p in parts])
        cd = np.concatenate([p[2] for p in parts])
        if any(p[3] for p in parts):
            self.truncated = True
        if cd.size > CANDIDATE_CAP:
            keep = np.argpartition(cd, CANDIDATE_CAP - 1)[:CANDIDATE_CAP]
            ci, cj, cd = ci[keep], cj[keep], cd[keep]
            self.truncated = True

        out = []
        for i, j, d in zip(ci.tolist(), cj.tolist(), cd.tolist(), strict=True):
            a, b = records[i], records[j]
            # Exact duplicates are reported separately, and hardlinks are one
            # file, not two.
            if a.exact and a.exact == b.exact:
                continue
            if a.inode is not None and a.inode == b.inode:
                continue
            score = score_pair(kind, d, a, b)
            if score >= threshold:
                # Larger file first: usually the higher quality copy.
                if b.size > a.size:
                    a, b = b, a
                out.append(Match(a.path, b.path, score, 'FUZZY', kind, a.size, b.size))
        return out

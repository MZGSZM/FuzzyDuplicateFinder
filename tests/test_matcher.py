import os

import numpy as np
import pytest
from conftest import make_image, make_text, posix_only

from fdf import matcher as m
from fdf.matcher import FileRec, Matcher, hamming_candidates, max_distance, score_pair
from fdf.scanner import Scanner, ScanOptions


def rec(name, size=1000, ext=".jpg", duration=None, exact=None, inode=None):
    return FileRec(path="/x/" + name, name=name, ext=ext, size=size, exact=exact,
                   duration=duration, inode=inode)


def test_max_distance_is_lossless():
    """No pair beyond the gate may score at or above the threshold, even with
    every other signal perfect."""
    for kind in m.KINDS:
        for threshold in (40, 55, 70, 85, 100):
            gate = max_distance(kind, threshold)
            a = rec("same.jpg", duration=10.0)
            b = rec("same.jpg", duration=10.0)
            for dist in range(gate + 1, m.ZERO_DISTANCE[kind] + 1):
                assert score_pair(kind, dist, a, b) < threshold
            if gate >= 0:
                assert score_pair(kind, gate, a, b) >= threshold


def test_gate_shrinks_as_threshold_rises():
    for kind in m.KINDS:
        gates = [max_distance(kind, t) for t in range(30, 101, 5)]
        assert gates == sorted(gates, reverse=True)


def test_duration_replaces_size_for_media():
    a = rec("clip.mp4", size=10_000_000, ext=".mp4", duration=60.0)
    b = rec("clip.mp4", size=2_000_000, ext=".mp4", duration=60.0)
    # A re-encode: very different size, same duration. Size must not count.
    assert score_pair("video", 0, a, b) == 100.0
    # Images have no duration, so size counts there.
    assert score_pair("image", 0, rec("x.jpg", 1000), rec("x.jpg", 500)) < 100.0


def test_popcount_fallback_matches_numpy(monkeypatch):
    arr = np.random.default_rng(0).integers(0, 2**63, size=(50, 3), dtype=np.uint64)
    expected = np.vectorize(lambda v: bin(int(v)).count("1"))(arr)
    assert (m.popcount64(arr) == expected).all()
    monkeypatch.delattr(np, "bitwise_count", raising=False)
    assert (m.popcount64(arr) == expected).all()


def brute_force(hashes, gate):
    out = set()
    n = len(hashes)
    for i in range(n):
        for j in range(i + 1, n):
            row = np.atleast_1d(hashes[i] ^ hashes[j])
            if sum(bin(int(v)).count("1") for v in row) <= gate:
                out.add((i, j))
    return out


@pytest.mark.parametrize("words", [1, 3])
def test_hamming_candidates_match_brute_force(words):
    rng = np.random.default_rng(7)
    base = rng.integers(0, 2**63, size=(8, words), dtype=np.uint64)
    rows = []
    for b in base:                      # clusters of near-identical hashes
        for _ in range(6):
            noisy = b.copy()
            for _ in range(rng.integers(0, 4)):
                w = rng.integers(0, words)
                noisy[w] ^= np.uint64(1) << np.uint64(rng.integers(0, 64))
            rows.append(noisy)
    hashes = np.array(rows, dtype=np.uint64)
    if words == 1:
        hashes = hashes[:, 0].copy()
    gate = 4
    found = set()
    for offset in range(5):
        i, j, d, truncated = hamming_candidates(hashes, gate, offset, 5)
        assert not truncated
        found |= set(zip(i.tolist(), j.tolist(), strict=True))
    assert found == brute_force(hashes, gate)


def test_candidate_cap_keeps_closest():
    hashes = np.zeros(200, dtype=np.uint64)       # every pair is distance 0
    hashes[:100] = np.uint64(1)                   # half are distance 1 from the rest
    i, j, d, truncated = hamming_candidates(hashes, 1, 0, 1, cap=500)
    assert truncated
    assert len(d) <= 500
    assert d.max() == 0


@pytest.fixture
def scanned(tmp_path):
    lib = tmp_path / "lib"
    lib.mkdir()
    make_image(lib / "photo.jpg", quality=95)
    make_image(lib / "photo_copy.jpg", quality=55)
    (lib / "photo_exact.jpg").write_bytes((lib / "photo.jpg").read_bytes())
    make_image(lib / "other.jpg", seed=50)
    make_text(lib / "a.txt")
    make_text(lib / "a_edit.txt", edit=lambda t: t + " appended words")
    make_text(lib / "unrelated.txt", seed=60)
    db = tmp_path / "i.db"
    Scanner(ScanOptions()).scan([str(lib)], str(db))
    return lib, str(db)


def test_exact_groups(scanned):
    lib, db = scanned
    with Matcher(db) as matcher:
        groups = matcher.exact_groups()
    assert [sorted(os.path.basename(r.path) for r in g) for g in groups] == [
        ["photo.jpg", "photo_exact.jpg"]
    ]


def test_fuzzy_matches(scanned):
    lib, db = scanned
    with Matcher(db) as matcher:
        found = matcher.fuzzy_matches(threshold=70)
    pairs = {frozenset((os.path.basename(x.file_a), os.path.basename(x.file_b))): x
             for x in found}
    # Exact copies collapse to one representative, so the near-duplicate is
    # reported once rather than once per copy.
    assert frozenset({"photo.jpg", "photo_copy.jpg"}) in pairs
    assert frozenset({"photo_exact.jpg", "photo_copy.jpg"}) not in pairs
    assert frozenset({"a.txt", "a_edit.txt"}) in pairs
    assert not any("other.jpg" in p or "unrelated.txt" in p for p in pairs)
    assert all(x.type == "FUZZY" for x in found)
    assert found == sorted(found, key=lambda x: x.score, reverse=True)


def test_threshold_and_kinds(scanned):
    _, db = scanned
    with Matcher(db) as matcher:
        assert matcher.fuzzy_matches(threshold=100.1) == []
        only_text = matcher.fuzzy_matches(kinds=("text",))
    assert {x.kind for x in only_text} == {"text"}


def test_roots_filter(scanned, tmp_path):
    _, db = scanned
    with Matcher(db, roots=[str(tmp_path / "elsewhere")]) as matcher:
        assert matcher.exact_groups() == []
        assert matcher.fuzzy_matches() == []


def test_verify_exists_skips_deleted(scanned):
    lib, db = scanned
    os.remove(lib / "photo_exact.jpg")
    with Matcher(db, verify_exists=True) as matcher:
        assert matcher.exact_groups() == []


def test_max_pairs_truncates(scanned):
    _, db = scanned
    with Matcher(db) as matcher:
        found = matcher.fuzzy_matches(max_pairs=1)
        assert len(found) == 1 and matcher.truncated


@posix_only
def test_hardlinks_are_one_file(tmp_path):
    lib = tmp_path / "lib"
    lib.mkdir()
    make_image(lib / "a.jpg")
    os.link(lib / "a.jpg", lib / "hard.jpg")
    db = tmp_path / "i.db"
    Scanner().scan([str(lib)], str(db))
    with Matcher(db) as matcher:
        assert matcher.exact_groups() == []
        assert matcher.fuzzy_matches() == []

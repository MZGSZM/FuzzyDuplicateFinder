import os
import sqlite3

import pytest
from conftest import make_image, make_text, posix_only

from fdf import fingerprints as fp
from fdf.index import COLUMN_NAMES, Index
from fdf.scanner import Scanner, ScanOptions, iter_files


def rows(db):
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    try:
        return {r["path"]: dict(r) for r in conn.execute("SELECT * FROM files")}
    finally:
        conn.close()


def scan(roots, db, **opts):
    return Scanner(ScanOptions(**opts)).scan(roots, str(db))


@pytest.fixture
def library(tmp_path):
    lib = tmp_path / "lib"
    (lib / "sub").mkdir(parents=True)
    make_image(lib / "a.jpg")
    make_image(lib / "sub" / "b.jpg", quality=60)
    (lib / "sub" / "a_copy.jpg").write_bytes((lib / "a.jpg").read_bytes())
    make_text(lib / "notes.txt")
    (lib / "doc.pdf").write_bytes(b"%PDF fake" * 50)
    (lib / "doc_copy.pdf").write_bytes(b"%PDF fake" * 50)
    return lib


def test_first_scan_indexes_and_hashes(library, tmp_path):
    db = tmp_path / "i.db"
    result = scan([str(library)], db)
    data = rows(db)
    assert result.files == 4                      # pdfs are not media
    assert result.issues == []
    a = data[str(library / "a.jpg")]
    copy = data[str(library / "sub" / "a_copy.jpg")]
    assert a["exact_hash"] and a["exact_hash"] == copy["exact_hash"]
    assert a["visual_hash"] and a["fp_version"] == fp.FINGERPRINT_VERSION
    assert data[str(library / "notes.txt")]["text_hash"]
    # Unique sizes never get an exact hash: they cannot have a byte twin.
    assert data[str(library / "sub" / "b.jpg")]["exact_hash"] is None


def test_rescan_reuses_unchanged_files(library, tmp_path):
    db = tmp_path / "i.db"
    scan([str(library)], db)
    again = scan([str(library)], db)
    assert again.fingerprinted == 0
    assert again.reused == 4


def test_changed_file_is_rehashed(library, tmp_path):
    db = tmp_path / "i.db"
    scan([str(library)], db)
    target = library / "sub" / "b.jpg"
    make_image(target, seed=42)
    os.utime(target, (1, 1))
    result = scan([str(library)], db)
    assert result.fingerprinted == 1


def test_deleted_files_are_purged(library, tmp_path):
    db = tmp_path / "i.db"
    scan([str(library)], db)
    os.remove(library / "notes.txt")
    result = scan([str(library)], db)
    assert result.removed == 1
    assert str(library / "notes.txt") not in rows(db)


def test_all_types_finds_exact_duplicates_of_anything(library, tmp_path):
    db = tmp_path / "i.db"
    scan([str(library)], db, all_types=True)
    data = rows(db)
    pdf = data[str(library / "doc.pdf")]
    assert pdf["exact_hash"] == data[str(library / "doc_copy.pdf")]["exact_hash"]


def test_kinds_filter_and_late_enable(library, tmp_path):
    db = tmp_path / "i.db"
    scan([str(library)], db, kinds=frozenset({"image"}))
    assert str(library / "notes.txt") not in rows(db)
    scan([str(library)], db, kinds=frozenset({"image", "text"}))
    assert rows(db)[str(library / "notes.txt")]["text_hash"]


def test_filters(tmp_path):
    root = tmp_path / "root"
    for d in ("keep", ".hidden", "node_modules", "skipme"):
        (root / d).mkdir(parents=True)
        make_text(root / d / "f.txt")
    (root / "keep" / "tiny.txt").write_text("hi there")
    (root / "keep" / "empty.txt").write_text("")
    opts = ScanOptions(excludes=("node_modules", "skip*"))
    found = {os.path.relpath(p[0], root) for p in iter_files([str(root)], opts)}
    assert found == {os.path.join("keep", "f.txt"), os.path.join("keep", "tiny.txt")}

    opts = ScanOptions(skip_hidden=False, excludes=(), min_size=100)
    found = {os.path.relpath(p[0], root) for p in iter_files([str(root)], opts)}
    assert os.path.join(".hidden", "f.txt") in found
    assert os.path.join("keep", "tiny.txt") not in found


def test_path_pattern_exclude(tmp_path):
    root = tmp_path / "root"
    (root / "a" / "cache").mkdir(parents=True)
    (root / "b" / "cache").mkdir(parents=True)
    make_text(root / "a" / "cache" / "f.txt")
    make_text(root / "b" / "cache" / "f.txt")
    pattern = os.path.join("*", "a", "cache")
    found = list(iter_files([str(root)], ScanOptions(excludes=(pattern,))))
    assert [os.path.relpath(p[0], root) for p in found] == [os.path.join("b", "cache", "f.txt")]


def test_nested_roots_are_walked_once(library, tmp_path):
    found = list(iter_files([str(library), str(library / "sub")], ScanOptions()))
    paths = [p[0] for p in found]
    assert len(paths) == len(set(paths)) == 4


@posix_only
def test_symlinks_are_not_indexed(library, tmp_path):
    os.symlink(library / "a.jpg", library / "link.jpg")
    os.symlink(library / "sub", library / "linkdir")
    found = {os.path.basename(p[0]) for p in iter_files([str(library)], ScanOptions())}
    assert "link.jpg" not in found
    assert len(found) == 4


def test_large_same_size_files_need_full_read(tmp_path):
    root = tmp_path / "r"
    root.mkdir()
    payload = os.urandom(400_000)
    (root / "a.txt").write_bytes(payload)
    (root / "b.txt").write_bytes(payload)
    (root / "c.txt").write_bytes(payload[:200_000] + b"!" + payload[200_001:])
    db = tmp_path / "i.db"
    result = Scanner(ScanOptions(all_types=True, kinds=frozenset())).scan([str(root)], str(db))
    data = rows(db)
    assert result.full_reads == 3
    assert data[str(root / "a.txt")]["exact_hash"] == data[str(root / "b.txt")]["exact_hash"]
    assert data[str(root / "c.txt")]["exact_hash"] != data[str(root / "a.txt")]["exact_hash"]


def test_broken_media_is_reported_but_still_exact_hashed(tmp_path):
    root = tmp_path / "r"
    root.mkdir()
    (root / "bad1.png").write_text("I am a teapot, not a PNG")
    (root / "bad2.png").write_text("I am a teapot, not a PNG")
    db = tmp_path / "i.db"
    result = Scanner().scan([str(root)], str(db))
    assert {os.path.basename(p) for p, _ in result.issues} == {"bad1.png", "bad2.png"}
    data = rows(db)
    assert data[str(root / "bad1.png")]["visual_hash"] == ""
    assert data[str(root / "bad1.png")]["exact_hash"] == data[str(root / "bad2.png")]["exact_hash"]
    # The failure is remembered: an unchanged broken file is not retried.
    assert Scanner().scan([str(root)], str(db)).fingerprinted == 0


def test_stop_signal_aborts(library, tmp_path):
    result = Scanner().scan([str(library)], str(tmp_path / "i.db"), stop_signal=lambda: True)
    assert result.aborted


def test_progress_reports_phases(library, tmp_path):
    phases = []
    Scanner().scan([str(library)], str(tmp_path / "i.db"),
                   progress=lambda phase, done, total, issues: phases.append(phase))
    assert phases[0] == "walk"
    assert "hash" in phases and "verify" in phases


def test_index_upgrades_old_schema(tmp_path):
    db = tmp_path / "old.db"
    conn = sqlite3.connect(db)
    conn.execute("""CREATE TABLE files (id INTEGER PRIMARY KEY AUTOINCREMENT, path TEXT UNIQUE,
        filename TEXT, extension TEXT, size INTEGER, mtime REAL, ctime REAL, exact_hash TEXT,
        visual_hash TEXT, audio_hash TEXT, scan_date TEXT)""")
    conn.execute("CREATE TABLE scan_roots (path TEXT PRIMARY KEY, priority INTEGER)")
    conn.execute("INSERT INTO files (path, filename, extension, size, mtime, audio_hash) "
                 "VALUES ('/x/song.mp3', 'song.mp3', '.mp3', 10, 1.0, 'abcdef')")
    conn.execute("INSERT INTO scan_roots VALUES ('/x', 7)")
    conn.commit()
    conn.close()

    index = Index(str(db))
    existing = index.load_existing()
    assert index.get_roots() == [{"path": "/x", "priority": 7}]
    index.close()
    assert existing["/x/song.mp3"][-1] is None          # fp_version: forces refresh
    cols = {r[1] for r in sqlite3.connect(db).execute("PRAGMA table_info(files)")}
    assert set(COLUMN_NAMES) <= cols


def test_old_fingerprints_are_recomputed(tmp_path):
    root = tmp_path / "r"
    root.mkdir()
    make_text(root / "a.txt")
    db = tmp_path / "i.db"
    Scanner().scan([str(root)], str(db))
    conn = sqlite3.connect(db)
    conn.execute("UPDATE files SET fp_version = 1, text_hash = 'deadbeefdeadbeef'")
    conn.commit()
    conn.close()
    result = Scanner().scan([str(root)], str(db))
    assert result.fingerprinted == 1
    assert rows(db)[str(root / "a.txt")]["text_hash"] != "deadbeefdeadbeef"

"""Index location modes, scan logs beside the index, and the Windows version
resource values. All Qt-free except the settings tests."""

import logging
import os

import pytest

from fdf import buildinfo, cli, logs, paths
from fdf.scanner import ScanOptions, iter_files

# ---------------------------------------------------------------------------
# paths.resolve_index and friends
# ---------------------------------------------------------------------------


@pytest.fixture
def roots(tmp_path):
    a = tmp_path / "a"
    b = tmp_path / "b"
    a.mkdir()
    b.mkdir()
    return [str(a), str(b)]


def test_folder_set_key_ignores_order_and_spelling(roots):
    assert paths.folder_set_key(roots) == paths.folder_set_key(list(reversed(roots)))
    assert paths.folder_set_key(roots) == paths.folder_set_key([r + os.sep for r in roots])
    assert paths.folder_set_key(roots[:1]) != paths.folder_set_key(roots)


def test_appdata_mode_matches_1_6_default(roots):
    choice = paths.resolve_index(roots, "appdata")
    assert choice.problem is None
    assert choice.path == paths.default_index_path(roots)
    assert os.path.dirname(choice.path) == paths.index_dir()


def test_folder_mode_uses_first_folder(roots):
    choice = paths.resolve_index(roots, "folder")
    key = paths.folder_set_key(roots)
    assert choice.problem is None
    assert choice.path == os.path.join(roots[0], f".fdf-index-{key}.db")


def test_folder_mode_finds_existing_index_in_any_folder(roots):
    existing = paths.folder_index_path(roots[1], roots)
    open(existing, "w").close()
    assert paths.resolve_index(roots, "folder").path == existing
    assert paths.existing_index(roots, "folder") == existing


def test_folder_mode_asks_for_holder_across_disks(roots, monkeypatch):
    monkeypatch.setattr(paths, "spans_volumes", lambda _r: True)
    choice = paths.resolve_index(roots, "folder")
    assert choice.problem == "holder" and choice.path is None
    # A single folder never needs the question.
    assert paths.resolve_index(roots[:1], "folder").problem is None


def test_folder_mode_never_falls_back_when_unwritable(roots, monkeypatch):
    monkeypatch.setattr(paths, "dir_writable", lambda _p: False)
    choice = paths.resolve_index(roots, "folder")
    assert choice.problem == "unwritable"
    assert choice.folder == roots[0]


def test_custom_mode(roots, tmp_path):
    custom = tmp_path / "idx"
    custom.mkdir()
    choice = paths.resolve_index(roots, "custom", str(custom))
    assert choice.problem is None
    assert choice.path == os.path.join(str(custom),
                                       f"fdf-index-{paths.folder_set_key(roots)}.db")
    assert paths.resolve_index(roots, "custom", "").problem == "unwritable"
    missing = paths.resolve_index(roots, "custom", str(tmp_path / "missing"))
    assert missing.problem == "unwritable"


def test_ask_mode_suggests_app_data(roots):
    choice = paths.resolve_index(roots, "ask")
    assert choice.problem == "ask"
    assert choice.path == paths.default_index_path(roots)
    assert paths.existing_index(roots, "ask") is None


def test_dir_writable(tmp_path):
    assert paths.dir_writable(str(tmp_path))
    assert not paths.dir_writable(str(tmp_path / "missing"))
    assert not paths.dir_writable("")
    assert os.listdir(tmp_path) == []           # the probe file is gone


def test_scan_log_path(roots, tmp_path):
    key = paths.folder_set_key(roots)
    assert paths.scan_log_path(paths.default_index_path(roots)) is None
    assert paths.scan_log_path(paths.folder_index_path(roots[0], roots)) == \
        os.path.join(roots[0], ".fdf-logs", f"scan-{key}.log")
    assert paths.scan_log_path(paths.custom_index_path(str(tmp_path), roots)) == \
        os.path.join(str(tmp_path), "fdf-logs", f"scan-{key}.log")
    assert paths.scan_log_path(str(tmp_path / "picked.db")) == \
        os.path.join(str(tmp_path), "fdf-logs", "scan-picked.log")


# ---------------------------------------------------------------------------
# The scanner never indexes the app's own files
# ---------------------------------------------------------------------------

def test_scanner_skips_indexes_and_scan_logs(tmp_path):
    for name in (".fdf-index-aaaa.db", ".fdf-index-aaaa.db-wal", "fdf-index-bbbb.db",
                 "fdf-index-bbbb.db-shm", "keep.txt"):
        (tmp_path / name).write_text("x")
    for folder in ("fdf-logs", ".fdf-logs"):
        (tmp_path / folder).mkdir()
        (tmp_path / folder / "scan-x.log").write_text("x")
    options = ScanOptions(all_types=True, skip_hidden=False, excludes=())
    found = sorted(os.path.basename(p) for p, *_ in iter_files([str(tmp_path)], options))
    assert found == ["keep.txt"]


# ---------------------------------------------------------------------------
# Scan logs
# ---------------------------------------------------------------------------

class _Collect(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records = []

    def emit(self, record):
        self.records.append(record)


@pytest.fixture
def app_log():
    root = logging.getLogger()
    handler = _Collect()
    old_level = root.level
    root.addHandler(handler)
    root.setLevel(logging.INFO)
    yield handler
    root.removeHandler(handler)
    root.setLevel(old_level)


def test_scan_log_routes_beside_the_index(roots, app_log):
    db = paths.folder_index_path(roots[0], roots)
    with logs.scan_log(db) as log_path:
        logging.getLogger("fdf.scanner").info("SECRET %s", "/enc/photo.jpg")
        logging.getLogger("fdf.watchdog.sub").warning("slow /enc/video.mkv")
        logging.getLogger("fdf.gui").info("window message")
    assert log_path and os.path.exists(log_path)
    with open(log_path, encoding="utf-8") as handle:
        text = handle.read()
    assert "/enc/photo.jpg" in text and "/enc/video.mkv" in text
    seen = [r.getMessage() for r in app_log.records]
    assert seen == ["window message"]
    # Routing ends with the block.
    assert logging.getLogger("fdf.scanner").propagate
    logging.getLogger("fdf.scanner").info("after")
    assert app_log.records[-1].getMessage() == "after"


def test_scan_log_leaves_app_data_indexes_alone(roots, app_log):
    with logs.scan_log(paths.default_index_path(roots)) as log_path:
        logging.getLogger("fdf.scanner").info("in app log")
    assert log_path is None
    assert [r.getMessage() for r in app_log.records] == ["in app log"]


def test_scan_log_drops_rather_than_leaks(tmp_path, app_log, monkeypatch):
    db = str(tmp_path / "x.db")

    def refuse(_path):
        raise OSError("read-only")

    monkeypatch.setattr(logs, "_file_handler", refuse)
    with logs.scan_log(db) as log_path:
        logging.getLogger("fdf.matcher").info("must not reach the app log")
    assert log_path is None
    assert app_log.records == []


# ---------------------------------------------------------------------------
# CLI follows the GUI setting, with overrides
# ---------------------------------------------------------------------------

@pytest.fixture
def lib(tmp_path):
    folder = tmp_path / "lib"
    folder.mkdir()
    (folder / "one.bin").write_bytes(b"same bytes" * 100)
    (folder / "two.bin").write_bytes(b"same bytes" * 100)
    return folder


def _scan(*extra):
    return cli.main(["prog", "scan", *extra, "--exact-only", "-q", "--no-watchdog"])


def test_cli_in_folder_flag(lib, capsys):
    assert _scan(str(lib), "--index-in-folder") == 0
    names = os.listdir(lib)
    assert any(n.startswith(".fdf-index-") for n in names)
    assert ".fdf-logs" in names
    assert "one.bin" in capsys.readouterr().out
    assert not os.listdir(paths.index_dir())


def test_cli_follows_gui_setting(lib):
    from fdf.settings import AppSettings
    settings = AppSettings()
    settings.index_mode = "folder"
    settings.q.sync()
    assert _scan(str(lib)) == 0
    assert any(n.startswith(".fdf-index-") for n in os.listdir(lib))
    # --index-appdata overrides it.
    assert _scan(str(lib), "--index-appdata") == 0
    assert os.listdir(paths.index_dir())


def test_cli_ask_mode_without_terminal_fails_cleanly(lib, capsys, monkeypatch):
    from fdf.settings import AppSettings
    settings = AppSettings()
    settings.index_mode = "ask"
    settings.q.sync()
    monkeypatch.setattr(cli, "_interactive", lambda: False)
    assert _scan(str(lib)) == 2
    assert "--index-dir" in capsys.readouterr().err
    assert not os.listdir(paths.index_dir())


def test_cli_ask_mode_on_a_terminal(lib, tmp_path, monkeypatch):
    from fdf.settings import AppSettings
    settings = AppSettings()
    settings.index_mode = "ask"
    settings.q.sync()
    target = tmp_path / "chosen"
    monkeypatch.setattr(cli, "_interactive", lambda: True)
    monkeypatch.setattr("builtins.input", lambda: str(target))
    assert _scan(str(lib)) == 0
    assert (tmp_path / "chosen.db").exists()


def test_cli_holder_question(lib, tmp_path, monkeypatch, capsys):
    other = tmp_path / "other"
    other.mkdir()
    monkeypatch.setattr(paths, "spans_volumes", lambda _r: True)
    monkeypatch.setattr(cli, "_interactive", lambda: False)
    assert _scan(str(lib), str(other), "--index-in-folder") == 2
    assert "different disks" in capsys.readouterr().err
    monkeypatch.setattr(cli, "_interactive", lambda: True)
    monkeypatch.setattr("builtins.input", lambda: "2")
    assert _scan(str(lib), str(other), "--index-in-folder") == 0
    assert any(n.startswith(".fdf-index-") for n in os.listdir(other))
    assert not any(n.startswith(".fdf-index-") for n in os.listdir(lib))


def test_cli_unwritable_never_falls_back(lib, monkeypatch, capsys):
    monkeypatch.setattr(paths, "dir_writable", lambda _p: False)
    monkeypatch.setattr(cli, "_interactive", lambda: False)
    assert _scan(str(lib), "--index-in-folder") == 2
    assert "cannot be written" in capsys.readouterr().err
    assert not os.listdir(paths.index_dir())


def test_cli_missing_folder(tmp_path, capsys):
    assert _scan(str(tmp_path / "nope")) == 2
    assert "Not a folder" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

def test_index_mode_setting_validates(tmp_path):
    from PyQt6.QtCore import QSettings

    from fdf.settings import AppSettings
    q = QSettings(str(tmp_path / "s.ini"), QSettings.Format.IniFormat)
    s = AppSettings(q)
    assert s.index_mode == "appdata" and s.index_custom_dir == ""
    q.setValue("index/mode", "bogus")
    assert s.index_mode == "appdata"
    s.index_mode = "folder"
    assert AppSettings(q).index_mode == "folder"


def test_legacy_folder_list_is_removed(tmp_path):
    from PyQt6.QtCore import QSettings

    from fdf.settings import LEGACY_FOLDERS_KEY, AppSettings
    q = QSettings(str(tmp_path / "s.ini"), QSettings.Format.IniFormat)
    q.setValue(LEGACY_FOLDERS_KEY, '[{"path": "/enc/photos", "priority": 10}]')
    q.setValue("match/threshold", 70.0)
    q.sync()
    s = AppSettings(q)
    assert s.forget_legacy_folders()
    assert not q.contains(LEGACY_FOLDERS_KEY)
    assert not s.forget_legacy_folders()
    with open(tmp_path / "s.ini", encoding="utf-8") as handle:
        assert "/enc/photos" not in handle.read()


# ---------------------------------------------------------------------------
# Windows version resource
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("tag, expected", [
    ("1.7.0", (1, 7, 0, 0)),
    ("1.7.0-rc1", (1, 7, 0, 0)),
    ("v2", (2, 0, 0, 0)),
    ("1.2.3.4.5", (1, 2, 3, 4)),
    ("dev", (0, 0, 0, 0)),
])
def test_numeric_version(tag, expected):
    assert buildinfo.numeric_version(tag) == expected


def test_version_strings_keep_full_tag():
    strings = buildinfo.version_strings("1.7.0-rc1")
    assert strings["ProductVersion"] == "1.7.0-rc1"
    assert strings["ProductName"] == "Fuzzy Duplicate Finder"
    assert strings["OriginalFilename"] == "FuzzyDuplicateFinder.exe"

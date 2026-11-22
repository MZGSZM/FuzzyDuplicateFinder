"""Offscreen GUI tests. They drive the real window without a display."""

import os
import time

import pytest
from conftest import make_image, make_text

QtWidgets = pytest.importorskip("PyQt6.QtWidgets")
from PyQt6.QtCore import QSettings  # noqa: E402
from PyQt6.QtWidgets import QApplication, QMessageBox  # noqa: E402

from fdf import gui  # noqa: E402
from fdf.matcher import Match  # noqa: E402
from fdf.settings import AppSettings  # noqa: E402
from fdf.theme import THEMES, ThemeManager, build_stylesheet  # noqa: E402


@pytest.fixture(scope="module")
def app():
    instance = QApplication.instance() or QApplication(["test"])
    yield instance


@pytest.fixture
def window(app, tmp_path, monkeypatch):
    monkeypatch.setattr(QMessageBox, "information", lambda *a, **k: None)
    monkeypatch.setattr(QMessageBox, "warning", lambda *a, **k: None)
    monkeypatch.setattr(QMessageBox, "critical", lambda *a, **k: None)
    q = QSettings(str(tmp_path / "settings.ini"), QSettings.Format.IniFormat)
    settings = AppSettings(q)
    settings.confirm_delete = False
    settings.low_priority = False
    theme = ThemeManager(app, settings=q)
    theme.apply()
    win = gui.DuplicateFinderApp(theme, settings)
    win.show()
    yield win
    win.close()


@pytest.fixture
def lib(tmp_path):
    a = tmp_path / "lib" / "a"
    b = tmp_path / "lib" / "b"
    a.mkdir(parents=True)
    b.mkdir(parents=True)
    make_image(a / "photo.jpg")
    (b / "photo_dup.jpg").write_bytes((a / "photo.jpg").read_bytes())
    make_image(b / "photo_small.jpg", size=(600, 450))
    make_text(a / "notes.txt")
    make_text(b / "notes2.txt", edit=lambda t: t + " tail")
    return a, b


def pump(app, condition, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        app.processEvents()
        if condition():
            return True
        time.sleep(0.01)
    return False


def run_scan(app, win):
    win.start_scan()
    assert pump(app, lambda: win.worker is not None and win.worker.isFinished())
    pump(app, lambda: not win._busy(), timeout=5)


def test_both_themes_build():
    for name in ("light", "dark"):
        assert "QPushButton" in build_stylesheet(THEMES[name])


def test_scan_review_delete(app, window, lib):
    a, b = lib
    for folder in (a, b):
        assert window._add_folder_path(str(folder))
    assert not window._add_folder_path(str(a))          # no duplicates in the list
    window._folders_changed()
    run_scan(app, window)

    assert window.model.rowCount() >= 3
    first = window.model.match_at(0)
    assert first.type == "EXACT"
    assert window.current_match is first
    assert pump(app, lambda: window.panel_a["details"].text().startswith("Size:"))

    window.combo_filter.setCurrentIndex(6)               # Text
    assert all(window.model.match_at(r).kind == "text" for r in range(window.model.rowCount()))
    window.combo_filter.setCurrentIndex(0)

    window._select_row(0)
    victim = window.current_match.file_b
    window.delete_files(("B",))
    assert not os.path.exists(victim)
    assert all(victim not in (m.file_a, m.file_b) for m in window.model.all_matches())
    assert window.exact_groups == []


def test_rematch_and_incremental(app, window, lib):
    a, b = lib
    window._add_folder_path(str(a))
    window._add_folder_path(str(b))
    window._folders_changed()
    run_scan(app, window)
    count = window.model.rowCount()
    assert os.path.exists(window._db_path())

    window.rematch()
    assert pump(app, lambda: window.worker.isFinished())
    pump(app, lambda: not window._busy(), timeout=5)
    assert window.model.rowCount() == count

    run_scan(app, window)
    assert "unchanged" in window.lbl_status.text()


def test_prune_plan_respects_priority(app, window, tmp_path):
    hi = tmp_path / "hi"
    lo = tmp_path / "lo"
    nested = lo / "nested"
    for d in (hi, nested):
        d.mkdir(parents=True)
    for d in (hi, lo, nested):
        (d / "f.bin").write_bytes(b"same")
    window.scan_folders = [
        {"path": str(hi), "priority": 50},
        {"path": str(lo), "priority": 10},
        {"path": str(nested), "priority": 5},
    ]
    assert window.get_folder_priority(str(nested / "f.bin")) == 5
    assert window.get_folder_priority(str(tmp_path / "hi2" / "f.bin")) == 10
    window.exact_groups = [[str(lo / "f.bin"), str(hi / "f.bin"), str(nested / "f.bin")]]
    plan = window.select_prune_victims()
    assert {v for v, _ in plan} == {str(lo / "f.bin"), str(nested / "f.bin")}
    assert {k for _, k in plan} == {str(hi / "f.bin")}


def test_model_filter_and_remove(app):
    model = gui.MatchListModel()
    matches = [
        Match("/a/x.jpg", "/b/x.jpg", 100.0, "EXACT", "image"),
        Match("/a/y.txt", "/b/y.txt", 80.0, "FUZZY", "text"),
        Match("/a/z.mp3", "/b/z.mp3", 75.0, "FUZZY", "audio"),
    ]
    model.set_matches(matches)
    assert model.rowCount() == 3
    model.set_filter("FUZZY", "")
    assert model.rowCount() == 2
    model.set_filter(None, "z.mp3")
    assert model.match_at(0) is matches[2]
    model.set_filter(None, "")
    model.remove_paths({"/b/y.txt"})
    assert model.rowCount() == 2


def test_settings_roundtrip(tmp_path):
    q = QSettings(str(tmp_path / "s.ini"), QSettings.Format.IniFormat)
    s = AppSettings(q)
    s.kinds = {"image", "text"}
    s.excludes = ("a", "b*")
    s.min_size_kb = 4
    s.index_mode = "custom"
    s.index_custom_dir = "/idx"
    again = AppSettings(q)
    assert again.kinds == {"image", "text"}
    assert again.excludes == ("a", "b*")
    assert again.index_mode == "custom" and again.index_custom_dir == "/idx"
    opts = again.scan_options(threads=3)
    assert opts.min_size == 4096 and opts.max_workers == 3
    s.kinds = set()
    assert AppSettings(q).kinds == frozenset()


# ---------------------------------------------------------------------------
# 1.7.0: theme button, menus, Re-match button, index location
# ---------------------------------------------------------------------------

def add_folders(window, *folders):
    for folder in folders:
        assert window._add_folder_path(str(folder))
    window._folders_changed()


def test_menus_and_toolbar_layout(window):
    titles = [a.text().replace("&", "") for a in window.menuBar().actions()]
    assert titles == ["File", "Scan", "Index", "Results", "Help"]
    toolbar_texts = {b.text().strip() for b in window.findChildren(QtWidgets.QPushButton)}
    assert "+ Add Folder" in toolbar_texts and "Options..." in toolbar_texts
    assert "Clear List" not in toolbar_texts
    assert "Load Index..." not in toolbar_texts
    assert "Re-match" in toolbar_texts


def test_theme_button_toggles_and_follows_system(app, window):
    button = window.btn_theme
    theme = window.theme
    theme.set_mode("light")
    assert theme.effective == "light"
    button.click()
    assert theme.mode == "dark" and theme.effective == "dark"
    assert "Dark theme." in button.toolTip()
    button.click()
    assert theme.mode == "light"

    menu = button.build_menu()
    labels = {a.text(): a for a in menu.actions()}
    assert set(labels) == {"Light", "Dark", "Follow system"}
    assert labels["Light"].isChecked()
    labels["Follow system"].trigger()
    assert theme.mode == "system"
    assert "following system" in button.toolTip()
    # From System, a click always changes the visible theme.
    before = theme.effective
    button.click()
    assert theme.effective != before
    assert not button.icon().isNull()


def test_theme_icons_render():
    from PyQt6.QtGui import QColor
    for effective in ("light", "dark"):
        icon = gui.theme_icon(effective, QColor("#888888"), 16, 2.0)
        image = icon.pixmap(16, 16).toImage()
        assert any(image.pixelColor(x, y).alpha() > 0 for x in range(16) for y in range(16))


def test_folder_list_is_not_saved(app, tmp_path, lib):
    from fdf.settings import LEGACY_FOLDERS_KEY
    q = QSettings(str(tmp_path / "legacy.ini"), QSettings.Format.IniFormat)
    q.setValue(LEGACY_FOLDERS_KEY, '[{"path": "/somewhere", "priority": 10}]')
    settings = AppSettings(q)
    theme = ThemeManager(app, settings=q)
    win = gui.DuplicateFinderApp(theme, settings)
    try:
        assert win.scan_folders == []
        assert not q.contains(LEGACY_FOLDERS_KEY)
        add_folders(win, *lib)
        win._save_window_state()
        assert not q.contains(LEGACY_FOLDERS_KEY)
    finally:
        win.close()


def test_rematch_button_tracks_threshold(app, window, lib):
    add_folders(window, *lib)
    assert not window.btn_rematch.isEnabled()          # no index yet
    run_scan(app, window)
    assert not window.btn_rematch.isEnabled()          # results match the threshold
    window.spin_threshold.setValue(window.spin_threshold.value() - 5)
    assert window.btn_rematch.isEnabled()
    window.btn_rematch.click()
    assert pump(app, lambda: window.worker.isFinished())
    pump(app, lambda: not window._busy(), timeout=5)
    assert not window.btn_rematch.isEnabled()


def test_in_folder_index_and_scan_log(app, window, lib):
    a, b = lib
    window.settings.index_mode = "folder"
    add_folders(window, a, b)
    run_scan(app, window)
    db = window.last_db_path
    assert os.path.dirname(db) == str(a)
    assert os.path.basename(db).startswith(".fdf-index-")
    assert os.path.isdir(a / ".fdf-logs")
    assert window._db_path() == db
    # Nothing in the app data indexes folder.
    from fdf.paths import index_dir
    assert os.listdir(index_dir()) == []
    # Deleting the saved index takes its scan log with it.
    import fdf.gui as gui_module
    original = gui_module.QMessageBox.question
    gui_module.QMessageBox.question = lambda *a, **k: QMessageBox.StandardButton.Yes
    try:
        window.forget_index()
    finally:
        gui_module.QMessageBox.question = original
    assert not os.path.exists(db)
    assert not os.path.exists(a / ".fdf-logs")


def test_ask_mode_asks_once_per_set(app, window, lib, tmp_path):
    window.settings.index_mode = "ask"
    asked = []
    target = str(tmp_path / "picked.db")

    def fake_ask(suggestion, title):
        asked.append(suggestion)
        return target

    window._ask_index_file = fake_ask
    add_folders(window, *lib)
    run_scan(app, window)
    run_scan(app, window)
    assert len(asked) == 1
    assert window.last_db_path == target and os.path.exists(target)


def test_holder_question_across_disks(app, window, lib, monkeypatch):
    from fdf import paths
    a, b = lib
    monkeypatch.setattr(paths, "spans_volumes", lambda _r: True)
    window.settings.index_mode = "folder"
    window._ask_index_holder = lambda roots: roots[1]
    add_folders(window, a, b)
    run_scan(app, window)
    assert os.path.dirname(window.last_db_path) == str(b)


def test_unwritable_location_does_not_scan(app, window, lib, monkeypatch):
    from fdf import paths
    monkeypatch.setattr(paths, "dir_writable", lambda _p: False)
    monkeypatch.setattr(gui, "dir_writable", lambda _p: False)
    window.settings.index_mode = "folder"
    offered = []
    window._ask_other_location = lambda folder, roots: offered.append(folder)
    add_folders(window, *lib)
    window.start_scan()
    assert offered == [str(lib[0])]
    assert window.worker is None
    from fdf.paths import index_dir
    assert os.listdir(index_dir()) == []

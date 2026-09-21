"""
Fuzzy Duplicate Finder.

"""

import math
import multiprocessing
import os
import subprocess
import sys
import time
from datetime import datetime

import cv2
from PyQt6.QtCore import QSize, Qt, QThread, QTimer, QUrl, pyqtSignal
from PyQt6.QtGui import (QAction, QActionGroup, QDesktopServices, QIcon, QImage,
                         QImageReader, QPixmap)
from PyQt6.QtWidgets import (QAbstractItemView, QApplication, QComboBox, QDialog,
                             QFileDialog, QFrame, QHBoxLayout, QHeaderView, QLabel,
                             QListWidget, QListWidgetItem, QMainWindow, QMessageBox,
                             QProgressBar, QProgressDialog, QPushButton, QSizePolicy,
                             QSplitter, QTableWidget, QTableWidgetItem, QVBoxLayout,
                             QWidget)
from send2trash import send2trash

from matcher import Matcher
from scanner_engine import AUDIO_EXTS, IMAGE_EXTS, VIDEO_EXTS, DatabaseManager, Scanner
from theme import MODE_LABELS, MODES, ThemeManager

# Version Info
VERSION = "1.5.0"
APP_NAME = "Fuzzy Duplicate Finder"
GITHUB_URL = "https://github.com/MZGSZM/FuzzyDuplicateFinder"

# Matches assets/fuzzy-duplicate-finder.desktop. On Wayland the compositor
# picks the taskbar icon from the desktop entry, not from setWindowIcon.
DESKTOP_FILE_NAME = "fuzzy-duplicate-finder"

# Consolidated extension sets used by the UI (kept in sync with scanner_engine)
UI_IMAGE_EXTS = IMAGE_EXTS
UI_VIDEO_EXTS = VIDEO_EXTS
UI_AUDIO_EXTS = AUDIO_EXTS

DEFAULT_FOLDER_PRIORITY = 10

# Largest dimension a preview is decoded at. Full-resolution decodes of large
# photographs cost hundreds of megabytes and are pointless for a preview pane.
PREVIEW_MAX_DIM = 2400

PROGRESS_STEPS = 1000

ICON_SIZES = (16, 24, 32, 48, 64, 128, 256, 512)


# -----------------------------------------------------------------------------
# Resources
# -----------------------------------------------------------------------------

def resource_path(*parts):
    """Path to a bundled resource, from source or from a PyInstaller build."""
    base = getattr(sys, "_MEIPASS", os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base, *parts)


def load_app_icon():
    """
    Build the application icon from the pre-rendered PNG set.

    The PNGs are generated from assets/icon.svg (and icon-small.svg for 16-32
    px) by tools/build_icons.py. Loading PNGs rather than the SVG avoids a
    runtime dependency on Qt's SVG image plugin in frozen builds, and lets the
    small sizes use the simplified artwork. The SVG is the fallback.
    """
    icon = QIcon()
    for size in ICON_SIZES:
        path = resource_path("assets", "icons", f"icon-{size}.png")
        if os.path.exists(path):
            icon.addFile(path, QSize(size, size))
    if icon.isNull():
        svg = resource_path("assets", "icon.svg")
        if os.path.exists(svg):
            icon = QIcon(svg)
    return icon


# -----------------------------------------------------------------------------
# Styling helpers. Colours live in theme.py; widgets only declare what they are.
# -----------------------------------------------------------------------------

def set_variant(button, variant):
    button.setProperty("variant", variant)
    button.setCursor(Qt.CursorShape.PointingHandCursor)
    return button


def set_role(label, role):
    label.setProperty("role", role)
    return label


def repolish(widget):
    """Re-evaluate property selectors after a dynamic property change."""
    style = widget.style()
    style.unpolish(widget)
    style.polish(widget)
    widget.update()


def set_preview_kind(label, kind):
    if label.property("kind") != kind:
        label.setProperty("kind", kind)
        repolish(label)


def make_arrow_button(text):
    return set_variant(QPushButton(text), "arrow")


# -----------------------------------------------------------------------------
# Misc helpers
# -----------------------------------------------------------------------------

def format_size(size_bytes):
    if not size_bytes:
        return "0 B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    i = max(0, min(i, len(size_name) - 1))
    p = math.pow(1024, i)
    return f"{round(size_bytes / p, 2)} {size_name[i]}"


def open_file_external(filepath):
    """Open a file in the OS default application."""
    try:
        if os.name == 'nt':
            os.startfile(filepath)
        elif sys.platform == 'darwin':
            subprocess.Popen(['open', filepath])
        else:
            subprocess.Popen(['xdg-open', filepath])
    except Exception as exc:
        print(f"Failed to open file: {exc}")


# -----------------------------------------------------------------------------
# Dialogs
# -----------------------------------------------------------------------------

class SkippedFileDialog(QDialog):
    def __init__(self, skipped_files, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Files With Issues")
        self.resize(600, 400)
        self.skipped_files = skipped_files

        layout = QVBoxLayout(self)

        lbl = QLabel(
            f"{len(skipped_files)} file(s) could not be fully processed "
            f"(permission denied, corrupted, or unreadable). They are still "
            f"indexed for exact-duplicate detection where possible:"
        )
        lbl.setWordWrap(True)
        layout.addWidget(lbl)

        self.list_widget = QListWidget()
        self.list_widget.addItems(skipped_files)
        layout.addWidget(self.list_widget)

        btn_layout = QHBoxLayout()
        btn_export = QPushButton("Export List to TXT")
        btn_export.clicked.connect(self.export_list)
        btn_close = QPushButton("Close")
        btn_close.clicked.connect(self.close)

        btn_layout.addStretch()
        btn_layout.addWidget(btn_export)
        btn_layout.addWidget(btn_close)
        layout.addLayout(btn_layout)

    def export_list(self):
        path, _ = QFileDialog.getSaveFileName(
            self, "Export Skipped Files", "skipped_files.txt", "Text Files (*.txt)"
        )
        if path:
            try:
                with open(path, "w", encoding="utf-8") as handle:
                    handle.write("\n".join(self.skipped_files))
                QMessageBox.information(self, "Export Successful", f"Saved to {path}")
            except Exception as exc:
                QMessageBox.critical(self, "Error", str(exc))


# -----------------------------------------------------------------------------
# Workers
# -----------------------------------------------------------------------------

class ScanAndMatchWorker(QThread):
    progress_update = pyqtSignal(str)
    progress_value = pyqtSignal(int, int)      # current, total
    scan_complete = pyqtSignal(list)           # skipped files
    # Named so it does not shadow QThread.finished, which has a different
    # signature and is used by Qt's own cleanup idioms.
    matching_finished = pyqtSignal(list, list)  # matches, exact groups (paths)
    error = pyqtSignal(str)
    aborted = pyqtSignal()

    def __init__(self, folder_list, db_path, skip_scan=False,
                 scan_workers=None, match_workers=None):
        super().__init__()
        self.folder_list = folder_list
        self.db_path = db_path
        self.skip_scan = skip_scan
        self.scan_workers = scan_workers
        self.match_workers = match_workers
        self._is_running = True

    def stop(self):
        self._is_running = False

    def is_stopped(self):
        return not self._is_running

    def on_scan_progress(self, current, total, skipped):
        if self._is_running:
            self.progress_value.emit(current, total)
            self.progress_update.emit(
                f"Scanning: {current} / {total} files  (Issues: {skipped})"
            )

    def on_match_progress(self, current, total):
        if self._is_running:
            self.progress_value.emit(current, total)

    def run(self):
        matcher = None
        try:
            if not self.skip_scan:
                self.progress_update.emit("Phase 1: Indexing files...")
                scanner = Scanner()
                result_db, skipped_list = scanner.scan_directory(
                    self.folder_list,
                    self.db_path,
                    stop_signal=self.is_stopped,
                    progress_callback=self.on_scan_progress,
                    max_workers=self.scan_workers,
                )

                if self.is_stopped():
                    self.aborted.emit()
                    return
                if not result_db:
                    self.error.emit("Database creation failed.")
                    return

                self.db_path = result_db
                self.scan_complete.emit(skipped_list)
            else:
                self.progress_update.emit("Skipping scan. Loading existing index...")
                self.scan_complete.emit([])

            self.progress_update.emit("Phase 2: Analyzing content...")
            matcher = Matcher(self.db_path)

            exact_groups = matcher.find_exact_duplicates()
            if self.is_stopped():
                self.aborted.emit()
                return

            fuzzy = matcher.find_fuzzy_matches(
                stop_signal=self.is_stopped,
                progress_callback=self.on_match_progress,
                max_workers=self.match_workers,
            )

            if self.is_stopped():
                self.aborted.emit()
                return

            self.progress_update.emit("Finalizing matches...")

            final_matches = []
            group_paths = []

            for group in exact_groups:
                paths = [f['path'] for f in group]
                group_paths.append(paths)
                base = paths[0]
                for duplicate in paths[1:]:
                    final_matches.append({
                        'file_a': base,
                        'file_b': duplicate,
                        'score': 100.0,
                        'type': 'EXACT',
                    })

            for match in fuzzy:
                match['type'] = 'FUZZY'
                final_matches.append(match)

            final_matches.sort(key=lambda m: m['score'], reverse=True)
            self.matching_finished.emit(final_matches, group_paths)

        except Exception as exc:
            # A failure that happens while stopping is an abort, not an error,
            # but it must still emit something or the UI stays disabled.
            if self.is_stopped():
                self.aborted.emit()
            else:
                self.error.emit(str(exc))
        finally:
            if matcher is not None:
                matcher.close()


class AutoPruneWorker(QThread):
    progress_update = pyqtSignal(str)
    progress_value = pyqtSignal(int, int)
    prune_finished = pyqtSignal(int, list)   # deleted count, deleted paths
    error = pyqtSignal(str)
    aborted = pyqtSignal()

    def __init__(self, files_to_trash):
        super().__init__()
        self.files_to_trash = list(files_to_trash)
        self._is_running = True

    def stop(self):
        self._is_running = False

    def is_stopped(self):
        return not self._is_running

    def run(self):
        deleted = []
        try:
            total = len(self.files_to_trash)
            for i, filepath in enumerate(self.files_to_trash):
                if self.is_stopped():
                    self.aborted.emit()
                    return
                try:
                    if os.path.exists(filepath):
                        send2trash(filepath)
                        deleted.append(filepath)
                except Exception as exc:
                    print(f"Failed to trash {filepath}: {exc}")

                self.progress_value.emit(i + 1, total)
                self.progress_update.emit(f"Pruning: {i + 1} / {total} files")

            self.prune_finished.emit(len(deleted), deleted)
        except Exception as exc:
            if self.is_stopped():
                self.aborted.emit()
            else:
                self.error.emit(str(exc))


# -----------------------------------------------------------------------------
# Widgets
# -----------------------------------------------------------------------------

class ThreadCountWidget(QWidget):
    """
    A down/value/up stepper that matches the folder-priority arrow buttons.
    Exposes .value() so it is a drop-in for the QSpinBox it replaces.
    """

    def __init__(self, min_val=1, max_val=8, default=4, parent=None):
        super().__init__(parent)
        self._value = max(min_val, min(default, max_val))
        self._min = min_val
        self._max = max_val

        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(2)

        btn_down = make_arrow_button("\u25bc")
        btn_down.clicked.connect(self._decrement)

        self._lbl = set_role(QLabel(str(self._value)), "stepper")
        self._lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)

        btn_up = make_arrow_button("\u25b2")
        btn_up.clicked.connect(self._increment)

        layout.addWidget(btn_down)
        layout.addWidget(self._lbl)
        layout.addWidget(btn_up)

    def _increment(self):
        if self._value < self._max:
            self._value += 1
            self._lbl.setText(str(self._value))

    def _decrement(self):
        if self._value > self._min:
            self._value -= 1
            self._lbl.setText(str(self._value))

    def value(self):
        return self._value


# -----------------------------------------------------------------------------
# Main window
# -----------------------------------------------------------------------------

class DuplicateFinderApp(QMainWindow):
    def __init__(self, theme):
        super().__init__()
        self.theme = theme
        self.setWindowTitle(APP_NAME)
        self.resize(1400, 950)

        self.scan_folders = []
        self.matches = []
        self.exact_groups = []
        self.skipped_files = []
        self.current_match_index = -1
        self.current_db_path = None
        self.worker = None
        self.prune_worker = None
        self.prune_progress_dialog = None
        self.pixmap_cache = {'A': None, 'B': None}

        # Rescaling a large pixmap on every resize event makes dragging the
        # window stutter, so coalesce them.
        self._resize_timer = QTimer(self)
        self._resize_timer.setSingleShot(True)
        self._resize_timer.setInterval(80)
        self._resize_timer.timeout.connect(self._apply_cached_pixmaps)

        self._build_menus()
        self._build_status_bar()
        self._build_body()

        self.theme.mode_changed.connect(self._sync_theme_controls)
        self._sync_theme_controls(self.theme.mode)

    # -------------------------------------------------------------------------
    # Construction
    # -------------------------------------------------------------------------

    def _build_menus(self):
        menubar = self.menuBar()

        file_menu = menubar.addMenu("&File")
        exit_action = QAction("E&xit", self)
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)

        view_menu = menubar.addMenu("&View")
        theme_menu = view_menu.addMenu("&Theme")
        self._theme_group = QActionGroup(self)
        self._theme_group.setExclusive(True)
        self._theme_actions = {}
        for mode in MODES:
            action = QAction(MODE_LABELS[mode], self, checkable=True)
            action.setData(mode)
            action.triggered.connect(lambda checked, m=mode: self.theme.set_mode(m))
            self._theme_group.addAction(action)
            theme_menu.addAction(action)
            self._theme_actions[mode] = action

        tools_menu = menubar.addMenu("&Tools")
        prune_exact_action = QAction("Auto-Prune Exact Duplicates...", self)
        prune_exact_action.triggered.connect(self.auto_prune_exact)
        tools_menu.addAction(prune_exact_action)

        help_menu = menubar.addMenu("&Help")
        repo_action = QAction("GitHub Repository", self)
        repo_action.triggered.connect(self.open_github)
        help_menu.addAction(repo_action)
        about_action = QAction(f"&About {APP_NAME}", self)
        about_action.triggered.connect(self.show_about)
        help_menu.addAction(about_action)

    def _build_status_bar(self):
        self.status_bar = self.statusBar()
        self.lbl_status = set_role(QLabel("Ready"), "status")
        self.status_bar.addWidget(self.lbl_status)

        self.lbl_version = set_role(QLabel(f"v{VERSION}"), "version")
        self.lbl_version.setCursor(Qt.CursorShape.PointingHandCursor)
        self.lbl_version.setToolTip(GITHUB_URL)
        self.lbl_version.mousePressEvent = lambda event: self.open_github()
        self.status_bar.addPermanentWidget(self.lbl_version)

    def _build_toolbar(self):
        toolbar = QFrame()
        toolbar.setObjectName("toolbar")
        row = QHBoxLayout(toolbar)
        row.setContentsMargins(8, 6, 8, 6)
        row.setSpacing(6)

        btn_add_folder = set_variant(QPushButton(" + Add Folder "), "neutral")
        btn_add_folder.clicked.connect(self.add_folder)

        btn_clear_folders = set_variant(QPushButton(" Clear List "), "neutral")
        btn_clear_folders.clicked.connect(self.clear_folders)

        btn_load_index = set_variant(QPushButton(" Load Index... "), "neutral")
        btn_load_index.clicked.connect(self.load_index)

        self.btn_scan = set_variant(QPushButton("  START SCAN  "), "primary")
        self.btn_scan.setEnabled(False)
        self.btn_scan.clicked.connect(self.start_scan)

        self.btn_stop = set_variant(QPushButton("  STOP  "), "danger")
        self.btn_stop.setEnabled(False)
        self.btn_stop.clicked.connect(self.stop_scan)

        self.btn_skipped = set_variant(QPushButton("0 Skipped"), "warn-link")
        self.btn_skipped.clicked.connect(self.show_skipped_dialog)
        self.btn_skipped.hide()

        lbl_threads = set_role(QLabel("Threads:"), "muted")

        cpu_count = os.cpu_count() or 4
        self.spin_workers = ThreadCountWidget(
            min_val=1,
            max_val=cpu_count * 2,
            default=cpu_count,
        )
        self.spin_workers.setToolTip(
            "Maximum worker threads used while indexing files.\n"
            f"Your system reports {cpu_count} logical CPU core(s).\n"
            "The matching phase sizes itself separately and never exceeds "
            "your core count."
        )

        lbl_theme = set_role(QLabel("Theme:"), "muted")
        self.theme_combo = QComboBox()
        for mode in MODES:
            self.theme_combo.addItem(MODE_LABELS[mode], mode)
        self.theme_combo.setToolTip("System follows your desktop's light/dark setting.")
        self.theme_combo.currentIndexChanged.connect(
            lambda index: self.theme.set_mode(self.theme_combo.itemData(index))
        )

        row.addWidget(btn_add_folder)
        row.addWidget(btn_clear_folders)
        row.addWidget(btn_load_index)
        row.addSpacing(20)
        row.addWidget(self.btn_scan)
        row.addWidget(self.btn_stop)
        row.addWidget(self.btn_skipped)
        row.addStretch()
        row.addWidget(lbl_threads)
        row.addWidget(self.spin_workers)
        row.addSpacing(16)
        row.addWidget(lbl_theme)
        row.addWidget(self.theme_combo)
        return toolbar

    def _build_body(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        main_layout.setContentsMargins(6, 6, 6, 6)

        v_splitter = QSplitter(Qt.Orientation.Vertical)
        v_splitter.setHandleWidth(1)

        # Top panel
        top_container = QWidget()
        top_layout = QVBoxLayout(top_container)
        top_layout.setContentsMargins(0, 0, 0, 6)
        top_layout.setSpacing(6)

        top_layout.addWidget(self._build_toolbar())

        self.folder_table = QTableWidget()
        self.folder_table.setColumnCount(2)
        self.folder_table.setHorizontalHeaderLabels(["Folder Path", "Priority"])
        self.folder_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.folder_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Fixed)
        self.folder_table.setColumnWidth(1, 90)
        self.folder_table.verticalHeader().setVisible(False)
        self.folder_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        top_layout.addWidget(self.folder_table)

        self.progress_bar = QProgressBar()
        self.progress_bar.setFixedHeight(5)
        self.progress_bar.setTextVisible(False)
        self.progress_bar.hide()
        top_layout.addWidget(self.progress_bar)

        v_splitter.addWidget(top_container)

        # Bottom panel
        bottom_container = QWidget()
        bottom_layout = QHBoxLayout(bottom_container)
        bottom_layout.setContentsMargins(0, 0, 0, 0)

        h_splitter = QSplitter(Qt.Orientation.Horizontal)
        h_splitter.setHandleWidth(1)

        self.match_list = QListWidget()
        self.match_list.setObjectName("matchList")
        self.match_list.setFrameShape(QFrame.Shape.NoFrame)
        self.match_list.currentRowChanged.connect(self.load_match_details)
        h_splitter.addWidget(self.match_list)

        comparison_widget = QWidget()
        comparison_widget.setObjectName("comparePane")
        # Needed for a QWidget subclass-less container to paint its background.
        comparison_widget.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)
        comp_layout = QVBoxLayout(comparison_widget)

        preview_splitter = QSplitter(Qt.Orientation.Horizontal)
        preview_splitter.setHandleWidth(1)
        self.panel_a = self.create_file_panel("Original / File A")
        self.panel_b = self.create_file_panel("Duplicate / File B")
        preview_splitter.addWidget(self.panel_a['container'])
        preview_splitter.addWidget(self.panel_b['container'])
        comp_layout.addWidget(preview_splitter, stretch=1)

        action_frame = QFrame()
        action_frame.setObjectName("actionBar")
        action_layout = QHBoxLayout(action_frame)

        self.lbl_score = set_role(QLabel("0%"), "score")

        btn_del_a = set_variant(QPushButton("Delete File A"), "danger")
        btn_del_a.clicked.connect(lambda: self.delete_file("A"))

        btn_del_both = set_variant(QPushButton("Delete Both Files"), "danger")
        btn_del_both.clicked.connect(self.delete_both_files)

        btn_keep = set_variant(QPushButton("Skip / Keep Both"), "neutral")
        btn_keep.clicked.connect(self.next_match)

        btn_del_b = set_variant(QPushButton("Delete File B"), "danger")
        btn_del_b.clicked.connect(lambda: self.delete_file("B"))

        action_layout.addStretch()
        action_layout.addWidget(btn_del_a)
        action_layout.addSpacing(20)
        action_layout.addWidget(btn_del_both)
        action_layout.addSpacing(20)
        action_layout.addWidget(self.lbl_score)
        action_layout.addWidget(btn_keep)
        action_layout.addSpacing(20)
        action_layout.addWidget(btn_del_b)
        action_layout.addStretch()

        comp_layout.addWidget(action_frame)

        h_splitter.addWidget(comparison_widget)
        h_splitter.setSizes([350, 900])

        bottom_layout.addWidget(h_splitter)
        v_splitter.addWidget(bottom_container)
        v_splitter.setSizes([220, 700])

        main_layout.addWidget(v_splitter)

    def create_file_panel(self, title):
        container = QWidget()
        layout = QVBoxLayout(container)
        layout.setContentsMargins(10, 10, 10, 10)

        lbl_title = set_role(QLabel(title), "panelTitle")
        lbl_title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(lbl_title)

        lbl_img = QLabel()
        lbl_img.setObjectName("preview")
        lbl_img.setProperty("kind", "image")
        lbl_img.setAlignment(Qt.AlignmentFlag.AlignCenter)
        lbl_img.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Ignored)
        lbl_img.setScaledContents(False)
        layout.addWidget(lbl_img)
        layout.setStretchFactor(lbl_img, 1)

        meta_frame = QFrame()
        meta_frame.setObjectName("metaFrame")
        meta_layout = QVBoxLayout(meta_frame)

        lbl_filename = set_role(QLabel("Filename"), "filename")
        lbl_filename.setWordWrap(True)

        lbl_path = set_role(QLabel("Path"), "path")
        lbl_path.setWordWrap(True)

        lbl_details = set_role(QLabel("Details"), "details")
        lbl_dates = set_role(QLabel("Dates"), "dates")

        btn_open = set_variant(QPushButton("Open in Viewer"), "link")

        meta_layout.addWidget(lbl_filename)
        meta_layout.addWidget(lbl_path)
        meta_layout.addWidget(lbl_details)
        meta_layout.addWidget(lbl_dates)
        meta_layout.addWidget(btn_open)
        layout.addWidget(meta_frame)

        return {
            "container": container,
            "img": lbl_img,
            "filename": lbl_filename,
            "path": lbl_path,
            "details": lbl_details,
            "dates": lbl_dates,
            "btn_open": btn_open,
            "filepath": None,
        }

    # -------------------------------------------------------------------------
    # Theme
    # -------------------------------------------------------------------------

    def _sync_theme_controls(self, mode):
        """Keep the menu and the toolbar selector in step with each other."""
        action = self._theme_actions.get(mode)
        if action is not None:
            action.setChecked(True)
        index = self.theme_combo.findData(mode)
        if index >= 0 and index != self.theme_combo.currentIndex():
            self.theme_combo.blockSignals(True)
            self.theme_combo.setCurrentIndex(index)
            self.theme_combo.blockSignals(False)

    # -------------------------------------------------------------------------
    # Folder management
    # -------------------------------------------------------------------------

    def add_folder(self):
        folder = QFileDialog.getExistingDirectory(self, "Select Directory")
        if folder:
            for existing in self.scan_folders:
                if existing['path'] == folder:
                    return
            self.scan_folders.append({'path': folder, 'priority': DEFAULT_FOLDER_PRIORITY})
            self.refresh_folder_table()
            self.btn_scan.setEnabled(True)

    def clear_folders(self):
        self.scan_folders = []
        self.refresh_folder_table()
        self.btn_scan.setEnabled(False)

    def load_index(self):
        db_path, _ = QFileDialog.getOpenFileName(
            self, "Load Existing Index", "", "Database Files (*.db)"
        )
        if db_path:
            self.current_db_path = db_path
            try:
                db = DatabaseManager(db_path)
                roots = db.get_roots()
                db.close()
            except Exception as exc:
                QMessageBox.critical(self, "Error", f"Could not open index: {exc}")
                self.current_db_path = None
                return
            if roots:
                self.scan_folders = roots
                self.refresh_folder_table()
            self.start_worker(skip_scan=True)

    def refresh_folder_table(self):
        self.folder_table.setRowCount(len(self.scan_folders))
        for i, folder_data in enumerate(self.scan_folders):
            path_item = QTableWidgetItem(folder_data['path'])
            path_item.setFlags(Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable)
            self.folder_table.setItem(i, 0, path_item)

            priority_widget = QWidget()
            priority_layout = QHBoxLayout(priority_widget)
            priority_layout.setContentsMargins(4, 0, 4, 0)
            priority_layout.setSpacing(2)

            lbl_value = set_role(QLabel(str(folder_data['priority'])), "stepper")
            lbl_value.setAlignment(Qt.AlignmentFlag.AlignCenter)

            btn_up = make_arrow_button("\u25b2")
            btn_down = make_arrow_button("\u25bc")

            btn_up.folder_index = i
            btn_up.priority_label = lbl_value
            btn_down.folder_index = i
            btn_down.priority_label = lbl_value

            btn_up.clicked.connect(self._on_priority_up_clicked)
            btn_down.clicked.connect(self._on_priority_down_clicked)

            priority_layout.addWidget(btn_down)
            priority_layout.addWidget(lbl_value)
            priority_layout.addWidget(btn_up)
            self.folder_table.setCellWidget(i, 1, priority_widget)

    def _adjust_priority(self, delta):
        btn = self.sender()
        idx = getattr(btn, 'folder_index', None)
        label = getattr(btn, 'priority_label', None)
        if idx is None or idx >= len(self.scan_folders):
            return
        new_val = max(0, min(100, self.scan_folders[idx]['priority'] + delta))
        self.scan_folders[idx]['priority'] = new_val
        if label is not None:
            label.setText(str(new_val))
        self.persist_folder_priorities()

    def _on_priority_up_clicked(self):
        self._adjust_priority(1)

    def _on_priority_down_clicked(self):
        self._adjust_priority(-1)

    # -------------------------------------------------------------------------
    # Scanning
    # -------------------------------------------------------------------------

    def start_scan(self):
        if not self.scan_folders:
            return
        if not self.current_db_path:
            if len(self.scan_folders) == 1:
                self.current_db_path = os.path.join(
                    self.scan_folders[0]['path'], "duplicate_index.db"
                )
            else:
                save_path, _ = QFileDialog.getSaveFileName(
                    self, "Save Database Location",
                    "duplicate_index.db", "Database Files (*.db)"
                )
                if save_path:
                    self.current_db_path = save_path
                else:
                    return

        if os.path.exists(self.current_db_path):
            confirm = QMessageBox.question(
                self, "Database Exists",
                f"Database already exists at:\n\n{self.current_db_path}\n\n"
                f"Overwrite and start fresh scan?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            )
            if confirm != QMessageBox.StandardButton.Yes:
                return

            for db_file in [
                self.current_db_path,
                self.current_db_path + "-shm",
                self.current_db_path + "-wal",
            ]:
                try:
                    if os.path.exists(db_file):
                        os.remove(db_file)
                except Exception as exc:
                    QMessageBox.warning(self, "Warning", f"Could not delete old database: {exc}")
                    return

        self.start_worker(skip_scan=False)

    def start_worker(self, skip_scan=False):
        self.lbl_status.setText("Working...")
        self.match_list.clear()
        self.matches = []
        self.exact_groups = []
        self.current_match_index = -1
        self.btn_skipped.hide()
        self.skipped_files = []

        self.progress_bar.setRange(0, 0)
        self.progress_bar.show()

        self.btn_scan.setEnabled(False)
        self.btn_stop.setEnabled(True)

        cpu_count = os.cpu_count() or 4
        self.worker = ScanAndMatchWorker(
            self.scan_folders,
            self.current_db_path,
            skip_scan=skip_scan,
            scan_workers=self.spin_workers.value(),
            # Matching runs in processes, so oversubscribing cores costs
            # memory and context switches rather than buying throughput.
            match_workers=min(self.spin_workers.value(), cpu_count),
        )
        self.worker.progress_update.connect(self.lbl_status.setText)
        self.worker.progress_value.connect(self.update_progress_bar)
        self.worker.scan_complete.connect(self.on_scan_phase_complete)
        self.worker.matching_finished.connect(self.on_process_complete)
        self.worker.aborted.connect(self.on_scan_aborted)
        self.worker.error.connect(self.on_error)
        self.worker.start()

    def update_progress_bar(self, current, total):
        """
        Normalise to a fixed step count.

        Qt's progress bar range is a C int. The matching phase used to report
        n*(n-1)/2 directly, which overflows above roughly 65,500 files.
        """
        self.progress_bar.show()
        if total <= 0:
            self.progress_bar.setRange(0, 0)
            return
        self.progress_bar.setRange(0, PROGRESS_STEPS)
        value = int(max(0, min(current, total)) * PROGRESS_STEPS / total)
        self.progress_bar.setValue(value)

    def on_scan_phase_complete(self, skipped_list):
        self.skipped_files = skipped_list
        if skipped_list:
            count = len(skipped_list)
            noun = "File" if count == 1 else "Files"
            self.btn_skipped.setText(f"{count} {noun} With Issues (View)")
            self.btn_skipped.show()

    def show_skipped_dialog(self):
        if not self.skipped_files:
            return
        SkippedFileDialog(self.skipped_files, self).exec()

    def stop_scan(self):
        if self.worker and self.worker.isRunning():
            self.lbl_status.setText("Stopping... please wait.")
            self.btn_stop.setEnabled(False)
            self.worker.stop()

    def on_scan_aborted(self):
        self.progress_bar.hide()
        self.btn_scan.setEnabled(True)
        self.btn_stop.setEnabled(False)
        self.lbl_status.setText("Operation aborted.")

    def on_process_complete(self, matches, exact_groups):
        self.progress_bar.hide()
        self.btn_scan.setEnabled(True)
        self.btn_stop.setEnabled(False)
        self.matches = matches
        self.exact_groups = exact_groups
        self.lbl_status.setText(f"Found {len(matches)} duplicate pair(s).")
        self._rebuild_match_list(0 if matches else -1)
        if not matches:
            QMessageBox.information(self, "Clean!", "No duplicates found.")

    def on_error(self, message):
        self.progress_bar.hide()
        self.btn_scan.setEnabled(True)
        self.btn_stop.setEnabled(False)
        QMessageBox.critical(self, "Error", message)

    # -------------------------------------------------------------------------
    # Match display
    # -------------------------------------------------------------------------

    def _rebuild_match_list(self, select_row):
        self.match_list.blockSignals(True)
        self.match_list.clear()
        for match in self.matches:
            name_a = os.path.basename(match['file_a'])
            prefix = "[=]" if match['type'] == 'EXACT' else f"[{int(match['score'])}%]"
            self.match_list.addItem(QListWidgetItem(f"{prefix} {name_a}"))
        self.match_list.blockSignals(False)

        self.current_match_index = -1
        if not self.matches or select_row < 0:
            return
        row = max(0, min(select_row, len(self.matches) - 1))
        self.match_list.setCurrentRow(row)
        # setCurrentRow does not emit currentRowChanged when the index is
        # unchanged, so drive the panels explicitly.
        self.load_match_details(row)
        self.match_list.scrollToItem(self.match_list.currentItem())

    def load_match_details(self, row_index):
        if row_index < 0 or row_index >= len(self.matches):
            return
        data = self.matches[row_index]
        self.current_match_index = row_index
        score_text = "Exact Match" if data['type'] == 'EXACT' else f"{int(data['score'])}% Match"
        self.lbl_score.setText(score_text)
        self.load_file_to_panel(self.panel_a, data['file_a'], 'A')
        self.load_file_to_panel(self.panel_b, data['file_b'], 'B')

    @staticmethod
    def _load_preview_image(filepath, panel_size):
        """
        Decode an image at preview resolution.

        Returns (pixmap|None, "WxH"|""). QImageReader can scale during decode,
        so a 100 megapixel photograph never becomes a 400 MB QPixmap.
        """
        reader = QImageReader(filepath)
        reader.setAutoTransform(True)
        source = reader.size()
        res_str = f"{source.width()}x{source.height()}" if source.isValid() else ""

        if source.isValid():
            target = max(PREVIEW_MAX_DIM, panel_size.width(), panel_size.height())
            longest = max(source.width(), source.height())
            if longest > target:
                scale = target / longest
                reader.setScaledSize(source * scale)

        image = reader.read()
        if image.isNull():
            return None, res_str
        return QPixmap.fromImage(image), res_str

    def _clear_panel(self, panel, cache_key):
        panel['filepath'] = None
        self.pixmap_cache[cache_key] = None
        set_preview_kind(panel['img'], "image")
        panel['img'].setPixmap(QPixmap())
        panel['img'].setText("")
        for key in ('filename', 'path', 'details', 'dates'):
            panel[key].setText("")

    def load_file_to_panel(self, panel, filepath, cache_key):
        panel['filepath'] = filepath
        self.pixmap_cache[cache_key] = None

        set_preview_kind(panel['img'], "image")
        panel['img'].setPixmap(QPixmap())
        panel['img'].setText("")

        if not os.path.exists(filepath):
            panel['filename'].setText("File Missing")
            panel['path'].setText(filepath)
            panel['details'].setText("")
            panel['dates'].setText("")
            panel['img'].setText("Missing on Disk")
            return

        try:
            stats = os.stat(filepath)
        except OSError as exc:
            panel['filename'].setText("Unreadable")
            panel['path'].setText(filepath)
            panel['details'].setText(str(exc))
            panel['img'].setText("Unreadable")
            return

        size_str = format_size(stats.st_size)
        ext = os.path.splitext(filepath)[1].lower()
        c_time = datetime.fromtimestamp(stats.st_ctime).strftime('%Y-%m-%d %H:%M')
        m_time = datetime.fromtimestamp(stats.st_mtime).strftime('%Y-%m-%d %H:%M')

        panel['dates'].setText(f"Created: {c_time}  |  Modified: {m_time}")
        panel['filename'].setText(os.path.basename(filepath))
        panel['path'].setText(os.path.dirname(filepath))

        try:
            panel['btn_open'].clicked.disconnect()
        except TypeError:
            pass
        panel['btn_open'].clicked.connect(
            lambda checked=False, target=filepath: open_file_external(target)
        )

        res_str = ""
        extra_str = ""

        if ext in UI_IMAGE_EXTS:
            try:
                pixmap, res_str = self._load_preview_image(filepath, panel['img'].size())
                if pixmap is not None:
                    self.pixmap_cache[cache_key] = pixmap
                    self.update_image_display(panel, pixmap)
                else:
                    panel['img'].setText("Image Error")
            except Exception:
                panel['img'].setText("Image Error")

        elif ext in UI_VIDEO_EXTS:
            cap = None
            try:
                cap = cv2.VideoCapture(filepath)
                if not cap.isOpened():
                    panel['img'].setText("Video File")
                else:
                    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
                    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
                    res_str = f"{width}x{height}"
                    fps = cap.get(cv2.CAP_PROP_FPS)
                    frame_count = cap.get(cv2.CAP_PROP_FRAME_COUNT)
                    if fps > 0 and frame_count > 0:
                        mins, secs = divmod(int(frame_count / fps), 60)
                        extra_str = f"Duration: {mins}:{secs:02d}"
                        cap.set(cv2.CAP_PROP_POS_FRAMES, int(frame_count // 3))

                    ret, frame = cap.read()
                    if not ret:
                        cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                        ret, frame = cap.read()

                    if ret:
                        frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                        h_img, w_img, channels = frame.shape
                        qimg = QImage(
                            frame.data, w_img, h_img,
                            channels * w_img, QImage.Format.Format_RGB888
                        ).copy()
                        pixmap = QPixmap.fromImage(qimg)
                        self.pixmap_cache[cache_key] = pixmap
                        self.update_image_display(panel, pixmap)
                    else:
                        panel['img'].setText("No Preview")
            except Exception:
                panel['img'].setText("Video Error")
            finally:
                if cap is not None:
                    cap.release()

        elif ext in UI_AUDIO_EXTS:
            set_preview_kind(panel['img'], "audio")
            panel['img'].setText("Audio File")

        else:
            set_preview_kind(panel['img'], "generic")
            panel['img'].setText(f"{ext.upper()} File")

        details = f"Size: {size_str}"
        if res_str:
            details += f"  |  Res: {res_str}"
        if extra_str:
            details += f"  |  {extra_str}"
        panel['details'].setText(details)

    def update_image_display(self, panel, pixmap):
        if pixmap and not pixmap.isNull():
            width = panel['img'].width()
            height = panel['img'].height()
            if width > 10 and height > 10:
                panel['img'].setPixmap(pixmap.scaled(
                    width, height,
                    Qt.AspectRatioMode.KeepAspectRatio,
                    Qt.TransformationMode.SmoothTransformation,
                ))

    def _apply_cached_pixmaps(self):
        if self.pixmap_cache['A']:
            self.update_image_display(self.panel_a, self.pixmap_cache['A'])
        if self.pixmap_cache['B']:
            self.update_image_display(self.panel_b, self.pixmap_cache['B'])

    # -------------------------------------------------------------------------
    # File deletion
    # -------------------------------------------------------------------------

    def delete_file(self, target):
        if self.current_match_index == -1:
            return
        panel = self.panel_a if target == "A" else self.panel_b
        filepath = panel.get('filepath')
        if not filepath:
            QMessageBox.warning(self, "Error", "No file selected.")
            return

        confirm = QMessageBox.question(
            self, "Confirm Delete",
            f"Send to Trash?\n\n{filepath}",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if confirm != QMessageBox.StandardButton.Yes:
            return

        try:
            if os.path.exists(filepath):
                send2trash(filepath)
            self.lbl_status.setText(f"Deleted: {os.path.basename(filepath)}")
            self._purge_deleted_paths([filepath])
            self.match_list.setFocus()
        except Exception as exc:
            QMessageBox.critical(self, "Error", str(exc))

    def delete_both_files(self):
        if self.current_match_index == -1:
            return
        path_a = self.panel_a.get('filepath')
        path_b = self.panel_b.get('filepath')
        if not path_a or not path_b:
            QMessageBox.warning(self, "Error", "Both files must be available to delete both.")
            return

        confirm = QMessageBox.question(
            self, "Confirm Delete Both",
            f"Send both files to Trash?\n\n{path_a}\n{path_b}",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if confirm != QMessageBox.StandardButton.Yes:
            return

        deleted = []
        for path in (path_a, path_b):
            try:
                if os.path.exists(path):
                    send2trash(path)
                deleted.append(path)
            except Exception as exc:
                QMessageBox.critical(self, "Error", f"Failed to delete {path}: {exc}")

        self.lbl_status.setText(f"Deleted {len(deleted)} file(s).")
        self._purge_deleted_paths(deleted)
        self.match_list.setFocus()

    def _purge_deleted_paths(self, paths):
        """
        Drop every match and group entry that references a deleted path.

        Removing only the current row left stale entries elsewhere in the list
        that rendered as "Missing on Disk" when the user reached them.
        """
        gone = set(paths)
        if not gone:
            return

        target_row = self.current_match_index
        self.matches = [
            m for m in self.matches
            if m['file_a'] not in gone and m['file_b'] not in gone
        ]
        self.exact_groups = [
            [p for p in group if p not in gone] for group in self.exact_groups
        ]
        self.exact_groups = [group for group in self.exact_groups if len(group) > 1]

        if not self.matches:
            self._rebuild_match_list(-1)
            self.lbl_score.setText("0%")
            self._clear_panel(self.panel_a, 'A')
            self._clear_panel(self.panel_b, 'B')
            QMessageBox.information(self, "Done", "No more matches!")
            return

        self._rebuild_match_list(target_row)

    def next_match(self):
        current_row = self.match_list.currentRow()
        if current_row < self.match_list.count() - 1:
            self.match_list.setCurrentRow(current_row + 1)
            self.match_list.scrollToItem(self.match_list.currentItem())
        else:
            QMessageBox.information(self, "Done", "No more matches!")

    # -------------------------------------------------------------------------
    # Auto-prune
    # -------------------------------------------------------------------------

    def get_folder_priority(self, filepath):
        """
        Priority of the most specific scan root containing `filepath`.

        Plain startswith matching treated /data/photos2 as being inside
        /data/photos, and returning the first match rather than the longest
        gave nested roots the wrong priority.
        """
        target = os.path.normcase(os.path.abspath(filepath))
        best_priority = None
        best_len = -1
        for folder_data in self.scan_folders:
            root = os.path.normcase(os.path.abspath(folder_data['path'])).rstrip(os.sep)
            if target == root or target.startswith(root + os.sep):
                if len(root) > best_len:
                    best_len = len(root)
                    best_priority = folder_data['priority']
        # A file under no configured root should not be the preferred victim.
        return best_priority if best_priority is not None else DEFAULT_FOLDER_PRIORITY

    def _select_prune_victims(self):
        """
        One keeper per exact-hash group, everything else goes.

        The previous implementation walked pairs built against group[0]. For a
        group of three or more it could delete group[0] on the first pair and
        then skip the rest, leaving duplicates behind.
        """
        victims = []
        for group in self.exact_groups:
            existing = [p for p in group if os.path.exists(p)]
            if len(existing) < 2:
                continue
            # Highest priority wins; ties go to the shortest path, then
            # alphabetically so the choice is deterministic.
            keeper = min(existing, key=lambda p: (-self.get_folder_priority(p), len(p), p))
            victims.extend(p for p in existing if p != keeper)
        return victims

    def auto_prune_exact(self):
        if self.worker and self.worker.isRunning():
            QMessageBox.warning(self, "Auto-Prune", "Cannot prune while a scan is in progress.")
            return
        if self.prune_worker and self.prune_worker.isRunning():
            return

        if not self.exact_groups:
            QMessageBox.information(self, "Auto-Prune", "No exact duplicates found.")
            return

        files_to_delete = self._select_prune_victims()
        if not files_to_delete:
            QMessageBox.information(self, "Auto-Prune", "No eligible files found for pruning.")
            return

        confirm = QMessageBox.question(
            self, "Auto-Prune Exact Duplicates",
            f"This will move {len(files_to_delete)} duplicate file(s) to Trash, "
            f"keeping one copy from each of {len(self.exact_groups)} group(s). Continue?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if confirm != QMessageBox.StandardButton.Yes:
            return

        self.progress_bar.setRange(0, 0)
        self.progress_bar.show()
        self.btn_scan.setEnabled(False)
        self.btn_stop.setEnabled(False)

        self.prune_progress_dialog = QProgressDialog(
            "Pruning exact duplicates...", "Cancel", 0, len(files_to_delete), self
        )
        self.prune_progress_dialog.setWindowTitle("Auto-Prune Exact Duplicates")
        self.prune_progress_dialog.setWindowModality(Qt.WindowModality.WindowModal)
        self.prune_progress_dialog.setAutoClose(False)
        self.prune_progress_dialog.setAutoReset(False)
        self.prune_progress_dialog.setMinimumDuration(100)
        self.prune_progress_dialog.setValue(0)

        self.prune_worker = AutoPruneWorker(files_to_delete)
        self.prune_worker.progress_update.connect(self.lbl_status.setText)
        self.prune_worker.progress_value.connect(self.update_progress_bar)
        self.prune_worker.progress_value.connect(self._update_prune_progress)
        self.prune_worker.prune_finished.connect(self.on_prune_complete)
        self.prune_worker.error.connect(self.on_error)
        self.prune_worker.aborted.connect(self.on_prune_aborted)
        self.prune_progress_dialog.canceled.connect(self.prune_worker.stop)
        self.prune_worker.start()

    def _update_prune_progress(self, current, total):
        dialog = self.prune_progress_dialog
        if not dialog:
            return
        try:
            dialog.setMaximum(total)
            dialog.setValue(current)
            dialog.setLabelText(f"Pruning: {current} / {total} files")
        except (RuntimeError, AttributeError):
            pass

    def _close_prune_progress_dialog(self):
        if self.prune_progress_dialog:
            self.prune_progress_dialog.close()
            self.prune_progress_dialog = None
        if self.prune_worker:
            try:
                self.prune_worker.progress_value.disconnect()
            except TypeError:
                pass

    def on_prune_complete(self, deleted_count, deleted_paths):
        self._close_prune_progress_dialog()
        self.progress_bar.hide()
        self.btn_scan.setEnabled(True)
        self.btn_stop.setEnabled(False)
        QMessageBox.information(self, "Complete", f"Moved {deleted_count} file(s) to Trash.")
        self._purge_deleted_paths(deleted_paths)
        self.lbl_status.setText("Pruning complete. Re-scan to refresh the index.")

    def on_prune_aborted(self):
        self._close_prune_progress_dialog()
        self.progress_bar.hide()
        self.btn_scan.setEnabled(True)
        self.btn_stop.setEnabled(False)
        self.lbl_status.setText("Pruning aborted.")

    def persist_folder_priorities(self):
        if not self.current_db_path or not os.path.exists(self.current_db_path):
            return
        try:
            db = DatabaseManager(self.current_db_path)
            db.save_roots(self.scan_folders)
            db.close()
        except Exception as exc:
            print(f"Failed to save priorities: {exc}")

    # -------------------------------------------------------------------------
    # Events
    # -------------------------------------------------------------------------

    def resizeEvent(self, event):
        if self.current_match_index != -1:
            self._resize_timer.start()
        super().resizeEvent(event)

    def closeEvent(self, event):
        if self.worker and self.worker.isRunning():
            self.worker.stop()
            self.worker.wait(5000)

        if self.prune_worker and self.prune_worker.isRunning():
            self.prune_worker.stop()
            self.prune_worker.wait(5000)

        if self.current_db_path and os.path.exists(self.current_db_path):
            reply = QMessageBox.question(
                self, "Cleanup",
                "Delete the index database file before exiting?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            )
            if reply == QMessageBox.StandardButton.Yes:
                self._trash_database_files()

        event.accept()

    def _trash_database_files(self):
        candidates = [
            self.current_db_path,
            self.current_db_path + "-shm",
            self.current_db_path + "-wal",
        ]
        for raw_path in candidates:
            clean_path = raw_path
            if clean_path.startswith('\\\\?\\'):
                clean_path = clean_path[4:]
            clean_path = os.path.abspath(clean_path)
            if not os.path.exists(clean_path):
                continue

            tried = 0
            while tried < 3:
                try:
                    send2trash(clean_path)
                    print(f"Sent to trash: {clean_path}")
                    break
                except Exception as exc:
                    tried += 1
                    if tried < 3:
                        time.sleep(0.1)
                        continue
                    button = QMessageBox.question(
                        self, "Failed to Move to Trash",
                        f"Failed to move to Recycle Bin:\n\n{clean_path}\n\n"
                        f"Error: {exc}\n\nRetry, delete permanently (Yes), or skip (No)?",
                        QMessageBox.StandardButton.Retry
                        | QMessageBox.StandardButton.Yes
                        | QMessageBox.StandardButton.No,
                    )
                    if button == QMessageBox.StandardButton.Retry:
                        tried = 0
                        continue
                    if button == QMessageBox.StandardButton.Yes:
                        try:
                            os.remove(clean_path)
                            print(f"Permanently removed: {clean_path}")
                        except Exception as remove_exc:
                            print(f"Permanent delete failed: {remove_exc}")
                    break

    def open_github(self):
        QDesktopServices.openUrl(QUrl(GITHUB_URL))

    def show_about(self):
        QMessageBox.about(
            self, f"About {APP_NAME}",
            f"<h3>{APP_NAME}</h3>"
            f"<p>Version {VERSION}</p>"
            f"<p>Find exact and visually or acoustically similar duplicate files.</p>"
            f"<p><a href='{GITHUB_URL}'>{GITHUB_URL}</a></p>",
        )


def main():
    # Required for ProcessPoolExecutor inside a PyInstaller-frozen executable
    # using the spawn start method.
    multiprocessing.freeze_support()

    app = QApplication(sys.argv)
    app.setApplicationName(APP_NAME)
    app.setApplicationVersion(VERSION)
    app.setOrganizationName("FuzzyDuplicateFinder")
    app.setDesktopFileName(DESKTOP_FILE_NAME)
    app.setWindowIcon(load_app_icon())

    theme = ThemeManager(app)
    theme.apply()

    window = DuplicateFinderApp(theme)
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()

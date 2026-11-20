"""
Main window.
"""

from __future__ import annotations

import logging
import os
import sys
import time

from PyQt6.QtCore import (
    QAbstractListModel,
    QModelIndex,
    QSize,
    Qt,
    QThread,
    QThreadPool,
    QTimer,
    QUrl,
    pyqtSignal,
)
from PyQt6.QtGui import QAction, QActionGroup, QDesktopServices, QIcon, QKeySequence, QPixmap
from PyQt6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QComboBox,
    QFileDialog,
    QFrame,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QListView,
    QMainWindow,
    QMessageBox,
    QProgressBar,
    QProgressDialog,
    QPushButton,
    QSizePolicy,
    QSpinBox,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)
from send2trash import send2trash

from . import APP_ID, APP_NAME, PROJECT_URL, VERSION, export, pipeline
from .dialogs import IssuesDialog, OptionsDialog
from .index import Index
from .paths import default_index_path, index_dir, index_files, log_dir, normalize_root
from .preview import PreviewSignals, PreviewTask
from .settings import AppSettings
from .theme import MODE_LABELS, MODES, ThemeManager
from .watchdog import ResourceWatchdog, format_bytes, lower_priority

try:
    import psutil
except ImportError:  # pragma: no cover
    psutil = None

log = logging.getLogger(__name__)

DEFAULT_FOLDER_PRIORITY = 10
PROGRESS_STEPS = 1000
ICON_SIZES = (16, 24, 32, 48, 64, 128, 256, 512)

PHASE_LABELS = {
    'walk': "Finding files",
    'hash': "Fingerprinting",
    'verify': "Verifying exact duplicates",
    'match': "Matching",
    'finalize': "Finalizing",
}

FILTERS = (
    ("All matches", None),
    ("Exact only", 'EXACT'),
    ("Similar only", 'FUZZY'),
    ("Images", 'image'),
    ("Videos", 'video'),
    ("Audio", 'audio'),
    ("Text", 'text'),
)


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def resource_path(*parts):
    """Path to a bundled resource, from source or from a PyInstaller build."""
    base = getattr(sys, "_MEIPASS", None)
    if base is None:
        base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base, *parts)


def load_app_icon():
    """PNG icon set, generated from assets/icon.svg by tools/build_icons.py.
    PNGs avoid needing Qt's SVG plugin in frozen builds; the SVG is the
    fallback."""
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


def set_variant(button, variant):
    button.setProperty("variant", variant)
    button.setCursor(Qt.CursorShape.PointingHandCursor)
    return button


def set_role(label, role):
    label.setProperty("role", role)
    return label


def repolish(widget):
    style = widget.style()
    style.unpolish(widget)
    style.polish(widget)
    widget.update()


def set_dynamic(widget, name, value):
    if widget.property(name) != value:
        widget.setProperty(name, value)
        repolish(widget)


def open_path(path):
    QDesktopServices.openUrl(QUrl.fromLocalFile(path))


def fmt_eta(seconds):
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    mins, secs = divmod(seconds, 60)
    if mins < 60:
        return f"{mins}m {secs:02d}s"
    hours, mins = divmod(mins, 60)
    return f"{hours}h {mins:02d}m"


# -----------------------------------------------------------------------------
# Workers
# -----------------------------------------------------------------------------

class ScanWorker(QThread):
    progress = pyqtSignal(str, int, int, int)   # phase, done, total, issues
    results_ready = pyqtSignal(object)          # pipeline.Results
    failed = pyqtSignal(str)

    def __init__(self, folders, db_path, options, threshold, skip_scan, match_workers,
                 watchdog):
        super().__init__()
        self.folders = [dict(f) for f in folders]
        self.db_path = db_path
        self.options = options
        self.threshold = threshold
        self.skip_scan = skip_scan
        self.match_workers = match_workers
        self.watchdog = watchdog
        self._stop = False

    def stop(self):
        self._stop = True

    def is_stopped(self):
        return self._stop

    def run(self):
        try:
            results = pipeline.run(
                self.folders, self.db_path, self.options, threshold=self.threshold,
                skip_scan=self.skip_scan, stop_signal=self.is_stopped,
                progress=self.progress.emit, watchdog=self.watchdog,
                match_workers=self.match_workers,
            )
            if self._stop and not results.aborted:
                results.aborted = True
            self.results_ready.emit(results)
        except Exception as exc:
            log.exception("Scan failed")
            if self._stop:
                self.results_ready.emit(pipeline.Results(aborted=True))
            else:
                self.failed.emit(str(exc) or type(exc).__name__)


class PruneWorker(QThread):
    progress_value = pyqtSignal(int, int)
    prune_finished = pyqtSignal(list, list)     # trashed paths, [(path, error)]

    def __init__(self, files_to_trash):
        super().__init__()
        self.files_to_trash = list(files_to_trash)
        self._stop = False

    def stop(self):
        self._stop = True

    def run(self):
        trashed, failed = [], []
        total = len(self.files_to_trash)
        for i, path in enumerate(self.files_to_trash):
            if self._stop:
                break
            try:
                if os.path.lexists(path):
                    send2trash(path)
                trashed.append(path)
            except Exception as exc:
                failed.append((path, str(exc)))
            self.progress_value.emit(i + 1, total)
        self.prune_finished.emit(trashed, failed)


# -----------------------------------------------------------------------------
# Match list model
# -----------------------------------------------------------------------------

class MatchListModel(QAbstractListModel):
    """
    Holds every match and a filtered view of them. A QListWidget item per match
    was the slow part of showing large result sets; a model renders only what
    is on screen.
    """

    MatchRole = Qt.ItemDataRole.UserRole + 1

    def __init__(self, parent=None):
        super().__init__(parent)
        self._all = []
        self._rows = []
        self._filter = None
        self._text = ""

    def rowCount(self, parent=QModelIndex()):
        return 0 if parent.isValid() else len(self._rows)

    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid() or index.row() >= len(self._rows):
            return None
        match = self._all[self._rows[index.row()]]
        if role == Qt.ItemDataRole.DisplayRole:
            prefix = "[=]" if match.type == 'EXACT' else f"[{int(match.score)}%]"
            return f"{prefix} {os.path.basename(match.file_a)}"
        if role == Qt.ItemDataRole.ToolTipRole:
            return f"{match.file_a}\n{match.file_b}"
        if role == self.MatchRole:
            return match
        return None

    def set_matches(self, matches):
        self.beginResetModel()
        self._all = list(matches)
        self._refilter()
        self.endResetModel()

    def set_filter(self, kind_or_type, text):
        self.beginResetModel()
        self._filter = kind_or_type
        self._text = text.strip().lower()
        self._refilter()
        self.endResetModel()

    def _refilter(self):
        f, text = self._filter, self._text
        rows = []
        for i, m in enumerate(self._all):
            if f in ('EXACT', 'FUZZY'):
                if m.type != f:
                    continue
            elif f is not None and m.kind != f:
                continue
            if text and text not in m.file_a.lower() and text not in m.file_b.lower():
                continue
            rows.append(i)
        self._rows = rows

    def match_at(self, row):
        if 0 <= row < len(self._rows):
            return self._all[self._rows[row]]
        return None

    def all_matches(self):
        return self._all

    def remove_paths(self, gone):
        self.beginResetModel()
        self._all = [m for m in self._all if m.file_a not in gone and m.file_b not in gone]
        self._refilter()
        self.endResetModel()


# -----------------------------------------------------------------------------
# Widgets
# -----------------------------------------------------------------------------

class ThreadCountWidget(QWidget):
    """A down/value/up stepper that matches the folder-priority arrows."""

    def __init__(self, min_val=1, max_val=8, default=4, parent=None):
        super().__init__(parent)
        self._min, self._max = min_val, max_val
        self._value = max(min_val, min(default, max_val))
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(2)
        btn_down = set_variant(QPushButton("▼"), "arrow")
        btn_down.clicked.connect(lambda: self.setValue(self._value - 1))
        self._lbl = set_role(QLabel(str(self._value)), "stepper")
        self._lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        btn_up = set_variant(QPushButton("▲"), "arrow")
        btn_up.clicked.connect(lambda: self.setValue(self._value + 1))
        layout.addWidget(btn_down)
        layout.addWidget(self._lbl)
        layout.addWidget(btn_up)

    def setValue(self, value):
        self._value = max(self._min, min(int(value), self._max))
        self._lbl.setText(str(self._value))

    def value(self):
        return self._value


# -----------------------------------------------------------------------------
# Main window
# -----------------------------------------------------------------------------

class DuplicateFinderApp(QMainWindow):
    def __init__(self, theme, settings=None):
        super().__init__()
        self.theme = theme
        self.settings = settings or AppSettings()
        self.setWindowTitle(APP_NAME)
        self.resize(1400, 950)
        self.setAcceptDrops(True)

        self.scan_folders = self.settings.folders
        self.exact_groups = []
        self.issues = []
        self.current_match = None
        self.loaded_index = None          # set when the user picked a .db by hand
        self.last_db_path = None
        self.worker = None
        self.watchdog = None
        self.prune_worker = None
        self.prune_dialog = None
        self.pixmap_cache = {'A': None, 'B': None}
        self._preview_token = {'A': 0, 'B': 0}
        self._phase = None
        self._phase_started = 0.0

        self.preview_pool = QThreadPool(self)
        self.preview_pool.setMaxThreadCount(2)
        self.preview_signals = PreviewSignals(self)
        self.preview_signals.ready.connect(self._on_preview_ready)

        self._resize_timer = QTimer(self)
        self._resize_timer.setSingleShot(True)
        self._resize_timer.setInterval(80)
        self._resize_timer.timeout.connect(self._apply_cached_pixmaps)

        self._resource_timer = QTimer(self)
        self._resource_timer.setInterval(1000)
        self._resource_timer.timeout.connect(self._update_resources)

        self._build_menus()
        self._build_status_bar()
        self._build_body()

        self.theme.mode_changed.connect(self._sync_theme_controls)
        self._sync_theme_controls(self.theme.mode)
        self._restore_window_state()
        self.refresh_folder_table()
        self._update_actions()
        self._resource_timer.start()

    # -------------------------------------------------------------------------
    # Construction
    # -------------------------------------------------------------------------

    def _action(self, menu, text, slot, shortcut=None, tip=None):
        action = QAction(text, self)
        action.triggered.connect(slot)
        if shortcut:
            action.setShortcut(QKeySequence(shortcut))
        if tip:
            action.setStatusTip(tip)
        menu.addAction(action)
        return action

    def _build_menus(self):
        bar = self.menuBar()

        file_menu = bar.addMenu("&File")
        self._action(file_menu, "&Add Folder...", self.add_folder, "Ctrl+N")
        self._action(file_menu, "&Clear Folder List", self.clear_folders)
        file_menu.addSeparator()
        self._action(file_menu, "&Load Index...", self.load_index, "Ctrl+O")
        self.act_export = self._action(file_menu, "&Export Results...", self.export_results,
                                       "Ctrl+E")
        file_menu.addSeparator()
        self._action(file_menu, "Open Index &Folder", lambda: open_path(index_dir()))
        self.act_forget = self._action(file_menu, "&Delete Saved Index for These Folders",
                                       self.forget_index)
        file_menu.addSeparator()
        self._action(file_menu, "E&xit", self.close, QKeySequence.StandardKey.Quit)

        scan_menu = bar.addMenu("S&can")
        self.act_scan = self._action(scan_menu, "&Start Scan", self.start_scan, "F5",
                                     "Index the folders (incrementally) and find duplicates")
        self.act_rematch = self._action(scan_menu, "&Re-match Without Scanning",
                                        self.rematch, "F6",
                                        "Re-run matching on the existing index, e.g. after "
                                        "changing the threshold")
        self.act_rebuild = self._action(scan_menu, "Re&build Index From Scratch",
                                        self.rebuild_index)
        self.act_stop = self._action(scan_menu, "S&top", self.stop_scan, "Esc")
        scan_menu.addSeparator()
        self._action(scan_menu, "Scan &Options...", self.show_options, "Ctrl+,")

        view_menu = bar.addMenu("&View")
        theme_menu = view_menu.addMenu("&Theme")
        group = QActionGroup(self)
        group.setExclusive(True)
        self._theme_actions = {}
        for mode in MODES:
            action = QAction(MODE_LABELS[mode], self, checkable=True)
            action.triggered.connect(lambda _checked, m=mode: self.theme.set_mode(m))
            group.addAction(action)
            theme_menu.addAction(action)
            self._theme_actions[mode] = action

        tools_menu = bar.addMenu("&Tools")
        self.act_prune = self._action(tools_menu, "Auto-&Prune Exact Duplicates...",
                                      self.auto_prune_exact)
        self.act_issues = self._action(tools_menu, "Files With &Issues...",
                                       self.show_issues)

        help_menu = bar.addMenu("&Help")
        self._action(help_menu, "Open &Log Folder", lambda: open_path(log_dir()))
        self._action(help_menu, f"&About {APP_NAME}", self.show_about)

    def _build_status_bar(self):
        self.status_bar = self.statusBar()
        self.lbl_status = set_role(QLabel("Ready"), "status")
        self.status_bar.addWidget(self.lbl_status, 1)

        self.lbl_resources = set_role(QLabel(""), "resource")
        self.status_bar.addPermanentWidget(self.lbl_resources)

        self.lbl_version = set_role(QLabel(f"v{VERSION}"), "version")
        self.lbl_version.setCursor(Qt.CursorShape.PointingHandCursor)
        self.lbl_version.setToolTip(PROJECT_URL)
        self.lbl_version.mousePressEvent = lambda _e: QDesktopServices.openUrl(QUrl(PROJECT_URL))
        self.status_bar.addPermanentWidget(self.lbl_version)

    def _build_toolbar(self):
        toolbar = QFrame()
        toolbar.setObjectName("toolbar")
        row = QHBoxLayout(toolbar)
        row.setContentsMargins(8, 6, 8, 6)
        row.setSpacing(6)

        btn_add = set_variant(QPushButton(" + Add Folder "), "neutral")
        btn_add.clicked.connect(self.add_folder)
        btn_clear = set_variant(QPushButton(" Clear List "), "neutral")
        btn_clear.clicked.connect(self.clear_folders)
        btn_load = set_variant(QPushButton(" Load Index... "), "neutral")
        btn_load.clicked.connect(self.load_index)
        btn_options = set_variant(QPushButton(" Options... "), "neutral")
        btn_options.clicked.connect(self.show_options)

        self.btn_scan = set_variant(QPushButton("  START SCAN  "), "primary")
        self.btn_scan.setToolTip("Start scan (F5). Unchanged files from the last scan of "
                                 "these folders are not re-read.")
        self.btn_scan.clicked.connect(self.start_scan)

        self.btn_stop = set_variant(QPushButton("  STOP  "), "danger")
        self.btn_stop.setToolTip("Stop (Esc)")
        self.btn_stop.clicked.connect(self.stop_scan)

        self.btn_issues = set_variant(QPushButton(""), "warn-link")
        self.btn_issues.clicked.connect(self.show_issues)
        self.btn_issues.hide()

        self.spin_threshold = QSpinBox()
        self.spin_threshold.setRange(30, 100)
        self.spin_threshold.setSuffix(" %")
        self.spin_threshold.setValue(int(round(self.settings.threshold)))
        self.spin_threshold.setToolTip(
            "Minimum similarity score for a fuzzy match. Lower finds more, with more\n"
            "false positives. Use Scan > Re-match (F6) to apply it without rescanning."
        )
        self.spin_threshold.valueChanged.connect(
            lambda v: setattr(self.settings, 'threshold', float(v))
        )

        cpu = os.cpu_count() or 4
        self.spin_workers = ThreadCountWidget(1, cpu * 2, min(self.settings.threads, cpu * 2))
        self.spin_workers.setToolTip(
            f"Worker threads for indexing and matching.\nThis system reports {cpu} "
            "logical CPU core(s). Lower it to leave headroom for other work."
        )

        row.addWidget(btn_add)
        row.addWidget(btn_clear)
        row.addWidget(btn_load)
        row.addWidget(btn_options)
        row.addSpacing(20)
        row.addWidget(self.btn_scan)
        row.addWidget(self.btn_stop)
        row.addWidget(self.btn_issues)
        row.addStretch()
        row.addWidget(set_role(QLabel("Threshold:"), "muted"))
        row.addWidget(self.spin_threshold)
        row.addSpacing(12)
        row.addWidget(set_role(QLabel("Threads:"), "muted"))
        row.addWidget(self.spin_workers)
        return toolbar

    def _build_results_pane(self):
        pane = QWidget()
        layout = QVBoxLayout(pane)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(4)

        filters = QHBoxLayout()
        filters.setContentsMargins(4, 4, 4, 0)
        self.combo_filter = QComboBox()
        for label, _ in FILTERS:
            self.combo_filter.addItem(label)
        self.combo_filter.currentIndexChanged.connect(self._apply_filter)
        self.edit_search = QLineEdit()
        self.edit_search.setPlaceholderText("Filter by path...")
        self.edit_search.setClearButtonEnabled(True)
        self._search_timer = QTimer(self)
        self._search_timer.setSingleShot(True)
        self._search_timer.setInterval(200)
        self._search_timer.timeout.connect(self._apply_filter)
        self.edit_search.textChanged.connect(lambda _t: self._search_timer.start())
        filters.addWidget(self.combo_filter)
        filters.addWidget(self.edit_search, 1)
        layout.addLayout(filters)

        self.model = MatchListModel(self)
        self.match_list = QListView()
        self.match_list.setObjectName("matchList")
        self.match_list.setModel(self.model)
        self.match_list.setFrameShape(QFrame.Shape.NoFrame)
        self.match_list.setUniformItemSizes(True)
        self.match_list.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.match_list.selectionModel().currentRowChanged.connect(
            lambda cur, _prev: self.load_match_details(cur.row())
        )
        layout.addWidget(self.match_list, 1)

        self.lbl_summary = set_role(QLabel(""), "muted")
        self.lbl_summary.setContentsMargins(6, 2, 6, 4)
        self.lbl_summary.setWordWrap(True)
        layout.addWidget(self.lbl_summary)
        return pane

    def _build_body(self):
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)
        main_layout.setContentsMargins(6, 6, 6, 6)

        self.v_splitter = QSplitter(Qt.Orientation.Vertical)
        self.v_splitter.setHandleWidth(1)

        top = QWidget()
        top_layout = QVBoxLayout(top)
        top_layout.setContentsMargins(0, 0, 0, 6)
        top_layout.setSpacing(6)
        top_layout.addWidget(self._build_toolbar())

        self.folder_table = QTableWidget()
        self.folder_table.setColumnCount(3)
        self.folder_table.setHorizontalHeaderLabels(["Folder Path", "Priority", ""])
        header = self.folder_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Fixed)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.Fixed)
        self.folder_table.setColumnWidth(1, 90)
        self.folder_table.setColumnWidth(2, 36)
        self.folder_table.verticalHeader().setVisible(False)
        self.folder_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.folder_table.setToolTip("Drop folders here to add them. Higher priority folders "
                                     "keep their copy when auto-pruning.")
        top_layout.addWidget(self.folder_table)

        self.progress_bar = QProgressBar()
        self.progress_bar.setFixedHeight(5)
        self.progress_bar.setTextVisible(False)
        self.progress_bar.hide()
        top_layout.addWidget(self.progress_bar)
        self.v_splitter.addWidget(top)

        self.h_splitter = QSplitter(Qt.Orientation.Horizontal)
        self.h_splitter.setHandleWidth(1)
        self.h_splitter.addWidget(self._build_results_pane())

        compare = QWidget()
        compare.setObjectName("comparePane")
        compare.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)
        comp_layout = QVBoxLayout(compare)

        previews = QSplitter(Qt.Orientation.Horizontal)
        previews.setHandleWidth(1)
        self.panel_a = self.create_file_panel("Original / File A")
        self.panel_b = self.create_file_panel("Duplicate / File B")
        previews.addWidget(self.panel_a['container'])
        previews.addWidget(self.panel_b['container'])
        comp_layout.addWidget(previews, stretch=1)

        action_frame = QFrame()
        action_frame.setObjectName("actionBar")
        actions = QHBoxLayout(action_frame)
        self.lbl_score = set_role(QLabel(""), "score")

        self.btn_del_a = set_variant(QPushButton("Delete File &A"), "danger")
        self.btn_del_a.setToolTip("Move File A to the Trash (Alt+A)")
        self.btn_del_a.clicked.connect(lambda: self.delete_files(("A",)))
        self.btn_del_both = set_variant(QPushButton("Delete B&oth Files"), "danger")
        self.btn_del_both.setToolTip("Move both files to the Trash (Alt+O)")
        self.btn_del_both.clicked.connect(lambda: self.delete_files(("A", "B")))
        self.btn_keep = set_variant(QPushButton("&Skip / Keep Both"), "neutral")
        self.btn_keep.setToolTip("Next match (Alt+S)")
        self.btn_keep.clicked.connect(self.next_match)
        self.btn_del_b = set_variant(QPushButton("Delete File &B"), "danger")
        self.btn_del_b.setToolTip("Move File B to the Trash (Alt+B)")
        self.btn_del_b.clicked.connect(lambda: self.delete_files(("B",)))

        actions.addStretch()
        actions.addWidget(self.btn_del_a)
        actions.addSpacing(20)
        actions.addWidget(self.btn_del_both)
        actions.addSpacing(20)
        actions.addWidget(self.lbl_score)
        actions.addWidget(self.btn_keep)
        actions.addSpacing(20)
        actions.addWidget(self.btn_del_b)
        actions.addStretch()
        comp_layout.addWidget(action_frame)

        self.h_splitter.addWidget(compare)
        self.h_splitter.setSizes([350, 900])
        self.v_splitter.addWidget(self.h_splitter)
        self.v_splitter.setSizes([220, 700])
        main_layout.addWidget(self.v_splitter)

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
        lbl_img.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        layout.addWidget(lbl_img, 1)

        meta = QFrame()
        meta.setObjectName("metaFrame")
        meta_layout = QVBoxLayout(meta)
        lbl_filename = set_role(QLabel(""), "filename")
        lbl_filename.setWordWrap(True)
        lbl_path = set_role(QLabel(""), "path")
        lbl_path.setWordWrap(True)
        lbl_path.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        lbl_details = set_role(QLabel(""), "details")
        lbl_dates = set_role(QLabel(""), "dates")

        buttons = QHBoxLayout()
        btn_open = set_variant(QPushButton("Open"), "link")
        btn_folder = set_variant(QPushButton("Show in Folder"), "link")
        buttons.addWidget(btn_open)
        buttons.addSpacing(16)
        buttons.addWidget(btn_folder)
        buttons.addStretch()

        for widget in (lbl_filename, lbl_path, lbl_details, lbl_dates):
            meta_layout.addWidget(widget)
        meta_layout.addLayout(buttons)
        layout.addWidget(meta)

        panel = {
            "container": container, "img": lbl_img, "filename": lbl_filename,
            "path": lbl_path, "details": lbl_details, "dates": lbl_dates,
            "filepath": None,
        }
        btn_open.clicked.connect(lambda: panel['filepath'] and open_path(panel['filepath']))
        btn_folder.clicked.connect(
            lambda: panel['filepath'] and open_path(os.path.dirname(panel['filepath']))
        )
        return panel

    def _restore_window_state(self):
        geometry = self.settings.value("window/geometry")
        if geometry is not None:
            self.restoreGeometry(geometry)
        for key, splitter in (("window/v_split", self.v_splitter),
                              ("window/h_split", self.h_splitter)):
            state = self.settings.value(key)
            if state is not None:
                splitter.restoreState(state)

    def _save_window_state(self):
        self.settings.set_value("window/geometry", self.saveGeometry())
        self.settings.set_value("window/v_split", self.v_splitter.saveState())
        self.settings.set_value("window/h_split", self.h_splitter.saveState())
        self.settings.threads = self.spin_workers.value()
        self.settings.folders = self.scan_folders

    def _sync_theme_controls(self, mode):
        action = self._theme_actions.get(mode)
        if action is not None:
            action.setChecked(True)

    # -------------------------------------------------------------------------
    # State
    # -------------------------------------------------------------------------

    def _busy(self):
        return bool((self.worker and self.worker.isRunning())
                    or (self.prune_worker and self.prune_worker.isRunning()))

    def _db_path(self):
        if self.loaded_index:
            return self.loaded_index
        if not self.scan_folders:
            return None
        return default_index_path([f['path'] for f in self.scan_folders])

    def _update_actions(self):
        busy = self._busy()
        has_folders = bool(self.scan_folders)
        db = self._db_path()
        has_index = bool(db and os.path.exists(db))
        has_results = self.model.rowCount() > 0 or bool(self.model.all_matches())
        self.btn_scan.setEnabled(has_folders and not busy)
        self.act_scan.setEnabled(has_folders and not busy)
        self.act_rebuild.setEnabled(has_folders and not busy and has_index)
        self.act_rematch.setEnabled(has_index and not busy)
        self.act_forget.setEnabled(has_index and not busy and not self.loaded_index)
        self.btn_stop.setEnabled(bool(self.worker and self.worker.isRunning()))
        self.act_stop.setEnabled(self.btn_stop.isEnabled())
        self.act_prune.setEnabled(bool(self.exact_groups) and not busy)
        self.act_export.setEnabled(has_results)
        self.act_issues.setEnabled(bool(self.issues))
        has_current = self.current_match is not None and not busy
        for button in (self.btn_del_a, self.btn_del_b, self.btn_del_both, self.btn_keep):
            button.setEnabled(has_current)

    # -------------------------------------------------------------------------
    # Folders
    # -------------------------------------------------------------------------

    def _add_folder_path(self, folder):
        folder = os.path.abspath(folder)
        if not os.path.isdir(folder):
            return False
        norm = normalize_root(folder)
        if any(normalize_root(f['path']) == norm for f in self.scan_folders):
            return False
        self.scan_folders.append({'path': folder, 'priority': DEFAULT_FOLDER_PRIORITY})
        return True

    def _folders_changed(self):
        self.refresh_folder_table()
        self.settings.folders = self.scan_folders
        self._update_actions()

    def add_folder(self):
        folder = QFileDialog.getExistingDirectory(self, "Select Folder")
        if folder and self._add_folder_path(folder):
            self._folders_changed()

    def clear_folders(self):
        self.scan_folders = []
        self.loaded_index = None
        self._folders_changed()

    def remove_folder(self, index):
        if 0 <= index < len(self.scan_folders):
            del self.scan_folders[index]
            self._folders_changed()

    def dragEnterEvent(self, event):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()

    def dropEvent(self, event):
        added = False
        for url in event.mimeData().urls():
            if url.isLocalFile():
                added = self._add_folder_path(url.toLocalFile()) or added
        if added:
            self._folders_changed()
        event.acceptProposedAction()

    def refresh_folder_table(self):
        self.folder_table.setRowCount(len(self.scan_folders))
        for i, folder in enumerate(self.scan_folders):
            item = QTableWidgetItem(folder['path'])
            item.setFlags(Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable)
            if not os.path.isdir(folder['path']):
                item.setToolTip("Folder not found")
                item.setText(folder['path'] + "   (missing)")
            self.folder_table.setItem(i, 0, item)

            cell = QWidget()
            cell_layout = QHBoxLayout(cell)
            cell_layout.setContentsMargins(4, 0, 4, 0)
            cell_layout.setSpacing(2)
            value = set_role(QLabel(str(folder['priority'])), "stepper")
            value.setAlignment(Qt.AlignmentFlag.AlignCenter)
            btn_down = set_variant(QPushButton("▼"), "arrow")
            btn_up = set_variant(QPushButton("▲"), "arrow")
            btn_down.clicked.connect(lambda _c, i=i, v=value: self._adjust_priority(i, -1, v))
            btn_up.clicked.connect(lambda _c, i=i, v=value: self._adjust_priority(i, 1, v))
            cell_layout.addWidget(btn_down)
            cell_layout.addWidget(value)
            cell_layout.addWidget(btn_up)
            self.folder_table.setCellWidget(i, 1, cell)

            btn_remove = set_variant(QPushButton("✕"), "arrow")
            btn_remove.setToolTip("Remove this folder from the list")
            btn_remove.clicked.connect(lambda _c, i=i: self.remove_folder(i))
            holder = QWidget()
            holder_layout = QHBoxLayout(holder)
            holder_layout.setContentsMargins(8, 0, 8, 0)
            holder_layout.addWidget(btn_remove)
            self.folder_table.setCellWidget(i, 2, holder)

    def _adjust_priority(self, index, delta, label):
        if not 0 <= index < len(self.scan_folders):
            return
        value = max(0, min(100, self.scan_folders[index]['priority'] + delta))
        self.scan_folders[index]['priority'] = value
        label.setText(str(value))
        self.settings.folders = self.scan_folders
        db = self._db_path()
        if db and os.path.exists(db):
            try:
                index_db = Index(db)
                index_db.save_roots(self.scan_folders)
                index_db.close()
            except Exception:
                log.exception("Could not save priorities to the index")

    # -------------------------------------------------------------------------
    # Scanning
    # -------------------------------------------------------------------------

    def start_scan(self):
        if not self.scan_folders or self._busy():
            return
        missing = [f['path'] for f in self.scan_folders if not os.path.isdir(f['path'])]
        if missing:
            QMessageBox.warning(self, "Missing Folders",
                                "These folders do not exist:\n\n" + "\n".join(missing))
            return
        self._start_worker(skip_scan=False)

    def rematch(self):
        db = self._db_path()
        if db and os.path.exists(db) and not self._busy():
            self._start_worker(skip_scan=True)

    def rebuild_index(self):
        db = self._db_path()
        if not db or self._busy():
            return
        confirm = QMessageBox.question(
            self, "Rebuild Index",
            "Discard the saved index for these folders and re-read every file?",
        )
        if confirm != QMessageBox.StandardButton.Yes:
            return
        if not self._remove_index(db):
            return
        self.start_scan()

    def forget_index(self):
        db = self._db_path()
        if not db or self.loaded_index or not os.path.exists(db):
            return
        confirm = QMessageBox.question(
            self, "Delete Saved Index",
            f"Delete the saved index for these folders?\n\n{db}\n\n"
            "The next scan will re-read every file.",
        )
        if confirm == QMessageBox.StandardButton.Yes and self._remove_index(db):
            self.lbl_status.setText("Saved index deleted.")
        self._update_actions()

    def _remove_index(self, db):
        for path in index_files(db):
            try:
                if os.path.exists(path):
                    os.remove(path)
            except OSError as exc:
                QMessageBox.warning(self, "Index", f"Could not delete {path}: {exc}")
                return False
        return True

    def load_index(self):
        if self._busy():
            return
        db_path, _ = QFileDialog.getOpenFileName(
            self, "Load Existing Index", index_dir(), "Index Files (*.db);;All Files (*)"
        )
        if not db_path:
            return
        try:
            index_db = Index(db_path)
            roots = index_db.get_roots()
            index_db.close()
        except Exception as exc:
            QMessageBox.critical(self, "Error", f"Could not open index: {exc}")
            return
        self.loaded_index = db_path
        if roots:
            self.scan_folders = roots
            self._folders_changed()
        self._start_worker(skip_scan=True)

    def _start_worker(self, skip_scan):
        db_path = self._db_path()
        if not db_path:
            return
        self.last_db_path = db_path
        self.model.set_matches([])
        self.exact_groups = []
        self.issues = []
        self.current_match = None
        self._clear_panels()
        self.btn_issues.hide()
        self.lbl_summary.setText("")
        self.progress_bar.setRange(0, 0)
        self.progress_bar.show()
        self.lbl_status.setText("Starting...")

        threads = self.spin_workers.value()
        self.settings.threads = threads
        if self.settings.low_priority:
            lower_priority()
        self.watchdog = None
        if self.settings.watchdog:
            self.watchdog = ResourceWatchdog(stall_seconds=self.settings.stall_seconds).start()

        self.worker = ScanWorker(
            self.scan_folders, db_path, self.settings.scan_options(threads),
            float(self.spin_threshold.value()), skip_scan,
            min(threads, os.cpu_count() or 4), self.watchdog,
        )
        self.worker.progress.connect(self._on_progress)
        self.worker.results_ready.connect(self._on_results)
        self.worker.failed.connect(self._on_failed)
        self.worker.finished.connect(self._on_worker_finished)
        self._phase = None
        self.worker.start()
        self._update_actions()

    def stop_scan(self):
        if self.worker and self.worker.isRunning():
            self.lbl_status.setText("Stopping... finishing files already in progress.")
            self.btn_stop.setEnabled(False)
            self.worker.stop()

    def _on_progress(self, phase, done, total, issues):
        now = time.monotonic()
        if phase != self._phase:
            self._phase = phase
            self._phase_started = now
        label = PHASE_LABELS.get(phase, phase)
        if total > 0:
            self.progress_bar.setRange(0, PROGRESS_STEPS)
            self.progress_bar.setValue(int(min(done, total) * PROGRESS_STEPS / total))
            text = f"{label}: {done:,} / {total:,}"
            elapsed = now - self._phase_started
            if phase in ('hash', 'verify') and done > 0 and elapsed > 3:
                text += f"  |  ETA {fmt_eta(elapsed * (total - done) / done)}"
        else:
            self.progress_bar.setRange(0, 0)
            text = f"{label}: {done:,} files" if (phase == 'walk' and done) else f"{label}..."
        if issues:
            text += f"  |  Issues: {issues}"
        self.lbl_status.setText(text)

    def _on_results(self, results):
        self.progress_bar.hide()
        self.issues = list(results.issues)
        self._show_issue_button()
        if results.aborted:
            self.lbl_status.setText(results.abort_reason or "Stopped.")
            if results.abort_reason:
                QMessageBox.warning(self, "Scan Stopped", results.abort_reason)
            return

        self.exact_groups = results.exact_groups
        self.model.set_matches(results.matches)
        self._apply_filter()
        exact = len(results.matches) - sum(1 for m in results.matches if m.type == 'FUZZY')
        fuzzy = len(results.matches) - exact
        summary = (f"{exact:,} exact, {fuzzy:,} similar  |  "
                   f"{format_bytes(results.reclaimable_bytes)} reclaimable from exact copies")
        if results.truncated:
            summary += "  |  result cap reached, showing the closest matches"
        self.lbl_summary.setText(summary)

        status = f"Found {len(results.matches):,} duplicate pair(s)."
        scan = results.scan
        if scan is not None:
            status += (f"  {scan.files:,} files, {scan.reused:,} unchanged, "
                       f"{scan.elapsed:.1f}s.")
        if results.stale_fingerprints:
            status += (f"  {results.stale_fingerprints:,} video/audio files have fingerprints "
                       "from an older version; run a scan to update them.")
        self.lbl_status.setText(status)
        if not results.matches:
            QMessageBox.information(self, "Clean!", "No duplicates found.")

    def _on_failed(self, message):
        self.progress_bar.hide()
        self.lbl_status.setText("Scan failed.")
        QMessageBox.critical(self, "Error", message)

    def _on_worker_finished(self):
        if self.watchdog is not None:
            self.watchdog.stop()
        self._update_actions()

    def _show_issue_button(self):
        if self.issues:
            count = len(self.issues)
            self.btn_issues.setText(f"{count} {'File' if count == 1 else 'Files'} With Issues")
            self.btn_issues.show()
        else:
            self.btn_issues.hide()
        self._update_actions()

    def show_issues(self):
        if self.issues:
            IssuesDialog(self.issues, self).exec()

    def show_options(self):
        OptionsDialog(self.settings, self).exec()

    def _update_resources(self):
        alert = False
        if self.watchdog is not None and self.worker and self.worker.isRunning():
            snap = self.watchdog.snapshot()
            parts = [f"RAM {format_bytes(snap.rss)}", f"CPU {snap.cpu_percent:.0f}%"]
            if snap.total:
                parts.append(f"Free {format_bytes(snap.available)}")
            if snap.reason:
                parts.append(snap.reason)
                alert = True
            if snap.stalled_path:
                parts.append(f"Slow: {os.path.basename(snap.stalled_path)} "
                             f"({fmt_eta(snap.stalled_seconds)})")
                alert = True
            text = "  |  ".join(parts)
            tip = snap.stalled_path or ""
        elif psutil is not None:
            try:
                rss = psutil.Process().memory_info().rss
                text, tip = f"RAM {format_bytes(rss)}", ""
            except Exception:
                text, tip = "", ""
        else:
            text, tip = "", ""
        self.lbl_resources.setText(text)
        self.lbl_resources.setToolTip(tip)
        set_dynamic(self.lbl_resources, "alert", "true" if alert else "false")

    # -------------------------------------------------------------------------
    # Results list
    # -------------------------------------------------------------------------

    def _apply_filter(self):
        current = self.current_match
        _, value = FILTERS[self.combo_filter.currentIndex()]
        self.model.set_filter(value, self.edit_search.text())
        self._select_row(self._row_of(current) if current else 0)

    def _row_of(self, match):
        for row in range(self.model.rowCount()):
            if self.model.match_at(row) is match:
                return row
        return 0

    def _select_row(self, row):
        count = self.model.rowCount()
        if count == 0:
            self.current_match = None
            self._clear_panels()
            self._update_actions()
            return
        row = max(0, min(row, count - 1))
        index = self.model.index(row)
        self.match_list.setCurrentIndex(index)
        self.match_list.scrollTo(index)
        # setCurrentIndex does not emit when the row is unchanged.
        self.load_match_details(row)

    def load_match_details(self, row):
        match = self.model.match_at(row)
        if match is None:
            return
        if match is self.current_match and self.panel_a['filepath'] == match.file_a:
            return
        self.current_match = match
        score = "Exact Match" if match.type == 'EXACT' else f"{int(match.score)}% Match"
        self.lbl_score.setText(score)
        self.load_file_to_panel(self.panel_a, match.file_a, 'A')
        self.load_file_to_panel(self.panel_b, match.file_b, 'B')
        self._update_actions()

    def _clear_panels(self):
        self.lbl_score.setText("")
        for panel, key in ((self.panel_a, 'A'), (self.panel_b, 'B')):
            self._preview_token[key] += 1
            panel['filepath'] = None
            self.pixmap_cache[key] = None
            set_dynamic(panel['img'], "kind", "image")
            panel['img'].setPixmap(QPixmap())
            panel['img'].setText("")
            for field in ('filename', 'path', 'details', 'dates'):
                panel[field].setText("")

    def load_file_to_panel(self, panel, filepath, key):
        panel['filepath'] = filepath
        self.pixmap_cache[key] = None
        self._preview_token[key] += 1
        img = panel['img']
        set_dynamic(img, "kind", "image")
        img.setPixmap(QPixmap())
        img.setText("Loading...")
        img.setWordWrap(False)
        img.setAlignment(Qt.AlignmentFlag.AlignCenter)
        panel['filename'].setText(os.path.basename(filepath))
        panel['path'].setText(os.path.dirname(filepath))
        panel['details'].setText("")
        panel['dates'].setText("")
        longest = max(img.width(), img.height(), 800)
        self.preview_pool.start(PreviewTask(self.preview_signals, self._preview_token[key],
                                            key, filepath, max_dim=min(2400, longest * 2)))

    def _on_preview_ready(self, token, key, image, info):
        if token != self._preview_token.get(key):
            return
        panel = self.panel_a if key == 'A' else self.panel_b
        img = panel['img']
        if info.get('missing'):
            panel['filename'].setText("File Missing")
            img.setText("Missing on Disk")
            return
        if info.get('error'):
            img.setText("Unreadable")
            panel['details'].setText(info['error'])
            return

        details = f"Size: {format_bytes(info.get('size', 0))}"
        if info.get('res'):
            details += f"  |  Res: {info['res']}"
        if info.get('extra'):
            details += f"  |  {info['extra']}"
        panel['details'].setText(details)
        panel['dates'].setText(f"Created: {info.get('ctime', '')}  |  "
                               f"Modified: {info.get('mtime', '')}")

        kind = info.get('kind')
        if image is not None:
            pixmap = QPixmap.fromImage(image)
            self.pixmap_cache[key] = pixmap
            img.setText("")
            self.update_image_display(panel, pixmap)
        elif kind == 'text' and 'text' in info:
            set_dynamic(img, "kind", "text")
            img.setWordWrap(True)
            img.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
            img.setText(info['text'])
        else:
            set_dynamic(img, "kind", "audio" if kind == 'audio' else "generic")
            img.setText(info.get('placeholder', ""))

    def update_image_display(self, panel, pixmap):
        if pixmap and not pixmap.isNull():
            width, height = panel['img'].width(), panel['img'].height()
            if width > 10 and height > 10:
                panel['img'].setPixmap(pixmap.scaled(
                    width, height, Qt.AspectRatioMode.KeepAspectRatio,
                    Qt.TransformationMode.SmoothTransformation,
                ))

    def _apply_cached_pixmaps(self):
        for panel, key in ((self.panel_a, 'A'), (self.panel_b, 'B')):
            if self.pixmap_cache[key]:
                self.update_image_display(panel, self.pixmap_cache[key])

    def next_match(self):
        row = self.match_list.currentIndex().row()
        if row < self.model.rowCount() - 1:
            self._select_row(row + 1)
        else:
            self.lbl_status.setText("End of the list.")

    # -------------------------------------------------------------------------
    # Deleting
    # -------------------------------------------------------------------------

    def delete_files(self, sides):
        if self.current_match is None or self._busy():
            return
        paths = [p for p in (self.panel_a['filepath'] if 'A' in sides else None,
                             self.panel_b['filepath'] if 'B' in sides else None) if p]
        if not paths:
            return
        if self.settings.confirm_delete:
            noun = "this file" if len(paths) == 1 else "both files"
            confirm = QMessageBox.question(self, "Move to Trash",
                                           f"Move {noun} to the Trash?\n\n" + "\n".join(paths))
            if confirm != QMessageBox.StandardButton.Yes:
                return
        trashed = []
        for path in paths:
            try:
                if os.path.lexists(path):
                    send2trash(path)
                trashed.append(path)
            except Exception as exc:
                QMessageBox.critical(self, "Error", f"Could not move to Trash:\n{path}\n\n{exc}")
        if trashed:
            names = ", ".join(os.path.basename(p) for p in trashed)
            self.lbl_status.setText(f"Moved to Trash: {names}")
            self._purge_paths(trashed)
        self.match_list.setFocus()

    def _purge_paths(self, paths):
        """Drop every match and group entry that references a removed path, and
        drop the paths from the index."""
        gone = set(paths)
        if not gone:
            return
        row = self.match_list.currentIndex().row()
        self.current_match = None
        self.model.remove_paths(gone)
        groups = [[p for p in g if p not in gone] for g in self.exact_groups]
        self.exact_groups = [g for g in groups if len(g) > 1]
        db = self.last_db_path
        if db and os.path.exists(db):
            try:
                index_db = Index(db)
                index_db.delete_paths(gone)
                index_db.close()
            except Exception:
                log.exception("Could not remove trashed files from the index")
        if self.model.rowCount() == 0:
            self._clear_panels()
            self.lbl_status.setText("No more matches.")
        else:
            self._select_row(max(0, row))
        self._update_actions()

    # -------------------------------------------------------------------------
    # Auto-prune
    # -------------------------------------------------------------------------

    def get_folder_priority(self, filepath):
        """Priority of the most specific scan root containing `filepath`."""
        target = normalize_root(filepath)
        best_priority, best_len = None, -1
        for folder in self.scan_folders:
            root = normalize_root(folder['path']).rstrip(os.sep)
            inside = target == root or target.startswith(root + os.sep)
            if inside and len(root) > best_len:
                best_len, best_priority = len(root), folder['priority']
        return best_priority if best_priority is not None else DEFAULT_FOLDER_PRIORITY

    def select_prune_victims(self):
        """
        One keeper per exact group: highest folder priority, then shortest
        path, then alphabetical so the choice is deterministic. Returns
        [(victim, keeper)].
        """
        plan = []
        for group in self.exact_groups:
            existing = [p for p in group if os.path.exists(p)]
            if len(existing) < 2:
                continue
            keeper = min(existing, key=lambda p: (-self.get_folder_priority(p), len(p), p))
            plan.extend((p, keeper) for p in existing if p != keeper)
        return plan

    def auto_prune_exact(self):
        if self._busy():
            return
        plan = self.select_prune_victims()
        if not plan:
            QMessageBox.information(self, "Auto-Prune", "No exact duplicates left to prune.")
            return
        box = QMessageBox(self)
        box.setIcon(QMessageBox.Icon.Question)
        box.setWindowTitle("Auto-Prune Exact Duplicates")
        box.setText(f"Move {len(plan):,} duplicate file(s) to the Trash, keeping one copy "
                    f"from each of {len(self.exact_groups):,} group(s)?")
        box.setInformativeText("Show Details lists every file and the copy that is kept.")
        detail = "\n".join(f"{victim}\n    kept: {keeper}" for victim, keeper in plan[:5000])
        if len(plan) > 5000:
            detail += f"\n... and {len(plan) - 5000:,} more"
        box.setDetailedText(detail)
        box.setStandardButtons(QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        box.setDefaultButton(QMessageBox.StandardButton.No)
        if box.exec() != QMessageBox.StandardButton.Yes:
            return

        victims = [v for v, _ in plan]
        self.prune_dialog = QProgressDialog("Moving duplicates to the Trash...", "Cancel",
                                            0, len(victims), self)
        self.prune_dialog.setWindowTitle("Auto-Prune")
        self.prune_dialog.setWindowModality(Qt.WindowModality.WindowModal)
        self.prune_dialog.setAutoClose(False)
        self.prune_dialog.setAutoReset(False)
        self.prune_dialog.setMinimumDuration(200)

        self.prune_worker = PruneWorker(victims)
        self.prune_worker.progress_value.connect(self._on_prune_progress)
        self.prune_worker.prune_finished.connect(self._on_prune_finished)
        self.prune_worker.finished.connect(self._update_actions)
        self.prune_dialog.canceled.connect(self.prune_worker.stop)
        self.prune_worker.start()
        self._update_actions()

    def _on_prune_progress(self, current, total):
        if self.prune_dialog is not None:
            self.prune_dialog.setValue(current)
            self.prune_dialog.setLabelText(f"Moving to Trash: {current:,} / {total:,}")

    def _on_prune_finished(self, trashed, failed):
        if self.prune_dialog is not None:
            self.prune_dialog.close()
            self.prune_dialog = None
        self._purge_paths(trashed)
        message = f"Moved {len(trashed):,} file(s) to the Trash."
        if failed:
            message += f"\n\n{len(failed):,} could not be moved:\n" + "\n".join(
                f"{p}: {e}" for p, e in failed[:20]
            )
        QMessageBox.information(self, "Auto-Prune", message)
        self.lbl_status.setText(f"Pruned {len(trashed):,} file(s).")

    # -------------------------------------------------------------------------
    # Export
    # -------------------------------------------------------------------------

    def export_results(self):
        matches = self.model.all_matches()
        if not matches:
            return
        path, chosen = QFileDialog.getSaveFileName(
            self, "Export Results", "duplicates.csv",
            "CSV (*.csv);;JSON (*.json)",
        )
        if not path:
            return
        try:
            if path.lower().endswith(".json") or chosen.startswith("JSON"):
                results = pipeline.Results(matches=matches, exact_groups=self.exact_groups,
                                           issues=self.issues)
                export.write_json(results, path, self.scan_folders)
            else:
                export.write_csv(matches, path)
            self.lbl_status.setText(f"Exported {len(matches):,} matches to {path}")
        except OSError as exc:
            QMessageBox.critical(self, "Export Failed", str(exc))

    # -------------------------------------------------------------------------
    # Events
    # -------------------------------------------------------------------------

    def keyPressEvent(self, event):
        if event.key() == Qt.Key.Key_Space and self.current_match is not None \
                and not self.edit_search.hasFocus():
            self.next_match()
            return
        super().keyPressEvent(event)

    def resizeEvent(self, event):
        if self.current_match is not None:
            self._resize_timer.start()
        super().resizeEvent(event)

    def closeEvent(self, event):
        self._save_window_state()
        for worker in (self.worker, self.prune_worker):
            if worker and worker.isRunning():
                worker.stop()
                if not worker.wait(15000):
                    # A decoder stuck inside native code cannot be interrupted.
                    # The index is in WAL mode, so leaving now is safe.
                    log.warning("Worker did not stop in time; exiting anyway")
                    logging.shutdown()
                    os._exit(0)
        if self.watchdog is not None:
            self.watchdog.stop()
        self.preview_pool.clear()
        self.preview_pool.waitForDone(3000)
        event.accept()

    def show_about(self):
        QMessageBox.about(
            self, f"About {APP_NAME}",
            f"<h3>{APP_NAME}</h3><p>Version {VERSION}</p>"
            "<p>Find exact and visually, acoustically or textually similar duplicate "
            "files.</p>"
            f"<p><a href='{PROJECT_URL}'>{PROJECT_URL}</a></p>"
            f"<p>Index folder:<br>{index_dir()}</p>",
        )


def main(argv=None):
    argv = sys.argv if argv is None else argv
    app = QApplication(argv)
    app.setApplicationName(APP_NAME)
    app.setApplicationVersion(VERSION)
    app.setOrganizationName("FuzzyDuplicateFinder")
    app.setDesktopFileName(APP_ID)
    app.setWindowIcon(load_app_icon())

    theme = ThemeManager(app)
    theme.apply()

    window = DuplicateFinderApp(theme)
    window.show()
    return app.exec()

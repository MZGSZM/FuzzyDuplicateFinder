"""Scan options and the files-with-issues list."""

from __future__ import annotations

from PyQt6.QtWidgets import (
    QApplication,
    QCheckBox,
    QDialog,
    QDialogButtonBox,
    QFileDialog,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QSpinBox,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
)

from .exts import KINDS
from .fingerprints import audio_installed
from .scanner import DEFAULT_EXCLUDES

KIND_LABELS = {'image': "Images", 'video': "Videos", 'audio': "Audio", 'text': "Text and code"}


class OptionsDialog(QDialog):
    def __init__(self, settings, parent=None):
        super().__init__(parent)
        self.settings = settings
        self.setWindowTitle("Scan Options")
        self.setMinimumWidth(520)
        layout = QVBoxLayout(self)

        # What to look for
        look = QGroupBox("Look for similar")
        look_layout = QVBoxLayout(look)
        self.kind_boxes = {}
        row = QHBoxLayout()
        for kind in KINDS:
            box = QCheckBox(KIND_LABELS[kind])
            box.setChecked(kind in settings.kinds)
            if kind == 'audio' and not audio_installed():
                box.setToolTip("librosa is not installed, so audio fingerprinting is off.")
            self.kind_boxes[kind] = box
            row.addWidget(box)
        row.addStretch()
        look_layout.addLayout(row)
        self.chk_all_types = QCheckBox("Also find exact duplicates of every other file type")
        self.chk_all_types.setToolTip(
            "Documents, archives, disk images and so on. These are matched byte for byte\n"
            "only; there is no similarity check for them."
        )
        self.chk_all_types.setChecked(settings.all_types)
        look_layout.addWidget(self.chk_all_types)
        layout.addWidget(look)

        # Filters
        filters = QGroupBox("Filters")
        form = QFormLayout(filters)
        self.chk_hidden = QCheckBox("Skip hidden files and folders")
        self.chk_hidden.setChecked(settings.skip_hidden)
        form.addRow(self.chk_hidden)

        exclude_row = QHBoxLayout()
        self.edit_excludes = QLineEdit("; ".join(settings.excludes))
        self.edit_excludes.setToolTip(
            "Names to skip, separated by semicolons. Wildcards work (*.tmp, .Trash-*).\n"
            "A pattern with a slash is matched against the full path instead."
        )
        btn_reset = QPushButton("Defaults")
        btn_reset.clicked.connect(
            lambda: self.edit_excludes.setText("; ".join(DEFAULT_EXCLUDES))
        )
        exclude_row.addWidget(self.edit_excludes, 1)
        exclude_row.addWidget(btn_reset)
        form.addRow("Exclude:", exclude_row)

        self.spin_min_size = QSpinBox()
        self.spin_min_size.setRange(0, 10_000_000)
        self.spin_min_size.setSuffix(" KB")
        self.spin_min_size.setSpecialValueText("No minimum (empty files are always skipped)")
        self.spin_min_size.setValue(settings.min_size_kb)
        form.addRow("Minimum size:", self.spin_min_size)
        layout.addWidget(filters)

        # Performance
        perf = QGroupBox("Resources")
        perf_form = QFormLayout(perf)
        self.chk_low_priority = QCheckBox("Run scans at background CPU priority")
        self.chk_low_priority.setToolTip(
            "Keeps the desktop responsive during a scan. On Linux and macOS the\n"
            "priority stays lowered until the app is restarted."
        )
        self.chk_low_priority.setChecked(settings.low_priority)
        perf_form.addRow(self.chk_low_priority)
        self.chk_watchdog = QCheckBox("Pause when free memory runs low, stop if it gets critical")
        self.chk_watchdog.setChecked(settings.watchdog)
        perf_form.addRow(self.chk_watchdog)
        self.spin_stall = QSpinBox()
        self.spin_stall.setRange(10, 3600)
        self.spin_stall.setSuffix(" s")
        self.spin_stall.setValue(settings.stall_seconds)
        self.spin_stall.setToolTip("A file still being processed after this long is named in "
                                   "the status bar and listed as an issue.")
        perf_form.addRow("Flag files slower than:", self.spin_stall)
        layout.addWidget(perf)

        review = QGroupBox("Review")
        review_layout = QVBoxLayout(review)
        self.chk_confirm = QCheckBox("Ask before moving each file to the Trash")
        self.chk_confirm.setChecked(settings.confirm_delete)
        review_layout.addWidget(self.chk_confirm)
        layout.addWidget(review)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok
                                   | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def accept(self):
        kinds = {k for k, box in self.kind_boxes.items() if box.isChecked()}
        if not kinds and not self.chk_all_types.isChecked():
            QMessageBox.warning(self, "Scan Options",
                                "Pick at least one kind of file, or enable exact matching "
                                "of all file types.")
            return
        s = self.settings
        s.kinds = kinds
        s.all_types = self.chk_all_types.isChecked()
        s.skip_hidden = self.chk_hidden.isChecked()
        s.excludes = tuple(p.strip() for p in self.edit_excludes.text().split(";") if p.strip())
        s.min_size_kb = self.spin_min_size.value()
        s.low_priority = self.chk_low_priority.isChecked()
        s.watchdog = self.chk_watchdog.isChecked()
        s.stall_seconds = self.spin_stall.value()
        s.confirm_delete = self.chk_confirm.isChecked()
        super().accept()


class IssuesDialog(QDialog):
    def __init__(self, issues, parent=None):
        super().__init__(parent)
        self.issues = list(issues)
        self.setWindowTitle("Files With Issues")
        self.resize(820, 440)
        layout = QVBoxLayout(self)

        lbl = QLabel(
            f"{len(self.issues)} file(s) could not be fully processed. Files that could be "
            f"read are still indexed for exact-duplicate detection."
        )
        lbl.setWordWrap(True)
        layout.addWidget(lbl)

        self.tree = QTreeWidget()
        self.tree.setHeaderLabels(["File", "Problem"])
        self.tree.setRootIsDecorated(False)
        self.tree.setUniformRowHeights(True)
        self.tree.header().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.tree.header().setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        self.tree.addTopLevelItems([QTreeWidgetItem([p, r]) for p, r in self.issues])
        layout.addWidget(self.tree)

        row = QHBoxLayout()
        btn_copy = QPushButton("Copy")
        btn_copy.clicked.connect(self.copy_list)
        btn_export = QPushButton("Export to TXT...")
        btn_export.clicked.connect(self.export_list)
        btn_close = QPushButton("Close")
        btn_close.clicked.connect(self.close)
        row.addStretch()
        row.addWidget(btn_copy)
        row.addWidget(btn_export)
        row.addWidget(btn_close)
        layout.addLayout(row)

    def _as_text(self):
        return "\n".join(f"{p}\t{r}" for p, r in self.issues)

    def copy_list(self):
        QApplication.clipboard().setText(self._as_text())

    def export_list(self):
        path, _ = QFileDialog.getSaveFileName(self, "Export Issues", "scan_issues.txt",
                                              "Text Files (*.txt)")
        if not path:
            return
        try:
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(self._as_text() + "\n")
        except OSError as exc:
            QMessageBox.critical(self, "Export Failed", str(exc))


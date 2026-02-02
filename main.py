import sys
import os
import cv2
import threading
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                             QHBoxLayout, QPushButton, QLabel, QFileDialog, 
                             QListWidget, QListWidgetItem, QSplitter, QMessageBox, 
                             QProgressBar, QFrame)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QPixmap, QImage, QAction
from send2trash import send2trash

# Import your engines
# Make sure scanner_engine.py and matcher.py are in the same folder
from scanner_engine import Scanner
from matcher import Matcher

class WorkerThread(QThread):
    """Runs the heavy scanning/matching in the background so the UI doesn't freeze."""
    progress_update = pyqtSignal(str)
    finished = pyqtSignal()

    def __init__(self, target_dir):
        super().__init__()
        self.target_dir = target_dir

    def run(self):
        self.progress_update.emit("Scanning directory...")
        scanner = Scanner()
        scanner.scan_directory(self.target_dir)
        self.progress_update.emit("Scan complete. Analyzing matches...")
        self.finished.emit()

class DuplicateFinderApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Insane Duplicate Finder")
        self.resize(1200, 800)

        # State
        self.matches = []
        self.current_match_index = -1

        # --- UI LAYOUT ---
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)

        # Toolbar / Header
        header_layout = QHBoxLayout()
        self.btn_scan = QPushButton("Select Folder & Scan")
        self.btn_scan.clicked.connect(self.start_scan)
        self.lbl_status = QLabel("Ready")
        header_layout.addWidget(self.btn_scan)
        header_layout.addWidget(self.lbl_status)
        main_layout.addLayout(header_layout)

        # Progress Bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 0) # Indeterminate mode
        self.progress_bar.hide()
        main_layout.addWidget(self.progress_bar)

        # Splitter (List on left, Details on right)
        splitter = QSplitter(Qt.Orientation.Horizontal)
        
        # Left: List of Matches
        self.match_list = QListWidget()
        self.match_list.currentRowChanged.connect(self.load_match_details)
        splitter.addWidget(self.match_list)

        # Right: Comparison Area
        self.comparison_container = QWidget()
        comp_layout = QHBoxLayout(self.comparison_container)
        
        # File A View
        self.panel_a = self.create_file_panel("File A")
        # Middle Actions
        self.panel_actions = self.create_action_panel()
        # File B View
        self.panel_b = self.create_file_panel("File B")

        comp_layout.addLayout(self.panel_a)
        comp_layout.addLayout(self.panel_actions)
        comp_layout.addLayout(self.panel_b)
        
        splitter.addWidget(self.comparison_container)
        splitter.setStretchFactor(1, 4) # Give right side more space

        main_layout.addWidget(splitter)

        # Styling
        self.apply_styles()

    def create_file_panel(self, title):
        layout = QVBoxLayout()
        lbl_title = QLabel(title)
        lbl_title.setStyleSheet("font-weight: bold; font-size: 14px;")
        
        lbl_img = QLabel()
        lbl_img.setAlignment(Qt.AlignmentFlag.AlignCenter)
        lbl_img.setStyleSheet("background-color: #222; border: 1px solid #444;")
        lbl_img.setMinimumSize(300, 300)
        
        lbl_info = QLabel("No File")
        lbl_info.setWordWrap(True)
        
        btn_open = QPushButton("Open File")
        
        layout.addWidget(lbl_title)
        layout.addWidget(lbl_img)
        layout.addWidget(lbl_info)
        layout.addWidget(btn_open)
        
        return {
            "layout": layout,
            "img": lbl_img,
            "info": lbl_info,
            "btn_open": btn_open,
            "path": None
        }

    def create_action_panel(self):
        layout = QVBoxLayout()
        layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
        
        self.lbl_score = QLabel("0%")
        self.lbl_score.setStyleSheet("font-size: 24px; font-weight: bold; color: #00ff00;")
        
        btn_del_a = QPushButton("Delete A")
        btn_del_a.setStyleSheet("background-color: #aa0000; color: white; font-weight: bold;")
        btn_del_a.clicked.connect(lambda: self.delete_file("A"))
        
        btn_del_b = QPushButton("Delete B")
        btn_del_b.setStyleSheet("background-color: #aa0000; color: white; font-weight: bold;")
        btn_del_b.clicked.connect(lambda: self.delete_file("B"))
        
        btn_skip = QPushButton("Skip / Keep Both")
        btn_skip.clicked.connect(self.next_match)

        layout.addWidget(QLabel("Similarity"))
        layout.addWidget(self.lbl_score)
        layout.addSpacing(20)
        layout.addWidget(btn_del_a)
        layout.addWidget(btn_del_b)
        layout.addSpacing(20)
        layout.addWidget(btn_skip)
        
        return layout

    def start_scan(self):
        folder = QFileDialog.getExistingDirectory(self, "Select Directory")
        if folder:
            self.lbl_status.setText(f"Scanning: {folder}")
            self.match_list.clear()
            self.progress_bar.show()
            self.btn_scan.setEnabled(False)
            
            self.worker = WorkerThread(folder)
            self.worker.progress_update.connect(lambda s: self.lbl_status.setText(s))
            self.worker.finished.connect(self.on_scan_finished)
            self.worker.start()

    def on_scan_finished(self):
        self.progress_bar.hide()
        self.btn_scan.setEnabled(True)
        self.lbl_status.setText("Scan Complete. Loading matches...")
        self.load_matches()

    def load_matches(self):
        # Use your Matcher engine
        matcher = Matcher()
        
        # 1. Exact Matches
        exact = matcher.find_exact_duplicates()
        # 2. Fuzzy Matches
        fuzzy = matcher.find_fuzzy_matches()
        
        self.matches = []
        
        # Format Exact matches into pairs for the UI
        for group in exact:
            # If we have 3 copies (A, B, C), compare A-B, then A-C
            base = group[0]
            for duplicate in group[1:]:
                self.matches.append({
                    'file_a': base['path'],
                    'file_b': duplicate['path'],
                    'score': 100.0,
                    'type': 'Exact MD5'
                })

        # Add Fuzzy matches
        self.matches.extend(fuzzy)

        # Sort by score
        self.matches.sort(key=lambda x: x['score'], reverse=True)

        # Populate List
        for m in self.matches:
            name_a = os.path.basename(m['file_a'])
            name_b = os.path.basename(m['file_b'])
            item = QListWidgetItem(f"[{m['score']}%] {name_a} vs {name_b}")
            self.match_list.addItem(item)

        self.lbl_status.setText(f"Found {len(self.matches)} potential duplicates.")

    def load_match_details(self, row_index):
        if row_index < 0 or row_index >= len(self.matches): return
        
        data = self.matches[row_index]
        self.current_match_index = row_index
        
        # Update Score
        self.lbl_score.setText(f"{int(data['score'])}%")
        
        # Load Files
        self.load_file_into_panel(self.panel_a, data['file_a'])
        self.load_file_into_panel(self.panel_b, data['file_b'])

    def load_file_into_panel(self, panel, filepath):
        panel['path'] = filepath
        
        if not os.path.exists(filepath):
            panel['info'].setText("File deleted or missing.")
            panel['img'].clear()
            panel['img'].setText("Missing")
            return

        # Info Text
        size_mb = os.path.getsize(filepath) / (1024 * 1024)
        ext = os.path.splitext(filepath)[1].lower()
        info = f"{os.path.basename(filepath)}\n{size_mb:.2f} MB\n{ext}"
        panel['info'].setText(info)
        
        # Button Connection
        try: 
            panel['btn_open'].clicked.disconnect() 
        except: pass
        panel['btn_open'].clicked.connect(lambda: os.startfile(filepath) if os.name == 'nt' else os.system(f"open '{filepath}'"))

        # Image Preview
        if ext in ['.jpg', '.png', '.jpeg', '.bmp']:
            pixmap = QPixmap(filepath)
            if not pixmap.isNull():
                panel['img'].setPixmap(pixmap.scaled(300, 300, Qt.AspectRatioMode.KeepAspectRatio))
            else:
                panel['img'].setText("Image Error")
        elif ext in ['.mp4', '.avi', '.mkv']:
            # Attempt to grab a thumbnail using CV2
            try:
                cap = cv2.VideoCapture(filepath)
                ret, frame = cap.read()
                cap.release()
                if ret:
                    frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                    h, w, ch = frame.shape
                    qimg = QImage(frame.data, w, h, ch * w, QImage.Format.Format_RGB888)
                    panel['img'].setPixmap(QPixmap.fromImage(qimg).scaled(300, 300, Qt.AspectRatioMode.KeepAspectRatio))
                else:
                    panel['img'].setText("Video (No Preview)")
            except:
                panel['img'].setText("Video File")
        else:
            panel['img'].setText(f"{ext} File")

    def delete_file(self, target):
        if self.current_match_index == -1: return
        
        panel = self.panel_a if target == "A" else self.panel_b
        filepath = panel['path']

        confirm = QMessageBox.question(self, "Confirm Delete", 
                                       f"Are you sure you want to send this to Trash?\n{filepath}",
                                       QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        
        if confirm == QMessageBox.StandardButton.Yes:
            try:
                send2trash(filepath)
                self.lbl_status.setText(f"Deleted {os.path.basename(filepath)}")
                # Move to next item automatically
                self.next_match()
            except Exception as e:
                QMessageBox.critical(self, "Error", str(e))

    def next_match(self):
        current_row = self.match_list.currentRow()
        if current_row < self.match_list.count() - 1:
            self.match_list.setCurrentRow(current_row + 1)

    def apply_styles(self):
        # Dark Theme because we are civilized
        self.setStyleSheet("""
            QMainWindow { background-color: #333; color: #fff; }
            QLabel { color: #eee; }
            QListWidget { background-color: #222; color: #fff; border: 1px solid #555; font-size: 14px; }
            QListWidget::item:selected { background-color: #007acc; }
            QPushButton { padding: 8px; background-color: #555; color: #fff; border-radius: 4px; }
            QPushButton:hover { background-color: #666; }
        """)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = DuplicateFinderApp()
    window.show()
    sys.exit(app.exec())
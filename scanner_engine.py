import os
import sqlite3
import hashlib
import time
import concurrent.futures
import threading
from datetime import datetime
import cv2
import imagehash
from PIL import Image
import numpy as np

try:
    import librosa
    AUDIO_AVAILABLE = True
except ImportError:
    AUDIO_AVAILABLE = False

IMAGE_EXTS = {'.jpg', '.jpeg', '.png', '.bmp', '.gif', '.webp', '.tiff'}
VIDEO_EXTS = {'.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv', '.m4v', '.webm', '.ts', '.mts'}
AUDIO_EXTS = {'.mp3', '.wav', '.flac', '.m4a', '.aac', '.ogg', '.wma'}
TEXT_EXTS = {'.txt', '.md', '.py', '.js', '.json', '.html', '.css', '.c', '.cpp'}

class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.lock = threading.Lock()
        self.conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False, isolation_level=None)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.create_table()

    def create_table(self):
        query = """
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            path TEXT UNIQUE,
            filename TEXT,
            extension TEXT,
            size INTEGER,
            mtime REAL,
            ctime REAL, 
            exact_hash TEXT,
            visual_hash TEXT,
            audio_hash TEXT,
            scan_date TEXT
        )
        """
        with self.lock: 
            self.conn.execute(query)
            self.conn.commit()

    def get_file_record(self, path):
        with self.lock:
            cursor = self.conn.execute("SELECT mtime, exact_hash, visual_hash, audio_hash FROM files WHERE path = ?", (path,))
            return cursor.fetchone()

    def upsert_file(self, data):
        query = """
        INSERT INTO files (path, filename, extension, size, mtime, ctime, exact_hash, visual_hash, audio_hash, scan_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(path) DO UPDATE SET
            size=excluded.size, mtime=excluded.mtime, ctime=excluded.ctime,
            exact_hash=excluded.exact_hash, visual_hash=excluded.visual_hash,
            audio_hash=excluded.audio_hash, scan_date=excluded.scan_date
        """
        try:
            with self.lock:
                self.conn.execute(query, data)
                self.conn.commit()
        except Exception as e:
            print(f"DB Write Error: {e}")

    def close(self):
        try:
            self.conn.close()
        except:
            pass

class Scanner:
    def __init__(self):
        self.db = None 

    def generate_exact_hash(self, filepath):
        try:
            hasher = hashlib.md5()
            with open(filepath, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hasher.update(chunk)
            return hasher.hexdigest()
        except Exception:
            return None

    def generate_visual_hash(self, filepath, ext):
        try:
            img = None
            if ext in IMAGE_EXTS:
                img = Image.open(filepath)
            elif ext in VIDEO_EXTS:
                cap = cv2.VideoCapture(filepath)
                if cap.isOpened():
                    total = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
                    cap.set(cv2.CAP_PROP_POS_FRAMES, total // 2)
                    ret, frame = cap.read()
                    cap.release()
                    if ret:
                        img = Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))
            if img: return str(imagehash.phash(img))
        except Exception:
            pass
        return None

    def generate_audio_hash(self, filepath):
        if not AUDIO_AVAILABLE: return None
        try:
            import warnings
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                y, sr = librosa.load(filepath, duration=30, sr=22050)
                chroma = librosa.feature.chroma_stft(y=y, sr=sr)
                chroma_mean = np.mean(chroma, axis=1)
                fingerprint = ",".join([str(round(x, 1)) for x in chroma_mean])
                return hashlib.md5(fingerprint.encode()).hexdigest()
        except Exception:
            return None

    def process_file(self, filepath):
        try:
            filepath = os.path.abspath(os.path.normpath(filepath))
            stats = os.stat(filepath)
            size = stats.st_size
            mtime = stats.st_mtime
            ctime = getattr(stats, 'st_ctime', 0)
            filename = os.path.basename(filepath)
            ext = os.path.splitext(filepath)[1].lower()

            is_img = ext in IMAGE_EXTS
            is_vid = ext in VIDEO_EXTS
            is_aud = ext in AUDIO_EXTS
            is_txt = ext in TEXT_EXTS

            if not any([is_img, is_vid, is_aud, is_txt]): return

            existing = self.db.get_file_record(filepath)
            if existing and round(existing[0], 2) == round(mtime, 2): return 

            # print(f"Processing: {filename}")
            
            exact_hash = self.generate_exact_hash(filepath)
            visual_hash = None
            audio_hash = None

            if is_img or is_vid: visual_hash = self.generate_visual_hash(filepath, ext)
            if is_aud: audio_hash = self.generate_audio_hash(filepath)

            data = (filepath, filename, ext, size, mtime, ctime, exact_hash, visual_hash, audio_hash, datetime.now().isoformat())
            self.db.upsert_file(data)
            
        except Exception as e:
            print(f"Error on {filepath}: {e}")

    def scan_directory(self, folder_list, db_path, stop_signal=None):
        """
        stop_signal: A function that returns True if we should stop.
        """
        print(f"--- Starting Scan ---")
        self.db = DatabaseManager(db_path)
        files_to_process = []
        
        for root_dir in folder_list:
            if stop_signal and stop_signal(): break
            root_dir = os.path.abspath(os.path.normpath(root_dir))
            for root, dirs, files in os.walk(root_dir):
                if stop_signal and stop_signal(): break
                for file in files:
                    if file == "duplicate_index.db": continue
                    files_to_process.append(os.path.join(root, file))

        print(f"Found {len(files_to_process)} files. Processing...")
        
        # Use ThreadPool
        # We cannot easily kill threads in Python, so we iterate and check signal
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            for filepath in files_to_process:
                if stop_signal and stop_signal():
                    print("Scan stopping...")
                    break
                futures.append(executor.submit(self.process_file, filepath))
            
            # Wait for submitted tasks
            concurrent.futures.wait(futures)

        # CRITICAL: Close DB connection so file can be deleted
        self.db.close()
        
        if stop_signal and stop_signal():
            return None
            
        print("--- Scan Complete ---")
        return db_path
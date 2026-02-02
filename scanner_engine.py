import os
import sqlite3
import hashlib
import time
import concurrent.futures
from datetime import datetime
import cv2
import imagehash
from PIL import Image
import numpy as np

# Try importing librosa for audio analysis
try:
    import librosa
    AUDIO_AVAILABLE = True
except ImportError:
    AUDIO_AVAILABLE = False
    print("Warning: 'librosa' not found. Audio sonic analysis will be skipped.")
    print("Run: pip install librosa numpy")

# --- CONFIGURATION ---
DB_NAME = "file_index.db"
MAX_FILE_SIZE_MB = 2000 

IMAGE_EXTS = {'.jpg', '.jpeg', '.png', '.bmp', '.gif', '.webp'}
VIDEO_EXTS = {'.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv'}
# New Audio Extensions
AUDIO_EXTS = {'.mp3', '.wav', '.flac', '.m4a', '.aac', '.ogg'}
TEXT_EXTS = {'.txt', '.md', '.py', '.js', '.json', '.html'}

class DatabaseManager:
    def __init__(self, db_name=DB_NAME):
        # FIX: Added timeout and isolation_level to prevent "API Misuse" / Locking errors
        self.conn = sqlite3.connect(
            db_name, 
            timeout=30, 
            check_same_thread=False,
            isolation_level=None
        )
        # Enable Write-Ahead Logging for better speed/concurrency
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.create_table()

    def create_table(self):
        # Added 'audio_hash' column
        query = """
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            path TEXT UNIQUE,
            filename TEXT,
            extension TEXT,
            size INTEGER,
            mtime REAL,
            exact_hash TEXT,
            visual_hash TEXT,
            audio_hash TEXT,
            scan_date TEXT
        )
        """
        self.conn.execute(query)
        self.conn.commit()

    def get_file_record(self, path):
        # We fetch audio_hash too now
        cursor = self.conn.execute(
            "SELECT mtime, exact_hash, visual_hash, audio_hash FROM files WHERE path = ?", 
            (path,)
        )
        return cursor.fetchone()

    def upsert_file(self, data):
        query = """
        INSERT INTO files (path, filename, extension, size, mtime, exact_hash, visual_hash, audio_hash, scan_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(path) DO UPDATE SET
            size=excluded.size,
            mtime=excluded.mtime,
            exact_hash=excluded.exact_hash,
            visual_hash=excluded.visual_hash,
            audio_hash=excluded.audio_hash,
            scan_date=excluded.scan_date
        """
        try:
            self.conn.execute(query, data)
            self.conn.commit()
        except Exception as e:
            print(f"DB Write Error: {e}")

    def close(self):
        self.conn.close()

class Scanner:
    def __init__(self):
        self.db = DatabaseManager()

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
            if img:
                return str(imagehash.phash(img))
        except Exception:
            pass
        return None

    def generate_audio_hash(self, filepath):
        """
        Generates a 'sonic fingerprint' using Chroma features.
        We take the first 30 seconds, extract pitch features, and hash the average.
        """
        if not AUDIO_AVAILABLE: return None
        try:
            # Load only first 30s to keep it fast
            y, sr = librosa.load(filepath, duration=30, sr=22050)
            
            # Extract Chroma STFT (Pitch classes: 12 tones)
            chroma = librosa.feature.chroma_stft(y=y, sr=sr)
            
            # Calculate the mean of each pitch class across time
            chroma_mean = np.mean(chroma, axis=1)
            
            # Convert this array of floats into a string hash
            # We round to 1 decimal place to allow for "fuzziness"
            fingerprint = ",".join([str(round(x, 1)) for x in chroma_mean])
            
            # MD5 the fingerprint string to make it storable/comparable
            return hashlib.md5(fingerprint.encode()).hexdigest()
        except Exception as e:
            print(f"Audio Error {os.path.basename(filepath)}: {e}")
            return None

    def process_file(self, filepath):
        try:
            stats = os.stat(filepath)
            size = stats.st_size
            mtime = stats.st_mtime
            filename = os.path.basename(filepath)
            ext = os.path.splitext(filepath)[1].lower()

            # Filter valid extensions
            is_img = ext in IMAGE_EXTS
            is_vid = ext in VIDEO_EXTS
            is_aud = ext in AUDIO_EXTS
            is_txt = ext in TEXT_EXTS

            if not any([is_img, is_vid, is_aud, is_txt]):
                return

            existing = self.db.get_file_record(filepath)
            
            # Optimization: If file is in DB and hasn't changed, skip
            if existing and existing[0] == mtime:
                return 

            print(f"Processing: {filename}")
            
            exact_hash = self.generate_exact_hash(filepath)
            visual_hash = None
            audio_hash = None

            if is_img or is_vid:
                visual_hash = self.generate_visual_hash(filepath, ext)
            
            if is_aud:
                audio_hash = self.generate_audio_hash(filepath)

            # Data tuple must match the SQL INSERT statement count (9 items now)
            data = (filepath, filename, ext, size, mtime, exact_hash, visual_hash, audio_hash, datetime.now().isoformat())
            self.db.upsert_file(data)
            
        except PermissionError:
            print(f"Permission Denied: {filepath}")
        except Exception as e:
            print(f"Error on {filepath}: {e}")

    def scan_directory(self, root_dir):
        print(f"--- Starting Scan of {root_dir} ---")
        files_to_process = []
        for root, dirs, files in os.walk(root_dir):
            for file in files:
                files_to_process.append(os.path.join(root, file))

        print(f"Found {len(files_to_process)} files. Processing...")
        
        # Reduced workers to 2 to play nice with the database lock
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            executor.map(self.process_file, files_to_process)
            
        print("--- Scan Complete ---")

if __name__ == "__main__":
    scanner = Scanner()
    target_dir = input("Enter directory path to scan: ").strip('"')
    if os.path.exists(target_dir):
        start_time = time.time()
        scanner.scan_directory(target_dir)
        print(f"Duration: {round(time.time() - start_time, 2)} seconds")
    else:
        print("Invalid directory path.")

import os
import sqlite3
import hashlib
import time
import concurrent.futures
from datetime import datetime
import cv2
import imagehash
from PIL import Image

# --- CONFIGURATION ---
DB_NAME = "file_index.db"
# Files larger than this will be skipped to save time during testing (optional)
MAX_FILE_SIZE_MB = 2000 
# Extensions to look for
IMAGE_EXTS = {'.jpg', '.jpeg', '.png', '.bmp', '.gif', '.webp'}
VIDEO_EXTS = {'.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv'}
TEXT_EXTS = {'.txt', '.md', '.py', '.js', '.json', '.html'}

class DatabaseManager:
    def __init__(self, db_name=DB_NAME):
        self.conn = sqlite3.connect(db_name, check_same_thread=False)
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
            exact_hash TEXT,
            visual_hash TEXT,
            scan_date TEXT
        )
        """
        self.conn.execute(query)
        self.conn.commit()

    def get_file_record(self, path):
        cursor = self.conn.execute("SELECT mtime, exact_hash, visual_hash FROM files WHERE path = ?", (path,))
        return cursor.fetchone()

    def upsert_file(self, data):
        """Insert or Update a file record."""
        query = """
        INSERT INTO files (path, filename, extension, size, mtime, exact_hash, visual_hash, scan_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(path) DO UPDATE SET
            size=excluded.size,
            mtime=excluded.mtime,
            exact_hash=excluded.exact_hash,
            visual_hash=excluded.visual_hash,
            scan_date=excluded.scan_date
        """
        try:
            self.conn.execute(query, data)
            self.conn.commit()
        except Exception as e:
            print(f"DB Error: {e}")

    def close(self):
        self.conn.close()

class Scanner:
    def __init__(self):
        self.db = DatabaseManager()

    def generate_exact_hash(self, filepath):
        """MD5 for binary exactness."""
        try:
            hasher = hashlib.md5()
            with open(filepath, 'rb') as f:
                # Read in chunks to avoid memory overflow
                for chunk in iter(lambda: f.read(4096), b""):
                    hasher.update(chunk)
            return hasher.hexdigest()
        except Exception:
            return None

    def generate_visual_hash(self, filepath, ext):
        """pHash for Images and Video Snapshots."""
        try:
            img = None
            if ext in IMAGE_EXTS:
                img = Image.open(filepath)
            
            elif ext in VIDEO_EXTS:
                # Extract frame from middle of video
                cap = cv2.VideoCapture(filepath)
                if cap.isOpened():
                    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
                    cap.set(cv2.CAP_PROP_POS_FRAMES, total_frames // 2)
                    ret, frame = cap.read()
                    cap.release()
                    if ret:
                        img = Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))
            
            if img:
                # Return string representation of the hash
                return str(imagehash.phash(img))
        except Exception as e:
            print(f"Visual Hash Error ({filepath}): {e}")
        return None

    def process_file(self, filepath):
        """Worker function to process a single file."""
        try:
            stats = os.stat(filepath)
            size = stats.st_size
            mtime = stats.st_mtime
            filename = os.path.basename(filepath)
            ext = os.path.splitext(filepath)[1].lower()

            # Skip massive files for now or irrelevant extensions
            if ext not in IMAGE_EXTS and ext not in VIDEO_EXTS and ext not in TEXT_EXTS:
                return

            # Check DB: Do we need to rescan?
            existing = self.db.get_file_record(filepath)
            
            exact_hash = None
            visual_hash = None

            # If file exists in DB and mtime hasn't changed, skip heavy processing
            if existing and existing[0] == mtime:
                # print(f"Skipping (Unchanged): {filename}")
                return 

            print(f"Processing: {filename}")
            
            # 1. Exact Hash (All monitored files)
            exact_hash = self.generate_exact_hash(filepath)

            # 2. Visual Hash (Images/Video only)
            if ext in IMAGE_EXTS or ext in VIDEO_EXTS:
                visual_hash = self.generate_visual_hash(filepath, ext)

            # Save to DB
            data = (filepath, filename, ext, size, mtime, exact_hash, visual_hash, datetime.now().isoformat())
            self.db.upsert_file(data)
            
        except PermissionError:
            print(f"Permission Denied: {filepath}")
        except Exception as e:
            print(f"Error on {filepath}: {e}")

    def scan_directory(self, root_dir):
        print(f"--- Starting Scan of {root_dir} ---")
        files_to_process = []
        
        # 1. Walk directory and collect file paths
        for root, dirs, files in os.walk(root_dir):
            for file in files:
                files_to_process.append(os.path.join(root, file))

        print(f"Found {len(files_to_process)} files. Starting processing pool...")

        # 2. Process in parallel (adjust max_workers based on your CPU)
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            executor.map(self.process_file, files_to_process)
            
        print("--- Scan Complete ---")

# --- EXECUTION ---
if __name__ == "__main__":
    scanner = Scanner()
    
    # Change this path to a folder you want to test!
    # On Windows use double backslashes or r"path"
    target_dir = input("Enter directory path to scan: ").strip('"')
    
    if os.path.exists(target_dir):
        start_time = time.time()
        scanner.scan_directory(target_dir)
        print(f"Duration: {round(time.time() - start_time, 2)} seconds")
    else:
        print("Invalid directory path.")
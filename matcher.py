import sqlite3
import imagehash
import os
from difflib import SequenceMatcher

# --- CONFIGURATION ---
DB_NAME = "file_index.db"
SIMILARITY_THRESHOLD = 70.0 

class Matcher:
    def __init__(self, db_name=DB_NAME):
        if not os.path.exists(db_name):
            raise FileNotFoundError(f"Database {db_name} not found. Run scanner_engine.py first.")
        self.conn = sqlite3.connect(db_name)
        self.conn.row_factory = sqlite3.Row 

    def fetch_all_files(self):
        cursor = self.conn.execute("SELECT * FROM files")
        return [dict(row) for row in cursor.fetchall()]

    def find_exact_duplicates(self):
        query = """
        SELECT exact_hash, COUNT(*) as count 
        FROM files 
        WHERE exact_hash IS NOT NULL 
        GROUP BY exact_hash 
        HAVING count > 1
        """
        cursor = self.conn.execute(query)
        exact_groups = []
        for row in cursor.fetchall():
            file_hash = row['exact_hash']
            files_cursor = self.conn.execute("SELECT path, filename FROM files WHERE exact_hash = ?", (file_hash,))
            exact_groups.append([dict(f) for f in files_cursor.fetchall()])
        return exact_groups

    def calculate_score(self, file_a, file_b):
        score = 0
        total_weight = 0
        
        # 1. VISUAL SIMILARITY (Weight: 50)
        if file_a['visual_hash'] and file_b['visual_hash']:
            try:
                h1 = imagehash.hex_to_hash(file_a['visual_hash'])
                h2 = imagehash.hex_to_hash(file_b['visual_hash'])
                dist = h1 - h2
                vis_score = max(0, (10 - dist) / 10) * 100
                score += vis_score * 0.50
                total_weight += 0.50
            except: pass

        # 2. AUDIO SIMILARITY (Weight: 50) - NEW LOGIC
        if file_a['audio_hash'] and file_b['audio_hash']:
            # If MD5 of sonic fingerprints match, it's a 100% sonic match
            if file_a['audio_hash'] == file_b['audio_hash']:
                score += 100 * 0.50
            else:
                # If they don't match exactly, they are sonically different 
                # based on our current 1-decimal rounding logic.
                score += 0 * 0.50
            total_weight += 0.50

        # 3. FILENAME SIMILARITY (Weight: 20)
        name_sim = SequenceMatcher(None, file_a['filename'], file_b['filename']).ratio()
        score += (name_sim * 100) * 0.20
        total_weight += 0.20

        # 4. SIZE SIMILARITY (Weight: 10)
        size_a, size_b = file_a['size'], file_b['size']
        if size_a > 0 and size_b > 0:
            size_sim = (1 - (abs(size_a - size_b) / max(size_a, size_b))) * 100
            score += size_sim * 0.10
            total_weight += 0.10

        if total_weight == 0: return 0
        return round(score / total_weight, 1)

    def find_fuzzy_matches(self):
        files = self.fetch_all_files()
        potential_matches = []
        total = len(files)
        
        for i in range(total):
            for j in range(i + 1, total):
                f1, f2 = files[i], files[j]
                
                # Grouping Logic: Only compare relevant types
                is_media_1 = f1['extension'] in (['.mp3','.wav','.flac','.m4a'] + ['.jpg','.png','.mp4'])
                is_media_2 = f2['extension'] in (['.mp3','.wav','.flac','.m4a'] + ['.jpg','.png','.mp4'])
                
                # Skip comparing a text file to an audio file unless names match
                if f1['extension'] != f2['extension'] and f1['filename'] != f2['filename']:
                    continue

                score = self.calculate_score(f1, f2)
                if score >= SIMILARITY_THRESHOLD:
                    potential_matches.append({
                        'file_a': f1['path'],
                        'file_b': f2['path'],
                        'score': score
                    })
        return potential_matches

if __name__ == "__main__":
    # Test block remains the same
    m = Matcher()
    print(f"Fuzzy matches found: {len(m.find_fuzzy_matches())}")
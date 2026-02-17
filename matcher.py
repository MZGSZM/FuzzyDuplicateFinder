import sqlite3
import imagehash
import os
from difflib import SequenceMatcher

# --- CONFIGURATION ---
SIMILARITY_THRESHOLD = 70.0 

# Must match Scanner Engine extensions
VISUAL_EXTS = {'.jpg', '.jpeg', '.png', '.bmp', '.gif', '.webp', '.tiff', '.tif', '.psd', '.raw',
               '.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv', '.m4v', '.webm', '.ts', '.mts', '.3gp'}
AUDIO_EXTS = {'.mp3', '.wav', '.flac', '.m4a', '.aac', '.ogg', '.wma'}

class Matcher:
    def __init__(self, db_path):
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"Database not found at {db_path}")
        
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row 

    def close(self):
        try: self.conn.close()
        except: pass

    def fetch_all_files(self):
        cursor = self.conn.execute("SELECT * FROM files")
        # Ensure we only process files that exist on disk
        valid_files = []
        for row in cursor.fetchall():
            d = dict(row)
            if os.path.exists(d['path']):
                valid_files.append(d)
        return valid_files

    def find_exact_duplicates(self):
        all_files = self.fetch_all_files()
        hash_map = {}
        for f in all_files:
            if f['exact_hash']:
                if f['exact_hash'] not in hash_map:
                    hash_map[f['exact_hash']] = []
                hash_map[f['exact_hash']].append(f)
        
        exact_groups = []
        for k, group in hash_map.items():
            if len(group) > 1:
                exact_groups.append(group)
        return exact_groups

    def calculate_score(self, f1, f2):
        # Safety Check: If both are visual, we MUST share visual hashes.
        is_visual = f1['extension'] in VISUAL_EXTS
        has_visual_hashes = f1['visual_hash'] and f2['visual_hash']
        
        # New Feature retained: Fail immediately if visual hashes are missing for visual files
        if is_visual and not has_visual_hashes:
            return 0 

        score = 0
        total_weight = 0

        # 1. Visual Hash (50% - Reverted to strict threshold)
        if has_visual_hashes:
            try:
                h1 = imagehash.hex_to_hash(f1['visual_hash'])
                h2 = imagehash.hex_to_hash(f2['visual_hash'])
                dist = h1 - h2
                
                # FIX: Reverted to old math. 
                # Old logic: distance > 10 is 0% match.
                # Broken logic was: (64 - dist) / 64, which gave 50% match for random images.
                sim = max(0, (10 - dist) / 10) * 100
                
                score += sim * 0.50
                total_weight += 0.50
            except: pass

        # 2. Audio Hash (50% - Reverted weight)
        if f1['audio_hash'] and f2['audio_hash']:
             if f1['audio_hash'] == f2['audio_hash']:
                 score += 100 * 0.50
             total_weight += 0.50

        # 3. Filename Similarity (20%)
        if f1['filename'] and f2['filename']:
            name_sim = SequenceMatcher(None, f1['filename'], f2['filename']).ratio() * 100
            score += name_sim * 0.20
            total_weight += 0.20
        
        # 4. Size Similarity (10%)
        size_a, size_b = f1['size'], f2['size']
        if size_a > 0 and size_b > 0:
            size_sim = (1 - (abs(size_a - size_b) / max(size_a, size_b))) * 100
            score += size_sim * 0.10
            total_weight += 0.10

        # 5. Extension (5% - Restored from old version)
        if f1['extension'] == f2['extension']:
            score += 100 * 0.05
            total_weight += 0.05

        if total_weight == 0: return 0
        return round(score / total_weight, 1)

    def find_fuzzy_matches(self, stop_signal=None, progress_callback=None):
        files = self.fetch_all_files()
        potential_matches = []
        n = len(files)
        
        total_comparisons = (n * (n - 1)) // 2
        current_comparison = 0
        
        for i in range(n):
            if stop_signal and stop_signal(): break
            
            if progress_callback and i % 10 == 0:
                progress_callback(current_comparison, total_comparisons)

            for j in range(i + 1, n):
                current_comparison += 1
                
                f1, f2 = files[i], files[j]
                
                # Basic Type Check
                is_aud_1 = f1['extension'] in AUDIO_EXTS
                is_aud_2 = f2['extension'] in AUDIO_EXTS
                is_vis_1 = f1['extension'] in VISUAL_EXTS
                is_vis_2 = f2['extension'] in VISUAL_EXTS

                if (is_aud_1 != is_aud_2) and (f1['filename'] != f2['filename']): continue
                if (is_vis_1 != is_vis_2) and (f1['filename'] != f2['filename']): continue

                score = self.calculate_score(f1, f2)
                if score >= SIMILARITY_THRESHOLD:
                    potential_matches.append({
                        'file_a': f1['path'],
                        'file_b': f2['path'],
                        'score': score
                    })
        
        self.close()
        return potential_matches
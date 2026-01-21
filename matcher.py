import sqlite3
import imagehash
import os
from difflib import SequenceMatcher

# --- CONFIGURATION ---
DB_NAME = "file_index.db"
SIMILARITY_THRESHOLD = 70.0  # Only show results with score > 70%

class Matcher:
    def __init__(self, db_name=DB_NAME):
        if not os.path.exists(db_name):
            raise FileNotFoundError(f"Database {db_name} not found. Run scanner_engine.py first.")
        self.conn = sqlite3.connect(db_name)
        self.conn.row_factory = sqlite3.Row # Allows accessing columns by name

    def fetch_all_files(self):
        """Get all file records into memory."""
        cursor = self.conn.execute("SELECT * FROM files")
        return [dict(row) for row in cursor.fetchall()]

    def find_exact_duplicates(self):
        """Fast SQL query to find files with identical binary content."""
        print("--- Searching for Exact Duplicates (MD5) ---")
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
            # Get the actual file paths for this hash
            files_cursor = self.conn.execute("SELECT path, filename FROM files WHERE exact_hash = ?", (file_hash,))
            files = [dict(f) for f in files_cursor.fetchall()]
            exact_groups.append(files)
            
        print(f"Found {len(exact_groups)} groups of exact duplicates.")
        return exact_groups

    def calculate_score(self, file_a, file_b):
        """The Master Scoring Algorithm (0-100)."""
        score = 0
        total_weight = 0
        
        # 1. VISUAL SIMILARITY (Weight: 50)
        # We only check this if both have visual hashes
        vis_score = 0
        if file_a['visual_hash'] and file_b['visual_hash']:
            try:
                # Convert hex string back to imagehash object
                h1 = imagehash.hex_to_hash(file_a['visual_hash'])
                h2 = imagehash.hex_to_hash(file_b['visual_hash'])
                dist = h1 - h2
                # Distance 0 = 100% match. Distance 10+ = 0% match.
                vis_score = max(0, (10 - dist) / 10) * 100
            except:
                vis_score = 0
            
            score += vis_score * 0.50
            total_weight += 0.50

        # 2. FILENAME SIMILARITY (Weight: 20)
        # SequenceMatcher ratio returns 0.0 to 1.0
        name_sim = SequenceMatcher(None, file_a['filename'], file_b['filename']).ratio()
        score += (name_sim * 100) * 0.20
        total_weight += 0.20

        # 3. SIZE SIMILARITY (Weight: 10)
        size_a, size_b = file_a['size'], file_b['size']
        if size_a > 0 and size_b > 0:
            diff = abs(size_a - size_b)
            max_size = max(size_a, size_b)
            size_sim = (1 - (diff / max_size)) * 100
        else:
            size_sim = 0
        
        score += size_sim * 0.10
        total_weight += 0.10

        # 4. EXTENSION MATCH (Weight: 5)
        # Small boost if they are the same file type
        if file_a['extension'] == file_b['extension']:
            score += 100 * 0.05
        total_weight += 0.05

        # Normalize score
        if total_weight == 0: return 0
        final_score = score / total_weight
        return round(final_score, 1)

    def find_fuzzy_matches(self):
        """
        Comparing every file against every other file is slow (O(n^2)).
        We optimize by only comparing files that share at least ONE attribute 
        (like extension or similar size) or just brute force it for small datasets.
        """
        print("--- Searching for Fuzzy Matches (Visual/Name) ---")
        files = self.fetch_all_files()
        potential_matches = []
        checked_pairs = set()

        # Iterate through files
        total = len(files)
        print(f"Analyzing {total} files against each other...")
        
        for i in range(total):
            for j in range(i + 1, total):
                f1 = files[i]
                f2 = files[j]
                
                # Optimization: Skip if different media types (e.g. don't compare .txt to .jpg)
                # You can disable this if you want to find a text file that has the same name as a jpg
                type1 = 'img' if f1['extension'] in ['.jpg','.png','.mp4'] else 'other'
                type2 = 'img' if f2['extension'] in ['.jpg','.png','.mp4'] else 'other'
                # Simple check to avoid checking text vs video
                if f1['extension'] != f2['extension'] and f1['filename'] != f2['filename']:
                    continue

                # Calculate Score
                score = self.calculate_score(f1, f2)
                
                if score >= SIMILARITY_THRESHOLD:
                    potential_matches.append({
                        'file_a': f1['path'],
                        'file_b': f2['path'],
                        'score': score,
                        'reason': f"Score: {score}%"
                    })

        print(f"Found {len(potential_matches)} fuzzy matches.")
        return potential_matches

# --- EXECUTION ---
if __name__ == "__main__":
    try:
        matcher = Matcher()
        
        # 1. Exact Matches
        exact = matcher.find_exact_duplicates()
        if exact:
            print("\n--- EXACT DUPLICATES FOUND ---")
            for group in exact:
                print(f"Match Group ({len(group)} files):")
                for f in group:
                    print(f"  - {f['path']}")
        else:
            print("\nNo exact duplicates found.")

        # 2. Fuzzy Matches
        fuzzy = matcher.find_fuzzy_matches()
        if fuzzy:
            print(f"\n--- FUZZY MATCHES (Threshold {SIMILARITY_THRESHOLD}%) ---")
            # Sort by highest score
            fuzzy.sort(key=lambda x: x['score'], reverse=True)
            
            for match in fuzzy:
                print(f"Match [{match['score']}%]:")
                print(f"  A: {match['file_a']}")
                print(f"  B: {match['file_b']}")
                print("-" * 20)
        else:
            print("\nNo fuzzy matches found.")
            
    except Exception as e:
        print(f"Error: {e}")
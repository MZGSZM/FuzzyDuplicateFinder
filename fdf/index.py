"""
The sqlite index.

One row per file. NULL in a hash column means "not computed"; an empty string
means "attempted and failed", so an unchanged broken file is not re-decoded on
every scan.

Older indexes (1.5.x and earlier) are upgraded in place by adding the missing
columns. Their rows have fp_version NULL, so the next scan recomputes fuzzy
fingerprints for them while keeping their exact hashes.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from datetime import datetime

log = logging.getLogger(__name__)

COLUMNS = (
    ('path', 'TEXT UNIQUE'),
    ('filename', 'TEXT'),
    ('extension', 'TEXT'),
    ('size', 'INTEGER'),
    ('mtime', 'REAL'),
    ('ctime', 'REAL'),
    ('exact_hash', 'TEXT'),
    ('visual_hash', 'TEXT'),
    ('audio_hash', 'TEXT'),
    ('scan_date', 'TEXT'),
    ('quick_hash', 'TEXT'),
    ('text_hash', 'TEXT'),
    ('duration', 'REAL'),
    ('dev', 'INTEGER'),
    ('inode', 'INTEGER'),
    ('fp_version', 'INTEGER'),
)
COLUMN_NAMES = tuple(name for name, _ in COLUMNS)

# Positions in the tuples returned by load_existing().
P_SIZE, P_MTIME, P_EXACT, P_QUICK, P_VISUAL, P_AUDIO, P_TEXT, P_DURATION, P_DEV, P_INODE, \
    P_VERSION = range(11)

_EXISTING_SELECT = (
    "SELECT path, size, mtime, exact_hash, quick_hash, visual_hash, audio_hash, "
    "text_hash, duration, dev, inode, fp_version FROM files"
)


class Index:
    """
    Single-threaded sqlite wrapper with buffered writes. The scanner only
    touches it from its coordinating thread, so there is no lock.
    """

    FLUSH_ROWS = 500

    def __init__(self, db_path):
        self.db_path = db_path
        parent = os.path.dirname(os.path.abspath(db_path))
        os.makedirs(parent, exist_ok=True)
        self.conn = sqlite3.connect(db_path, timeout=30)
        self.conn.execute("PRAGMA journal_mode=WAL")
        # A crash mid-scan costs a rescan, not user data.
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA temp_store=MEMORY")
        self.conn.execute("PRAGMA cache_size=-65536")
        self._pending = []
        self._migrate()

    # -- schema -------------------------------------------------------------

    def _migrate(self):
        cols = ",\n".join(f"{name} {kind}" for name, kind in COLUMNS)
        self.conn.execute(
            f"CREATE TABLE IF NOT EXISTS files (id INTEGER PRIMARY KEY AUTOINCREMENT,\n{cols})"
        )
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS scan_roots (path TEXT PRIMARY KEY, priority INTEGER)"
        )
        have = {row[1] for row in self.conn.execute("PRAGMA table_info(files)")}
        for name, kind in COLUMNS:
            if name not in have:
                # UNIQUE cannot be added by ALTER; path has always existed.
                plain = kind.replace(' UNIQUE', '')
                self.conn.execute(f"ALTER TABLE files ADD COLUMN {name} {plain}")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_files_size ON files(size)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_files_exact ON files(exact_hash)")
        self.conn.commit()

    # -- scan roots ---------------------------------------------------------

    def save_roots(self, folder_list):
        rows = []
        for item in folder_list:
            if isinstance(item, dict):
                rows.append((item['path'], int(item.get('priority', 10))))
            else:
                rows.append((item, 10))
        with self.conn:
            self.conn.execute("DELETE FROM scan_roots")
            self.conn.executemany(
                "INSERT OR REPLACE INTO scan_roots (path, priority) VALUES (?, ?)", rows
            )

    def get_roots(self):
        cursor = self.conn.execute("SELECT path, priority FROM scan_roots ORDER BY path")
        return [{'path': row[0], 'priority': row[1]} for row in cursor]

    # -- reads --------------------------------------------------------------

    def load_existing(self):
        """{path: (size, mtime, exact, quick, visual, audio, text, duration,
        dev, inode, fp_version)}. One read replaces a query per file."""
        return {row[0]: row[1:] for row in self.conn.execute(_EXISTING_SELECT)}

    # -- writes -------------------------------------------------------------

    def queue_row(self, values):
        """values: dict keyed by COLUMN_NAMES (scan_date is filled in)."""
        values.setdefault('scan_date', datetime.now().isoformat(timespec='seconds'))
        self._pending.append(tuple(values.get(name) for name in COLUMN_NAMES))
        if len(self._pending) >= self.FLUSH_ROWS:
            self.flush()

    def flush(self):
        if not self._pending:
            return
        rows, self._pending = self._pending, []
        placeholders = ", ".join("?" for _ in COLUMN_NAMES)
        updates = ", ".join(f"{n}=excluded.{n}" for n in COLUMN_NAMES if n != 'path')
        with self.conn:
            self.conn.executemany(
                f"INSERT INTO files ({', '.join(COLUMN_NAMES)}) VALUES ({placeholders}) "
                f"ON CONFLICT(path) DO UPDATE SET {updates}",
                rows,
            )

    def set_exact_hashes(self, pairs):
        """pairs: iterable of (exact_hash, path)."""
        self.flush()
        with self.conn:
            self.conn.executemany("UPDATE files SET exact_hash=? WHERE path=?", list(pairs))

    def delete_paths(self, paths):
        paths = [(p,) for p in paths]
        if not paths:
            return 0
        self.flush()
        with self.conn:
            self.conn.executemany("DELETE FROM files WHERE path=?", paths)
        return len(paths)

    def close(self):
        try:
            self.flush()
        except Exception:
            log.exception("Final index flush failed")
        try:
            self.conn.close()
        except Exception:
            pass

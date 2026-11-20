"""
Per-user locations, computed without Qt so the CLI and tests can use them.

    Linux    $XDG_DATA_HOME/FuzzyDuplicateFinder   (~/.local/share/...)
             $XDG_CACHE_HOME/FuzzyDuplicateFinder  (~/.cache/...)
    macOS    ~/Library/Application Support/FuzzyDuplicateFinder
             ~/Library/Caches/FuzzyDuplicateFinder
    Windows  %LOCALAPPDATA%\\FuzzyDuplicateFinder   (data and cache)

FDF_DATA_DIR overrides the data location, which the tests rely on.
"""

from __future__ import annotations

import hashlib
import os
import sys

APP_DIR = "FuzzyDuplicateFinder"


def _ensure(path):
    os.makedirs(path, exist_ok=True)
    return path


def data_dir():
    override = os.environ.get("FDF_DATA_DIR")
    if override:
        return _ensure(override)
    if sys.platform == "win32":
        base = os.environ.get("LOCALAPPDATA") or os.path.expanduser(r"~\AppData\Local")
    elif sys.platform == "darwin":
        base = os.path.expanduser("~/Library/Application Support")
    else:
        base = os.environ.get("XDG_DATA_HOME") or os.path.expanduser("~/.local/share")
    return _ensure(os.path.join(base, APP_DIR))


def cache_dir():
    override = os.environ.get("FDF_DATA_DIR")
    if override:
        return _ensure(os.path.join(override, "cache"))
    if sys.platform == "win32":
        return _ensure(os.path.join(data_dir(), "cache"))
    if sys.platform == "darwin":
        base = os.path.expanduser("~/Library/Caches")
    else:
        base = os.environ.get("XDG_CACHE_HOME") or os.path.expanduser("~/.cache")
    return _ensure(os.path.join(base, APP_DIR))


def index_dir():
    return _ensure(os.path.join(data_dir(), "indexes"))


def log_dir():
    return _ensure(os.path.join(data_dir(), "logs"))


def normalize_root(path):
    return os.path.normcase(os.path.abspath(os.path.normpath(path)))


def default_index_path(roots):
    """
    One index per set of scan folders, so rescanning the same folders is
    incremental without the user having to manage database files.
    """
    key = "\n".join(sorted({normalize_root(r) for r in roots}))
    digest = hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]
    return os.path.join(index_dir(), f"index-{digest}.db")


def index_files(db_path):
    """The database plus the sidecar files sqlite's WAL mode creates."""
    return [db_path, db_path + "-wal", db_path + "-shm"]

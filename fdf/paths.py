"""
Per-user locations, computed without Qt so the CLI and tests can use them.

    Linux    $XDG_DATA_HOME/FuzzyDuplicateFinder   (~/.local/share/...)
             $XDG_CACHE_HOME/FuzzyDuplicateFinder  (~/.cache/...)
    macOS    ~/Library/Application Support/FuzzyDuplicateFinder
             ~/Library/Caches/FuzzyDuplicateFinder
    Windows  %LOCALAPPDATA%\\FuzzyDuplicateFinder   (data and cache)

FDF_DATA_DIR overrides the data location, which the tests rely on.

Indexes can also live inside a scanned folder or in a folder the user picked;
resolve_index() decides, shared by the GUI and the CLI.
"""

from __future__ import annotations

import hashlib
import os
import sys
import tempfile
from dataclasses import dataclass

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


# -----------------------------------------------------------------------------
# Index location
# -----------------------------------------------------------------------------

# appdata  the app data indexes folder (default, the 1.6.0 behaviour)
# folder   inside a scanned folder, so the paths stay on the disk they describe
# custom   one folder the user picked
# ask      a save dialog (GUI) or prompt (CLI) once per folder set per session
INDEX_MODES = ("appdata", "folder", "custom", "ask")
DEFAULT_INDEX_MODE = "appdata"

FOLDER_INDEX_PREFIX = ".fdf-index-"
CUSTOM_INDEX_PREFIX = "fdf-index-"
SCAN_LOG_DIRS = ("fdf-logs", ".fdf-logs")

# The scanner never indexes these, whatever the exclude list says: another
# folder set's index or a scan log would otherwise show up as a "duplicate".
INTERNAL_FILE_PATTERN = "*fdf-index-*.db*"


def folder_set_key(roots):
    """Stable id for a set of scan folders: order and spelling do not matter."""
    key = "\n".join(sorted({normalize_root(r) for r in roots}))
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]


def default_index_path(roots):
    """
    One index per set of scan folders, so rescanning the same folders is
    incremental without the user having to manage database files.
    """
    return os.path.join(index_dir(), f"index-{folder_set_key(roots)}.db")


def folder_index_path(holder, roots):
    return os.path.join(os.path.abspath(holder), f"{FOLDER_INDEX_PREFIX}{folder_set_key(roots)}.db")


def custom_index_path(directory, roots):
    return os.path.join(os.path.abspath(directory),
                        f"{CUSTOM_INDEX_PREFIX}{folder_set_key(roots)}.db")


def find_folder_index(roots):
    """An in-folder index for this set that already exists in any of its
    folders. The holder does not have to be the first folder: on a set that
    spans disks the user picked it."""
    for root in roots:
        path = folder_index_path(root, roots)
        if os.path.exists(path):
            return path
    return None


def spans_volumes(roots):
    """True when the folders are on more than one disk. st_dev is the volume
    serial number on Windows and the device id elsewhere. A folder that cannot
    be read counts as its own disk, which errs toward asking."""
    devices = set()
    for root in roots:
        try:
            devices.add(os.stat(root).st_dev)
        except OSError:
            devices.add(("unreadable", normalize_root(root)))
    return len(devices) > 1


def dir_writable(path):
    """Actually create a file: os.access() is wrong for ACLs and read-only
    network shares on Windows."""
    if not path or not os.path.isdir(path):
        return False
    try:
        handle, probe = tempfile.mkstemp(prefix=".fdf-write-test-", dir=path)
    except OSError:
        return False
    os.close(handle)
    try:
        os.remove(probe)
    except OSError:
        pass
    return True


@dataclass
class IndexChoice:
    """
    Where a folder set's index goes. `problem` is None when `path` is ready to
    use, otherwise what has to be asked first:

        ask         the mode is "ask each scan"; `path` is the suggestion
        holder      the folders span disks; pick which one holds the index
        unwritable  `folder` cannot be written; pick another location
    """
    path: str | None
    problem: str | None = None
    folder: str | None = None


def resolve_index(roots, mode, custom_dir=None, check_writable=True):
    roots = [os.path.abspath(r) for r in roots]
    if mode == "ask":
        return IndexChoice(default_index_path(roots), "ask")
    if mode == "custom":
        if not custom_dir:
            return IndexChoice(None, "unwritable", None)
        path = custom_index_path(custom_dir, roots)
        if check_writable and not dir_writable(custom_dir):
            return IndexChoice(path, "unwritable", os.path.abspath(custom_dir))
        return IndexChoice(path)
    if mode == "folder":
        path = find_folder_index(roots)
        if path is None:
            if len(roots) > 1 and spans_volumes(roots):
                return IndexChoice(None, "holder")
            path = folder_index_path(roots[0], roots)
        folder = os.path.dirname(path)
        if check_writable and not dir_writable(folder):
            return IndexChoice(path, "unwritable", folder)
        return IndexChoice(path)
    return IndexChoice(default_index_path(roots))


def existing_index(roots, mode, custom_dir=None):
    """The index for this set if one exists where the mode would put it.
    Cheap enough to call on every UI update: no writes, no prompts."""
    if not roots or mode == "ask":
        return None
    if mode == "folder":
        return find_folder_index(roots)
    if mode == "custom":
        path = custom_index_path(custom_dir, roots) if custom_dir else None
    else:
        path = default_index_path(roots)
    return path if path and os.path.exists(path) else None


def scan_log_path(db_path):
    """
    Where a scan of this index logs. None for indexes in the app data folder,
    which log to the app log as before. Anywhere else the log sits beside the
    index, so file paths never land on a different disk from the files.
    """
    folder = os.path.dirname(os.path.abspath(db_path))
    if normalize_root(folder) == normalize_root(index_dir()):
        return None
    name = os.path.basename(db_path)
    stem = os.path.splitext(name)[0].lstrip(".")
    if stem.startswith(CUSTOM_INDEX_PREFIX):
        stem = stem[len(CUSTOM_INDEX_PREFIX):]
    logs = SCAN_LOG_DIRS[1] if name.startswith(".") else SCAN_LOG_DIRS[0]
    return os.path.join(folder, logs, f"scan-{stem}.log")


def index_files(db_path):
    """The database plus the sidecar files sqlite's WAL mode creates."""
    return [db_path, db_path + "-wal", db_path + "-shm"]

"""
Logging setup shared by the GUI and the CLI.

The app log lives in the app data folder. A scan whose index lives anywhere
else (inside a scanned folder, a custom folder, a file picked by hand) logs to
a file beside that index instead, so file paths from an encrypted disk never
end up in a log on another disk. Nothing is redacted: the scan log is a normal,
complete log, it is just stored with the data it describes.
"""

from __future__ import annotations

import logging
import logging.handlers
import os
from contextlib import contextmanager

from .paths import log_dir, scan_log_path

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
APP_LOG_NAME = "fuzzy-duplicate-finder.log"
MAX_BYTES = 1_000_000
BACKUPS = 2

# Every logger that can write a file path during a scan or re-match. Child
# loggers follow their parent.
SCAN_LOGGERS = (
    "fdf.scan",
    "fdf.scanner",
    "fdf.watchdog",
    "fdf.matcher",
    "fdf.index",
    "fdf.pipeline",
    "fdf.fingerprints",
)


def _file_handler(path):
    handler = logging.handlers.RotatingFileHandler(
        path, maxBytes=MAX_BYTES, backupCount=BACKUPS, encoding="utf-8",
    )
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    return handler


def setup_app_log():
    """Rotating app log in the app data folder. Returns the handler, or None
    when the folder cannot be written."""
    try:
        handler = _file_handler(os.path.join(log_dir(), APP_LOG_NAME))
    except OSError:
        return None
    root = logging.getLogger()
    root.addHandler(handler)
    root.setLevel(logging.INFO)
    return handler


@contextmanager
def scan_log(db_path, extra_handlers=()):
    """
    Route the scan loggers to the scan log for this index while the block runs.

    Yields the scan log's path, or None when the index is in the app data
    folder and the app log keeps everything as before. `extra_handlers` also
    receive scan messages while routed (the CLI passes its stderr handler).
    If the scan log cannot be opened the messages are dropped rather than sent
    to the app log, since that is the leak this exists to prevent.
    """
    path = scan_log_path(db_path) if db_path else None
    if path is None:
        yield None
        return

    handler = None
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        handler = _file_handler(path)
    except OSError:
        handler = None

    handlers = [h for h in (handler, *extra_handlers) if h is not None]
    saved = []
    for name in SCAN_LOGGERS:
        logger = logging.getLogger(name)
        saved.append((logger, logger.propagate))
        logger.propagate = False
        for h in handlers:
            logger.addHandler(h)
    try:
        yield path if handler is not None else None
    finally:
        for logger, propagate in saved:
            for h in handlers:
                logger.removeHandler(h)
            logger.propagate = propagate
        if handler is not None:
            handler.close()


def scan_log_folder(db_path):
    """Folder of the scan log for this index, if it has one on disk."""
    path = scan_log_path(db_path) if db_path else None
    if path and os.path.isdir(os.path.dirname(path)):
        return os.path.dirname(path)
    return None

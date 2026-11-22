"""
Typed access to the persisted settings. The GUI writes them; the CLI reads the
index location from here too, so both put an index in the same place.

The scan folder list is deliberately not stored: settings live on the system
disk, and folder names from an encrypted disk should not stay behind there.
"""

from __future__ import annotations

import os

from PyQt6.QtCore import QSettings

from .exts import KINDS
from .matcher import DEFAULT_THRESHOLD
from .paths import DEFAULT_INDEX_MODE, INDEX_MODES
from .scanner import DEFAULT_EXCLUDES, ScanOptions

ORG = "FuzzyDuplicateFinder"

# 1.6.0 kept the folder list here. Removed on first start of 1.7.0.
LEGACY_FOLDERS_KEY = "session/folders"


def _bool(value, default):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).lower() in ("1", "true", "yes")


def default_qsettings():
    """The user's settings. FDF_DATA_DIR (used by the tests) points them at an
    ini file inside that folder instead."""
    override = os.environ.get("FDF_DATA_DIR")
    if override:
        os.makedirs(override, exist_ok=True)
        return QSettings(os.path.join(override, "settings.ini"), QSettings.Format.IniFormat)
    return QSettings(ORG, ORG)


class AppSettings:
    def __init__(self, qsettings=None):
        self.q = qsettings or default_qsettings()

    # -- scan options -------------------------------------------------------

    @property
    def kinds(self):
        raw = self.q.value("scan/kinds")
        if not raw:
            return frozenset(KINDS)
        return frozenset(k for k in str(raw).split(",") if k in KINDS)

    @kinds.setter
    def kinds(self, value):
        self.q.setValue("scan/kinds", ",".join(k for k in KINDS if k in value) or "-")

    def _get_bool(self, key, default):
        return _bool(self.q.value(key), default)

    all_types = property(lambda s: s._get_bool("scan/all_types", False),
                         lambda s, v: s.q.setValue("scan/all_types", bool(v)))
    skip_hidden = property(lambda s: s._get_bool("scan/skip_hidden", True),
                           lambda s, v: s.q.setValue("scan/skip_hidden", bool(v)))
    low_priority = property(lambda s: s._get_bool("perf/low_priority", True),
                            lambda s, v: s.q.setValue("perf/low_priority", bool(v)))
    watchdog = property(lambda s: s._get_bool("perf/watchdog", True),
                        lambda s, v: s.q.setValue("perf/watchdog", bool(v)))
    confirm_delete = property(lambda s: s._get_bool("review/confirm_delete", True),
                              lambda s, v: s.q.setValue("review/confirm_delete", bool(v)))

    @property
    def excludes(self):
        raw = self.q.value("scan/excludes")
        if raw is None:
            return DEFAULT_EXCLUDES
        return tuple(p.strip() for p in str(raw).split(";") if p.strip())

    @excludes.setter
    def excludes(self, value):
        self.q.setValue("scan/excludes", "; ".join(value))

    @property
    def min_size_kb(self):
        try:
            return max(0, int(self.q.value("scan/min_size_kb", 0)))
        except (TypeError, ValueError):
            return 0

    @min_size_kb.setter
    def min_size_kb(self, value):
        self.q.setValue("scan/min_size_kb", int(value))

    @property
    def stall_seconds(self):
        try:
            return max(10, int(self.q.value("perf/stall_seconds", 120)))
        except (TypeError, ValueError):
            return 120

    @stall_seconds.setter
    def stall_seconds(self, value):
        self.q.setValue("perf/stall_seconds", int(value))

    @property
    def threshold(self):
        try:
            return float(self.q.value("match/threshold", DEFAULT_THRESHOLD))
        except (TypeError, ValueError):
            return DEFAULT_THRESHOLD

    @threshold.setter
    def threshold(self, value):
        self.q.setValue("match/threshold", float(value))

    @property
    def threads(self):
        try:
            value = int(self.q.value("perf/threads", 0))
        except (TypeError, ValueError):
            value = 0
        return value or (os.cpu_count() or 4)

    @threads.setter
    def threads(self, value):
        self.q.setValue("perf/threads", int(value))

    def scan_options(self, threads=None):
        return ScanOptions(
            kinds=self.kinds,
            all_types=self.all_types,
            skip_hidden=self.skip_hidden,
            excludes=self.excludes,
            # Empty files are always "identical", so never go below 1 byte.
            min_size=max(1, self.min_size_kb * 1024),
            max_workers=threads,
        )

    # -- index location -----------------------------------------------------

    @property
    def index_mode(self):
        value = str(self.q.value("index/mode", DEFAULT_INDEX_MODE))
        return value if value in INDEX_MODES else DEFAULT_INDEX_MODE

    @index_mode.setter
    def index_mode(self, value):
        self.q.setValue("index/mode", value if value in INDEX_MODES else DEFAULT_INDEX_MODE)

    @property
    def index_custom_dir(self):
        value = self.q.value("index/custom_dir", "")
        return str(value) if value else ""

    @index_custom_dir.setter
    def index_custom_dir(self, value):
        self.q.setValue("index/custom_dir", value or "")

    def forget_legacy_folders(self):
        """Drop the folder list 1.6.0 saved. Returns True if there was one."""
        if self.q.contains(LEGACY_FOLDERS_KEY):
            self.q.remove(LEGACY_FOLDERS_KEY)
            self.q.sync()
            return True
        return False

    # -- window state -------------------------------------------------------

    def value(self, key, default=None):
        return self.q.value(key, default)

    def set_value(self, key, value):
        self.q.setValue(key, value)

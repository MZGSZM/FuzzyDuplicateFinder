"""Typed access to the persisted GUI settings."""

from __future__ import annotations

import json
import os

from PyQt6.QtCore import QSettings

from .exts import KINDS
from .matcher import DEFAULT_THRESHOLD
from .scanner import DEFAULT_EXCLUDES, ScanOptions

ORG = "FuzzyDuplicateFinder"


def _bool(value, default):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).lower() in ("1", "true", "yes")


class AppSettings:
    def __init__(self, qsettings=None):
        self.q = qsettings or QSettings(ORG, ORG)

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

    # -- folders ------------------------------------------------------------

    @property
    def folders(self):
        raw = self.q.value("session/folders")
        if not raw:
            return []
        try:
            data = json.loads(raw)
        except (TypeError, ValueError):
            return []
        out = []
        for item in data:
            if isinstance(item, dict) and isinstance(item.get('path'), str):
                out.append({'path': item['path'], 'priority': int(item.get('priority', 10))})
        return out

    @folders.setter
    def folders(self, value):
        self.q.setValue("session/folders", json.dumps(value))

    # -- window state -------------------------------------------------------

    def value(self, key, default=None):
        return self.q.value(key, default)

    def set_value(self, key, value):
        self.q.setValue(key, value)

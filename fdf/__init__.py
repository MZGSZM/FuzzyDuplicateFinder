"""Fuzzy Duplicate Finder."""

from ._version import VERSION

APP_NAME = "Fuzzy Duplicate Finder"
APP_ID = "fuzzy-duplicate-finder"
PROJECT_URL = "https://github.com/MZGSZM/FuzzyDuplicateFinder"

# Windows taskbar identity. NEVER CHANGE THIS after a release: pinned taskbar
# shortcuts are tied to it, and a new value splits them from the running app.
WINDOWS_APP_USER_MODEL_ID = "MZGSZM.FuzzyDuplicateFinder"

__all__ = ["APP_ID", "APP_NAME", "PROJECT_URL", "VERSION", "WINDOWS_APP_USER_MODEL_ID"]

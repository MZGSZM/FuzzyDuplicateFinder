"""
Values for the Windows version resource, kept out of the spec file so they can
be tested without PyInstaller. Only the build imports this module.
"""

from __future__ import annotations

import re

from . import APP_NAME, PROJECT_URL, VERSION

EXE_NAME = "FuzzyDuplicateFinder.exe"


def numeric_version(version=VERSION):
    """
    Windows wants four 16-bit integers. "1.7.0" -> (1, 7, 0, 0); anything after
    the numbers ("1.7.0-rc1", "v1.7") is dropped. The string fields keep the
    full text.
    """
    numbers = re.match(r"v?(\d+(?:\.\d+)*)", version.strip())
    parts = [int(p) for p in numbers.group(1).split(".")] if numbers else []
    parts = [min(p, 0xFFFF) for p in parts[:4]]
    return tuple(parts + [0] * (4 - len(parts)))


def version_strings(version=VERSION):
    """The StringFileInfo block Explorer shows under Properties > Details."""
    return {
        "CompanyName": "MZGSZM",
        "FileDescription": APP_NAME,
        "FileVersion": version,
        "InternalName": "FuzzyDuplicateFinder",
        "LegalCopyright": "Licensed under the GNU GPL v3",
        "OriginalFilename": EXE_NAME,
        "ProductName": APP_NAME,
        "ProductVersion": version,
        "Comments": PROJECT_URL,
    }

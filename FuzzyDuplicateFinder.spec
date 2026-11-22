# -*- mode: python ; coding: utf-8 -*-
#
# PyInstaller build for all three platforms. Run from the repo root:
#
#     pyinstaller --noconfirm FuzzyDuplicateFinder.spec
#
#   Linux    dist/FuzzyDuplicateFinder/          one folder; CI wraps it in an AppImage
#   macOS    dist/FuzzyDuplicateFinder.app       one-folder bundle (onefile .app is
#                                                deprecated in PyInstaller 6)
#   Windows  dist/FuzzyDuplicateFinder.exe       single file
#
# A one-folder build starts much faster than a one-file build, which has to
# unpack Qt, OpenCV and SciPy to a temp directory on every launch. Windows
# keeps the single EXE because that is the expected download there.

import sys

from PyInstaller.building.api import COLLECT, EXE, PYZ
from PyInstaller.building.build_main import Analysis

is_macos = sys.platform == 'darwin'
is_windows = sys.platform == 'win32'
is_linux = sys.platform.startswith('linux')

if is_macos:
    from PyInstaller.building.osx import BUNDLE

datas = [
    ('assets/icons', 'assets/icons'),
    ('assets/icon.svg', 'assets'),
]

if is_windows:
    exe_icon = 'assets/icon.ico'
elif is_macos:
    exe_icon = 'assets/icon.icns'
else:
    exe_icon = None     # the AppImage and desktop entry provide it

# QtDBus is used for desktop theme detection on Linux only and does not exist
# in the Windows and macOS wheels. librosa is imported lazily, so name it.
hiddenimports = ['librosa', 'librosa.feature', 'soxr', 'soundfile']
if is_linux:
    hiddenimports.append('PyQt6.QtDBus')

# Large packages that end up in the analysis through optional imports and are
# never used by the app.
excludes = ['tkinter', 'matplotlib', 'IPython', 'pytest', 'PyQt6.QtWebEngineCore',
            'PyQt6.QtQml', 'PyQt6.QtQuick', 'PyQt6.Qt3DCore', 'PyQt6.QtMultimedia']

a = Analysis(
    ['main.py'],
    pathex=[],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=excludes,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data)


def windows_version_info():
    """
    Version resource for Explorer's Properties > Details tab, built from
    fdf/_version.py. The release workflow writes the tag there before building,
    so a release exe always carries its own tag.
    """
    from PyInstaller.utils.win32.versioninfo import (
        FixedFileInfo,
        StringFileInfo,
        StringStruct,
        StringTable,
        VarFileInfo,
        VarStruct,
        VSVersionInfo,
    )

    sys.path.insert(0, SPECPATH)  # noqa: F821  (defined by PyInstaller)
    from fdf.buildinfo import numeric_version, version_strings

    numbers = numeric_version()
    return VSVersionInfo(
        ffi=FixedFileInfo(filevers=numbers, prodvers=numbers),
        kids=[
            # 0409 = US English, 04B0 = Unicode.
            StringFileInfo([StringTable('040904B0', [
                StringStruct(key, value) for key, value in version_strings().items()
            ])]),
            VarFileInfo([VarStruct('Translation', [0x0409, 1200])]),
        ],
    )


if is_windows:
    exe = EXE(
        pyz,
        a.scripts,
        a.binaries,
        a.datas,
        [],
        name='FuzzyDuplicateFinder',
        debug=False,
        bootloader_ignore_signals=False,
        strip=False,
        # UPX saves little on top of PyInstaller's compression and is a common
        # antivirus false-positive trigger.
        upx=False,
        runtime_tmpdir=None,
        console=False,
        disable_windowed_traceback=False,
        icon=exe_icon,
        version=windows_version_info(),
    )
else:
    exe = EXE(
        pyz,
        a.scripts,
        [],
        exclude_binaries=True,
        name='FuzzyDuplicateFinder',
        debug=False,
        bootloader_ignore_signals=False,
        strip=False,
        upx=False,
        # Console stays attached so the CLI (`FuzzyDuplicateFinder scan ...`)
        # prints. On macOS the .app launcher hides it anyway.
        console=not is_macos,
        disable_windowed_traceback=False,
        target_arch=None,
        codesign_identity=None,
        entitlements_file=None,
        icon=exe_icon,
    )
    coll = COLLECT(
        exe,
        a.binaries,
        a.datas,
        strip=False,
        upx=False,
        name='FuzzyDuplicateFinder',
    )
    if is_macos:
        app = BUNDLE(
            coll,
            name='FuzzyDuplicateFinder.app',
            icon='assets/icon.icns',
            bundle_identifier='com.fuzzyduplicate.finder',
            info_plist={
                'NSPrincipalClass': 'NSApplication',
                'NSHighResolutionCapable': 'True',
            },
            codesign_identity=None,
        )

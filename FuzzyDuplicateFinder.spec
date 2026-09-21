# -*- mode: python ; coding: utf-8 -*-
import sys
from PyInstaller.building.build_main import Analysis
from PyInstaller.building.api import EXE, PYZ

# Platform detection
is_macos = sys.platform == 'darwin'
is_windows = sys.platform == 'win32'
is_linux = sys.platform.startswith('linux')

# Only import BUNDLE on macOS
if is_macos:
    from PyInstaller.building.osx import BUNDLE

# Runtime icon set loaded by main.load_app_icon(). The SVG is included as the
# fallback source. Paths are relative to the repo root, which is where CI runs
# pyinstaller from.
icon_datas = [
    ('assets/icons', 'assets/icons'),
    ('assets/icon.svg', 'assets'),
]

if is_windows:
    exe_icon = 'assets/icon.ico'
elif is_macos:
    exe_icon = 'assets/icon.icns'
else:
    # Linux executables carry no embedded icon; the desktop entry provides it.
    exe_icon = None

# QtDBus is used for desktop theme detection on Linux only and does not exist
# in the Windows and macOS wheels.
platform_hiddenimports = ['PyQt6.QtDBus'] if is_linux else []

a = Analysis(
    ['main.py'],
    pathex=[],
    binaries=[],
    datas=icon_datas,
    hiddenimports=[
        'librosa',
        'librosa.core',
        'librosa.feature',
        'numpy',
        'cv2',
        'imagehash',
        'PIL',
        'PyQt6',
        'PyQt6.QtCore',
        'PyQt6.QtGui',
        'PyQt6.QtWidgets',
        'send2trash',
        'theme',
    ] + platform_hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
)

# cipher= was removed in PyInstaller 6.x; it is silently ignored there.
pyz = PYZ(a.pure, a.zipped_data)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='FuzzyDuplicateFinder',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=is_windows,  # Only use UPX on Windows
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    target_arch=None,
    codesign_identity=None,  # Disable code signing during build
    entitlements_file=None,
    icon=exe_icon,
)

# Only create BUNDLE on macOS
if is_macos:
    app = BUNDLE(
        exe,
        name='FuzzyDuplicateFinder.app',
        icon='assets/icon.icns',
        bundle_identifier='com.fuzzyduplicate.finder',
        info_plist={
            'NSPrincipalClass': 'NSApplication',
            'NSHighResolutionCapable': 'True',
        },
        codesign_identity=None,
    )

"""
Fuzzy Duplicate Finder entry point.

    python main.py                 GUI
    python main.py scan DIR...     headless scan, see --help
"""

import logging
import sys


def _setup_logging():
    from fdf.logs import setup_app_log
    setup_app_log()


def _set_windows_app_id():
    """
    Give the process a fixed taskbar identity before any window exists.
    Without one, Windows identifies the app by its exe path and looks the
    taskbar icon up through the shell's per-path icon cache, which is how the
    icon went missing in some folders and not others.
    """
    if sys.platform != "win32":
        return
    from fdf import WINDOWS_APP_USER_MODEL_ID
    try:
        import ctypes
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(WINDOWS_APP_USER_MODEL_ID)
    except (AttributeError, OSError):
        logging.getLogger(__name__).warning("Could not set the taskbar app ID", exc_info=True)


def main():
    from fdf.cli import wants_cli
    if wants_cli(sys.argv):
        from fdf.cli import main as cli_main
        return cli_main(sys.argv)
    _setup_logging()
    _set_windows_app_id()
    from fdf.gui import main as gui_main
    return gui_main(sys.argv)


if __name__ == "__main__":
    sys.exit(main())

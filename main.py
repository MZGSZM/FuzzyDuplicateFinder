"""
Fuzzy Duplicate Finder entry point.

    python main.py                 GUI
    python main.py scan DIR...     headless scan, see --help
"""

import logging
import logging.handlers
import os
import sys


def _setup_logging():
    from fdf.paths import log_dir
    try:
        handler = logging.handlers.RotatingFileHandler(
            os.path.join(log_dir(), "fuzzy-duplicate-finder.log"),
            maxBytes=1_000_000, backupCount=2, encoding="utf-8",
        )
    except OSError:
        return
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    root = logging.getLogger()
    root.addHandler(handler)
    root.setLevel(logging.INFO)


def main():
    from fdf.cli import wants_cli
    if wants_cli(sys.argv):
        from fdf.cli import main as cli_main
        return cli_main(sys.argv)
    _setup_logging()
    from fdf.gui import main as gui_main
    return gui_main(sys.argv)


if __name__ == "__main__":
    sys.exit(main())

"""
Headless mode.

    FuzzyDuplicateFinder scan ~/Pictures /mnt/backup/Pictures --csv dupes.csv

Follows the GUI's Index Location setting, so a CLI scan makes the next GUI scan
of the same folders incremental and vice versa. --db, --index-appdata,
--index-in-folder and --index-dir override it for one run. Nothing is ever
deleted from the command line.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time

from . import APP_NAME, VERSION
from .exts import KINDS
from .matcher import DEFAULT_MAX_PAIRS, DEFAULT_THRESHOLD
from .scanner import DEFAULT_EXCLUDES, ScanOptions

COMMANDS = ('scan',)

_PHASES = {
    'walk': "Finding files",
    'hash': "Fingerprinting",
    'verify': "Verifying exact duplicates",
    'match': "Matching",
    'finalize': "Finalizing",
}


def wants_cli(argv):
    return len(argv) > 1 and argv[1] in COMMANDS + ('--version', '-V', '--help', '-h')


def build_parser():
    parser = argparse.ArgumentParser(
        prog="FuzzyDuplicateFinder",
        description=f"{APP_NAME} {VERSION}. Run with no arguments for the GUI.",
    )
    parser.add_argument('-V', '--version', action='version', version=f"{APP_NAME} {VERSION}")
    sub = parser.add_subparsers(dest='command', required=True)

    scan = sub.add_parser('scan', help="index folders and report duplicates")
    scan.add_argument('folders', nargs='+', help="folders to scan")
    where = scan.add_argument_group(
        "index location",
        "Default: the Index Location set in the GUI (app data unless changed). These "
        "override it for this run.",
    )
    where = where.add_mutually_exclusive_group()
    where.add_argument('--db', metavar='FILE', help="use this index file")
    where.add_argument('--index-appdata', action='store_true',
                       help="one index per folder set in the app data folder")
    where.add_argument('--index-in-folder', action='store_true',
                       help="keep the index (and scan log) inside the scanned folder")
    where.add_argument('--index-dir', metavar='DIR',
                       help="keep the index (and scan log) in DIR")
    scan.add_argument('--threshold', type=float, default=DEFAULT_THRESHOLD,
                      help=f"minimum fuzzy score, 0-100 (default {DEFAULT_THRESHOLD:g})")
    scan.add_argument('--kinds', default=",".join(KINDS),
                      help="comma list of image,video,audio,text (default: all)")
    scan.add_argument('--exact-only', action='store_true', help="skip fuzzy matching")
    scan.add_argument('--all-types', action='store_true',
                      help="also find exact duplicates of every other file type")
    scan.add_argument('--include-hidden', action='store_true', help="scan hidden files and folders")
    scan.add_argument('--exclude', action='append', default=None, metavar='GLOB',
                      help="skip names matching GLOB; repeatable. Replaces the default list")
    scan.add_argument('--min-size', type=int, default=1, metavar='BYTES',
                      help="ignore files smaller than this (default 1, skips empty files)")
    scan.add_argument('--threads', type=int, default=None, help="worker threads")
    scan.add_argument('--max-pairs', type=int, default=DEFAULT_MAX_PAIRS,
                      help=f"cap on reported fuzzy pairs (default {DEFAULT_MAX_PAIRS})")
    scan.add_argument('--csv', metavar='FILE', help="write matches as CSV")
    scan.add_argument('--json', metavar='FILE', help="write full results as JSON")
    scan.add_argument('--no-watchdog', action='store_true', help="disable the memory watchdog")
    scan.add_argument('--low-priority', action='store_true', help="run at background CPU priority")
    scan.add_argument('-q', '--quiet', action='store_true', help="no progress output")
    scan.add_argument('-v', '--verbose', action='store_true', help="debug logging")
    return parser


def _progress_printer(stream):
    state = {'phase': None, 'last': 0.0, 'key': None}

    def report(phase, done, total, issues):
        now = time.monotonic()
        if (phase, done, total) == state['key']:
            return
        state['key'] = (phase, done, total)
        finished = bool(total) and done >= total
        if phase == state['phase'] and now - state['last'] < 0.5 and not finished:
            return
        state['phase'], state['last'] = phase, now
        label = _PHASES.get(phase, phase)
        if total:
            text = f"{label}: {done}/{total} ({done * 100 // total}%)"
        elif done:
            text = f"{label}: {done}"
        else:
            text = f"{label}..."
        if issues:
            text += f"  issues: {issues}"
        stream.write("\r\033[K" + text if stream.isatty() else text + "\n")
        stream.flush()

    return report


def _saved_index_location():
    """The GUI's Index Location setting. QSettings needs QtCore only, no
    window; if Qt is missing the CLI falls back to the default."""
    try:
        from .settings import AppSettings
    except ImportError:
        return "appdata", ""
    settings = AppSettings()
    return settings.index_mode, settings.index_custom_dir


def _interactive():
    return sys.stdin is not None and sys.stdin.isatty()


def _ask(prompt, err):
    err.write(prompt)
    err.flush()
    try:
        return input().strip()
    except EOFError:
        return ""


def _ask_index_file(suggestion, err):
    from .paths import dir_writable
    answer = _ask(f"Index file [{suggestion}]: ", err) or suggestion
    answer = os.path.abspath(os.path.expanduser(answer))
    if not os.path.splitext(answer)[1]:
        answer += ".db"
    if not dir_writable(os.path.dirname(answer)):
        print(f"Cannot write to {os.path.dirname(answer)}", file=err)
        return None
    return answer


def resolve_index_path(args, err=sys.stderr, interactive=None):
    """
    Same rules as the GUI. Where the GUI would show a dialog, ask on the
    terminal; with no terminal (a script, the smoke test), fail with the flags
    that avoid the question instead of hanging or guessing. Never falls back to
    the app data folder on its own.
    """
    from .paths import default_index_path, dir_writable, folder_index_path, resolve_index

    if args.db:
        return args.db
    if args.index_appdata:
        mode, custom = "appdata", ""
    elif args.index_in_folder:
        mode, custom = "folder", ""
    elif args.index_dir:
        mode, custom = "custom", args.index_dir
    else:
        mode, custom = _saved_index_location()
    if interactive is None:
        interactive = _interactive()

    roots = [os.path.abspath(f) for f in args.folders]
    choice = resolve_index(roots, mode, custom)
    if choice.problem is None:
        return choice.path

    hint = "Use --db FILE, --index-dir DIR, --index-in-folder or --index-appdata."
    if choice.problem == "ask":
        if not interactive:
            print(f"The index location is set to ask each time. {hint}", file=err)
            return None
        return _ask_index_file(choice.path, err)

    if choice.problem == "holder":
        if not interactive:
            print("These folders are on different disks, so the folder that holds the index "
                  "has to be picked. Use --db with a file inside that folder, or "
                  "--index-dir DIR.", file=err)
            return None
        print("These folders are on different disks. The index lists the file paths of all "
              "of them.", file=err)
        for n, root in enumerate(roots, 1):
            print(f"  {n}. {root}", file=err)
        answer = _ask(f"Which folder holds the index? [1-{len(roots)}]: ", err)
        if not answer.isdigit() or not 1 <= int(answer) <= len(roots):
            return None
        holder = roots[int(answer) - 1]
        path = folder_index_path(holder, roots)
        if dir_writable(holder):
            return path
        choice.folder = holder

    # unwritable
    where = choice.folder or "(no folder set for index files)"
    print(f"The index cannot be written to: {where}", file=err)
    if not interactive:
        print(hint, file=err)
        return None
    return _ask_index_file(default_index_path(roots), err)


def run_scan(args):
    from . import export, pipeline
    from .logs import scan_log
    from .watchdog import ResourceWatchdog, format_bytes, lower_priority

    kinds = frozenset() if args.exact_only else frozenset(
        k.strip() for k in args.kinds.split(',') if k.strip()
    )
    unknown = kinds - set(KINDS)
    if unknown:
        print(f"Unknown kind(s): {', '.join(sorted(unknown))}", file=sys.stderr)
        return 2

    options = ScanOptions(
        kinds=kinds,
        all_types=args.all_types or args.exact_only,
        skip_hidden=not args.include_hidden,
        excludes=tuple(args.exclude) if args.exclude is not None else DEFAULT_EXCLUDES,
        min_size=args.min_size,
        max_workers=args.threads,
    )
    err = sys.stderr
    missing = [f for f in args.folders if not os.path.isdir(f)]
    if missing:
        print("Not a folder: " + ", ".join(missing), file=err)
        return 2
    db_path = resolve_index_path(args, err)
    if not db_path:
        return 2
    if args.low_priority:
        lower_priority()

    progress = None if args.quiet else _progress_printer(err)
    watchdog = None if args.no_watchdog else ResourceWatchdog().start()
    try:
        # Outside app data, scan messages go to a log beside the index (and
        # still to the terminal), never to the app data log.
        with scan_log(db_path, extra_handlers=logging.getLogger().handlers):
            results = pipeline.run(
                args.folders, db_path, options, threshold=args.threshold,
                progress=progress, watchdog=watchdog, match_workers=args.threads,
                max_pairs=args.max_pairs,
            )
    except KeyboardInterrupt:
        print("\nInterrupted.", file=err)
        return 130
    finally:
        if watchdog is not None:
            watchdog.stop()

    if not args.quiet and err.isatty():
        err.write("\r\033[K")
    if results.aborted:
        print(results.abort_reason or "Aborted.", file=err)
        return 3

    if args.csv:
        export.write_csv(results.matches, args.csv)
    if args.json:
        export.write_json(results, args.json, args.folders)

    exact = sum(1 for m in results.matches if m.type == 'EXACT')
    fuzzy = len(results.matches) - exact
    if not (args.csv or args.json):
        for m in results.matches:
            tag = "=" if m.type == 'EXACT' else f"{m.score:.0f}%"
            print(f"[{tag}] {m.file_a}\t{m.file_b}")
    if not args.quiet:
        scan = results.scan
        print(
            f"{exact} exact and {fuzzy} similar pair(s); "
            f"{format_bytes(results.reclaimable_bytes)} reclaimable from exact duplicates.",
            file=err,
        )
        if scan is not None:
            print(f"{scan.files} files, {scan.fingerprinted} processed, {scan.reused} unchanged, "
                  f"{scan.full_reads} full reads, {scan.elapsed:.1f}s. Index: {db_path}", file=err)
        if results.truncated:
            print("Result cap reached; only the closest matches are listed.", file=err)
        for path, reason in results.issues:
            print(f"issue: {path}: {reason}", file=err)
    return 0


def main(argv=None):
    # A windowed build (Windows EXE, macOS app) has no console streams.
    if sys.stdout is None:
        sys.stdout = open(os.devnull, 'w')  # noqa: SIM115
    if sys.stderr is None:
        sys.stderr = open(os.devnull, 'w')  # noqa: SIM115
    argv = list(sys.argv if argv is None else argv)
    args = build_parser().parse_args(argv[1:])
    logging.basicConfig(
        level=logging.DEBUG if getattr(args, 'verbose', False) else logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )
    if args.command == 'scan':
        return run_scan(args)
    return 2

"""
Headless mode.

    FuzzyDuplicateFinder scan ~/Pictures /mnt/backup/Pictures --csv dupes.csv

Uses the same index location as the GUI, so a CLI scan makes the next GUI scan
of the same folders incremental and vice versa. Nothing is ever deleted from
the command line.
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
    scan.add_argument('--db', help="index file (default: one per folder set, in the app "
                                   "data folder)")
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


def run_scan(args):
    from . import export, pipeline
    from .paths import default_index_path
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
    db_path = args.db or default_index_path(args.folders)
    if args.low_priority:
        lower_priority()

    err = sys.stderr
    progress = None if args.quiet else _progress_printer(err)
    watchdog = None if args.no_watchdog else ResourceWatchdog().start()
    try:
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

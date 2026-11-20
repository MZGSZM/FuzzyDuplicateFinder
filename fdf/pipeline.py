"""
Scan then match. Shared by the GUI worker thread and the CLI so both behave
the same way.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from .exts import ext_of, kind_of
from .matcher import DEFAULT_MAX_PAIRS, DEFAULT_THRESHOLD, Match, Matcher
from .scanner import Scanner, ScanOptions, ScanResult


@dataclass
class Results:
    matches: list = field(default_factory=list)          # [Match], best first
    exact_groups: list = field(default_factory=list)     # [[path, ...]]
    issues: list = field(default_factory=list)           # [(path, reason)]
    scan: ScanResult | None = None
    truncated: bool = False
    stale_fingerprints: int = 0
    aborted: bool = False
    abort_reason: str = ""

    @property
    def reclaimable_bytes(self):
        """Bytes freed by keeping one copy of every exact group."""
        total = 0
        for group in self.exact_groups:
            try:
                total += os.path.getsize(group[0]) * (len(group) - 1)
            except OSError:
                continue
        return total


def exact_pairs(groups):
    """One 'EXACT' pair per extra copy, against the group's first file."""
    pairs = []
    for group in groups:
        base = group[0]
        kind = kind_of(ext_of(base.path))
        for other in group[1:]:
            pairs.append(Match(base.path, other.path, 100.0, 'EXACT', kind,
                               base.size, other.size))
    return pairs


def run(folder_list, db_path, options=None, threshold=DEFAULT_THRESHOLD, skip_scan=False,
        stop_signal=None, progress=None, watchdog=None, match_workers=None,
        max_pairs=DEFAULT_MAX_PAIRS):
    """
    progress(phase, done, total, issues): phase is 'walk', 'hash', 'verify',
    'match' or 'finalize'.
    """
    options = options or ScanOptions()
    results = Results()

    def stopping():
        if watchdog is not None and watchdog.aborted:
            return True
        return bool(stop_signal and stop_signal())

    if not skip_scan:
        scanner = Scanner(options, watchdog)
        scan = scanner.scan(folder_list, db_path, stop_signal=stopping, progress=progress)
        results.scan = scan
        results.issues = list(scan.issues)
        if scan.aborted:
            results.aborted = True
            results.abort_reason = scan.abort_reason
            return results

    issue_count = len(results.issues)
    with Matcher(db_path, roots=folder_list or None, verify_exists=skip_scan) as matcher:
        if progress:
            progress('match', 0, 0, issue_count)
        groups = matcher.exact_groups()
        if stopping():
            results.aborted = True
            results.abort_reason = watchdog.abort_reason if watchdog else ""
            return results

        fuzzy = matcher.fuzzy_matches(
            threshold=threshold, kinds=options.kinds, stop_signal=stopping,
            progress=(lambda d, t: progress('match', d, t, issue_count)) if progress else None,
            workers=match_workers, max_pairs=max_pairs,
        )
        if stopping():
            results.aborted = True
            results.abort_reason = watchdog.abort_reason if watchdog else ""
            return results
        if skip_scan:
            results.stale_fingerprints = matcher.stale_fingerprints()
        results.truncated = matcher.truncated

    if progress:
        progress('finalize', 0, 0, issue_count)
    results.exact_groups = [[rec.path for rec in group] for group in groups]
    results.matches = exact_pairs(groups) + fuzzy
    return results

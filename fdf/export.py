"""CSV and JSON export of results."""

from __future__ import annotations

import csv
import json
from datetime import datetime

from . import VERSION

CSV_FIELDS = ('type', 'kind', 'score', 'file_a', 'file_b', 'size_a', 'size_b')


def write_csv(matches, path):
    with open(path, 'w', newline='', encoding='utf-8') as handle:
        writer = csv.writer(handle)
        writer.writerow(CSV_FIELDS)
        for m in matches:
            writer.writerow((m.type, m.kind, f"{m.score:.1f}", m.file_a, m.file_b,
                             m.size_a, m.size_b))


def to_dict(results, roots=()):
    return {
        'generator': f"Fuzzy Duplicate Finder {VERSION}",
        'created': datetime.now().isoformat(timespec='seconds'),
        'roots': [r['path'] if isinstance(r, dict) else r for r in roots],
        'exact_groups': results.exact_groups,
        'matches': [
            {'type': m.type, 'kind': m.kind, 'score': m.score, 'file_a': m.file_a,
             'file_b': m.file_b, 'size_a': m.size_a, 'size_b': m.size_b}
            for m in results.matches
        ],
        'issues': [{'path': p, 'reason': r} for p, r in results.issues],
        'truncated': results.truncated,
    }


def write_json(results, path, roots=()):
    with open(path, 'w', encoding='utf-8') as handle:
        json.dump(to_dict(results, roots), handle, indent=2, ensure_ascii=False)
        handle.write("\n")

import csv
import json
import threading

import pytest
from conftest import make_image, make_text

from fdf import cli, pipeline
from fdf.scanner import ScanOptions
from fdf.watchdog import MB, ResourceWatchdog


@pytest.fixture
def lib(tmp_path):
    root = tmp_path / "lib"
    root.mkdir()
    make_image(root / "a.jpg")
    (root / "a_copy.jpg").write_bytes((root / "a.jpg").read_bytes())
    make_image(root / "a_small.jpg", size=(600, 450))
    make_text(root / "t.txt")
    make_text(root / "t2.txt", edit=lambda t: t + " more")
    return root


def test_pipeline_end_to_end(lib, tmp_path):
    results = pipeline.run([str(lib)], str(tmp_path / "i.db"))
    assert not results.aborted
    assert results.exact_groups and len(results.exact_groups[0]) == 2
    types = [m.type for m in results.matches]
    assert types[0] == "EXACT" and "FUZZY" in types
    assert results.reclaimable_bytes == (lib / "a.jpg").stat().st_size


def test_pipeline_skip_scan_uses_existing_index(lib, tmp_path):
    db = str(tmp_path / "i.db")
    first = pipeline.run([str(lib)], db)
    second = pipeline.run([str(lib)], db, skip_scan=True)
    assert second.scan is None
    assert len(second.matches) == len(first.matches)


def test_watchdog_pause_then_resume(lib, tmp_path):
    memory = {"available": 100 * MB}
    dog = ResourceWatchdog(memory_source=lambda: (memory["available"], 8000 * MB),
                           thresholds=(1000 * MB, 1500 * MB, 10 * MB))
    dog.sample()
    assert dog.paused

    def recover():
        memory["available"] = 4000 * MB
        dog.sample()

    timer = threading.Timer(0.3, recover)
    timer.start()
    results = pipeline.run([str(lib)], str(tmp_path / "i.db"), watchdog=dog)
    timer.join()
    assert not results.aborted
    assert results.matches


def test_watchdog_abort_stops_scan(lib, tmp_path):
    dog = ResourceWatchdog(memory_source=lambda: (1 * MB, 8000 * MB),
                           thresholds=(1000 * MB, 1500 * MB, 10 * MB), critical_samples=1)
    dog.sample()
    results = pipeline.run([str(lib)], str(tmp_path / "i.db"), watchdog=dog)
    assert results.aborted and "free memory" in results.abort_reason


def test_exact_only_options(lib, tmp_path):
    options = ScanOptions(kinds=frozenset(), all_types=True)
    results = pipeline.run([str(lib)], str(tmp_path / "i.db"), options)
    assert {m.type for m in results.matches} == {"EXACT"}


def test_cli_scan_writes_csv_and_json(lib, tmp_path, capsys):
    out_csv = tmp_path / "out.csv"
    out_json = tmp_path / "out.json"
    code = cli.main(["prog", "scan", str(lib), "--csv", str(out_csv),
                     "--json", str(out_json), "-q", "--no-watchdog"])
    assert code == 0
    with open(out_csv, newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["type"] == "EXACT"
    data = json.loads(out_json.read_text(encoding="utf-8"))
    assert data["roots"] == [str(lib)]
    assert len(data["matches"]) == len(rows)


def test_cli_prints_pairs_and_reuses_index(lib, capsys):
    assert cli.main(["prog", "scan", str(lib), "--no-watchdog"]) == 0
    first = capsys.readouterr()
    assert "[=]" in first.out
    assert cli.main(["prog", "scan", str(lib), "--no-watchdog"]) == 0
    second = capsys.readouterr()
    assert "0 processed" in second.err


def test_cli_rejects_unknown_kind(lib, capsys):
    assert cli.main(["prog", "scan", str(lib), "--kinds", "image,smell"]) == 2


def test_cli_version_and_dispatch(capsys):
    assert cli.wants_cli(["prog", "scan", "x"])
    assert cli.wants_cli(["prog", "--version"])
    assert not cli.wants_cli(["prog"])
    assert not cli.wants_cli(["prog", "-platform", "offscreen"])
    with pytest.raises(SystemExit) as exc:
        cli.main(["prog", "--version"])
    assert exc.value.code == 0
    assert "Fuzzy Duplicate Finder" in capsys.readouterr().out

"""The release-notes script the build workflow runs."""

import os
import shutil
import subprocess

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPT = os.path.join(ROOT, ".github", "scripts", "release-notes.sh")

pytestmark = pytest.mark.skipif(
    os.name == "nt" or not shutil.which("bash") or not shutil.which("git"),
    reason="needs bash and git",
)


def git(repo, *args):
    env = dict(os.environ, GIT_AUTHOR_NAME="t", GIT_AUTHOR_EMAIL="t@example.com",
               GIT_COMMITTER_NAME="t", GIT_COMMITTER_EMAIL="t@example.com")
    subprocess.run(["git", "-C", str(repo), *args], check=True, env=env,
                   capture_output=True)


def commit(repo, name, message, content="x"):
    (repo / name).write_text(content)
    git(repo, "add", name)
    git(repo, "commit", "-q", "-m", message)


@pytest.fixture
def repo(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    git(repo, "init", "-q", "-b", "main")
    commit(repo, "a.py", "Initial import")
    git(repo, "tag", "v1.0.0")
    commit(repo, "b.py", "Add the thing")
    commit(repo, "a.py", "Fix the crash on start", "y")
    commit(repo, "c.py", "Add files via upload")
    commit(repo, "changes.md", "Write changelog",
           "2026.11.20 - Backend overhaul\n\n* Did stuff\n\n"
           "Reasons for changes: because\n\n2026.01.01 - Old\n\n* old\n")
    git(repo, "tag", "v1.1.0")
    return repo


def run(repo, *args):
    out = subprocess.run(["bash", SCRIPT, *args], cwd=repo, check=True,
                         capture_output=True, text=True)
    return out.stdout


def test_generate_sections(repo):
    notes = run(repo, "generate", "v1.1.0", "https://example.com/r")
    assert notes.startswith("<!-- auto-changelog:start -->")
    assert "## Changes since v1.0.0" in notes
    assert "2026.11.20 - Backend overhaul" in notes and "2026.01.01" not in notes
    assert "**Added**" in notes and "- Add the thing" in notes
    assert "**Fixed**" in notes and "- Fix the crash on start" in notes
    assert "Add files via upload" not in notes          # web uploader noise
    assert "added `c.py`" in notes
    assert "https://example.com/r/compare/v1.0.0...v1.1.0" in notes
    assert notes.rstrip().endswith("<!-- auto-changelog:end -->")


def test_first_tag_has_no_previous(repo):
    notes = run(repo, "generate", "v1.0.0")
    assert "## Changes in v1.0.0" in notes


def test_unknown_tag_fails(repo):
    result = subprocess.run(["bash", SCRIPT, "generate", "v9.9.9"], cwd=repo,
                            capture_output=True, text=True)
    assert result.returncode != 0
    assert "no such tag" in result.stderr


def test_merge_replaces_previous_block(repo, tmp_path):
    existing = tmp_path / "body.md"
    existing.write_text("Hand written.\n\n<!-- auto-changelog:start -->\nold\n"
                        "<!-- auto-changelog:end -->\n")
    fresh = tmp_path / "fresh.md"
    fresh.write_text("<!-- auto-changelog:start -->\nnew\n<!-- auto-changelog:end -->\n")
    merged = run(repo, "merge", str(existing), str(fresh))
    assert merged.count("auto-changelog:start") == 1
    assert "Hand written." in merged and "new" in merged and "old" not in merged

# Maintenance

A checklist for keeping Fuzzy Duplicate Finder healthy. Run through it every couple of months, and before any release that has been sitting for a while. Most of it takes ten minutes when nothing is broken.

Nothing in this repo pins exact versions. Python packages have minimums only, the workflows use `python-version: '3.x'` and floating action majors, and the AppImage tool comes from its continuous release. That keeps things current without manual bumps, but it means something upstream can break a build with no change on our side. This checklist is how you catch that before a release does.

## 1. Run the test suite against current packages

I use a throwaway venv so stale local packages cannot hide a problem:

```
python -m venv /tmp/fdf-check
. /tmp/fdf-check/bin/activate
pip install -U pip
pip install -U -r requirements-dev.txt
python -m pytest
ruff check .
```

On Windows activate with `\tmp\fdf-check\Scripts\activate`, or whichever path you used.

If tests fail, look at what upgraded (`pip list --outdated` in your normal environment shows what moved). The usual suspects:

- **librosa** changes function signatures between majors. Everything the app uses is in `fdf/fingerprints.py` (`feature.chroma_stft`). 1.0 dropped audioread, which is why `.m4a`/`.aac`/`.wma` go through `ffmpeg` directly.
- **numpy** removes deprecated APIs. `np.bitwise_count` is used when present, with a lookup-table fallback.
- **PyQt6** occasionally renames enums. The GUI tests catch most of it.
- **OpenCV** changes how many frames it reports for some containers. `test_video_hash_survives_rescale` covers the seeking path.
- **Pillow** changes which optional image formats it registers. `fdf/exts.py` only enables `.avif`/`.heic`/`.jxl` when Pillow says it can open them.

If a fix is not quick, raise the minimum in `requirements.txt` to the last version that works and note it in `changes.md`. Never pin with `==`.

## 2. Check the CI runs

Open the Actions tab and look at the latest **CI** run on `main`. If it has gone stale, re-run it: a green run from two months ago says nothing about today's packages.

## 3. Check the GitHub Actions versions

The workflows use `actions/checkout`, `actions/setup-python`, `actions/upload-artifact` and `actions/download-artifact` at a major tag. New majors come out roughly once a year, usually because GitHub moves to a newer Node. To see the latest majors:

```
for a in checkout setup-python upload-artifact download-artifact; do
  printf '%s: ' "$a"
  git ls-remote --tags "https://github.com/actions/$a" | grep -oE 'refs/tags/v[0-9]+$' | sort -V | tail -1
done
```

If any is newer than what `.github/workflows/*.yml` uses, bump it and read that action's release notes for breaking changes. Deprecation warnings in the run logs are the early sign.

## 4. Check the Linux build runner

`build.yaml` builds the AppImage on `ubuntu-22.04`, the oldest runner GitHub offers, so the AppImage works on older distros. GitHub announces runner retirements on their changelog well ahead. When 22.04 is on its way out, change it to the next oldest (`ubuntu-24.04`). The cost is that the AppImage then needs glibc 2.39 or newer; update the requirement line in the README's Linux install section to match.

## 5. Check the newest Python

`'3.x'` means a new Python release gets picked up by CI on its own, usually in October. numba and llvmlite (pulled in by librosa) tend to be the last to publish wheels for a new Python. If CI breaks right after a Python release with a build error from numba or llvmlite, that is the cause. The fix until wheels exist is to set `python-version: '3.13'` (or whichever the last working one is) in both workflows, with a comment, and remove it again at the next check.

To see whether a dependency has wheels for a version yet:

```
pip download --no-deps --only-binary=:all: --python-version 3.15 numba -d /tmp/wheels
```

## 6. Do a test release build

The release workflow only runs on a tag, so after any of the changes above, prove it still builds before you need it:

1. Make a throwaway pre-release on the Releases page, with a tag like `v0.0.0-test`, ticking **Set as a pre-release**.
2. Run **Build and release** on that tag (see "Making a release" in the README).
3. Download the AppImage and check it starts. Ideally do the same with the Windows EXE and macOS zip.
4. Delete the test release and its tag afterwards (Releases page, then Tags).

Things that have broken frozen builds in the past, and where to look:

- **librosa or numba fails to import in the build only.** PyInstaller's hooks for them live in `pyinstaller-hooks-contrib`, which updates with PyInstaller. `NUMBA_CACHE_DIR` is set in `fdf/fingerprints.py` for frozen builds because numba cannot cache inside a bundle otherwise.
- **Qt platform plugin errors on Linux** ("could not load the Qt platform plugin xcb"). The build runner installs `libxcb-cursor0` and friends so PyInstaller bundles them. If Qt adds a new system dependency, add it to the apt line in `build.yaml`.
- **appimagetool changes its options.** It comes from the continuous release at build time. Check `.github/scripts/build-appimage.sh` against its README.

## 7. Fingerprint format changes

If you change how any fuzzy fingerprint is computed (image, video, audio or text), bump `FINGERPRINT_VERSION` in `fdf/fingerprints.py`. Every existing index then recomputes those fingerprints on the next scan instead of comparing old hashes with new ones. Exact hashes are not affected and are kept. Add a test that shows the new fingerprint still matches what it should and rejects what it should.

If you add a column to the index, add it to `COLUMNS` in `fdf/index.py`. Existing indexes get it automatically through `ALTER TABLE`. `test_index_upgrades_old_schema` covers the upgrade path from 1.5 indexes.

## 8. Scripts

All shell scripts must pass shellcheck, which CI enforces:

```
shellcheck .github/scripts/*.sh packaging/appimage/AppRun tools/*.sh
```

## 9. Record what you did

Add an entry to `changes.md` for anything you changed, even if it is only dependency minimums or a workflow bump, so the next release summary picks it up.

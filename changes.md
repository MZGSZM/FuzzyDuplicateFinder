2026.11.20 - Test data

* Added FuzzyDuplicateFinder-test-images/: 200 generated images in 10 groups, graded from a byte-identical copy down to an unrelated scene, with manifest.csv recording what each file is and the score 1.6.0 gave it, and generate_test_images.py to rebuild the set.
* README: new "Test images" section under Development explaining what the set is for and how to use it, and the folder added to the project layout.

Reasons for changes: a repeatable set for checking by hand what the matcher catches and misses, and for spotting changes after work on fingerprints or scoring.

2026.11.20 - Backend overhaul / UX overhaul / Build and release automation

* Version 1.6.0.
* Code moved into an `fdf` package with one module per job. File type tables now live in one place instead of three copies that had to be kept in sync by hand.
* Scanning is about 4x faster on JPEG-heavy libraries: JPEGs decode at reduced size for hashing, since pHash only ever looks at a 32x32 thumbnail.
* Exact duplicate detection reads far less: files that share a size get a head/tail quick hash first, and only files that also match on that are read in full. Files up to 128 KB are covered by the quick hash alone.
* Scans are bounded: work is submitted a window at a time instead of queuing a future for every file, and worker threads never touch sqlite, so there is no lock contention.
* Files deleted or excluded since the last scan are removed from the index instead of lingering.
* Symlinks are no longer indexed, so the app can never offer to trash a link target and leave a dangling link, or report a link and its target as duplicates.
* Image fingerprints apply EXIF orientation, so a phone photo and an editor's rotated copy match.
* Video fingerprints use a four-frame montage instead of one middle frame, and record the duration.
* Audio fingerprints are now a real similarity measure. The old one was an MD5 of rounded chroma averages, so two encodes either matched exactly or not at all. The new 192-bit chroma grid matches re-encodes, other bitrates and other formats, and records the duration.
* `.m4a`, `.aac` and `.wma` decode through `ffmpeg`, since librosa 1.0 dropped its fallback decoder. Added `.opus`, `.oga`, `.aiff`, `.aif` audio, `.m2ts`, `.mpg`, `.mpeg`, `.vob` video, and many text and code extensions. `.avif`, `.heic`, `.heif`, `.jxl` and `.jp2` are used when Pillow can read them. Dropped `.raw` from images: Pillow cannot open it, so it only ever produced errors. It is still exact-matched with the new all-types option.
* Text files are compared by content (SimHash) instead of by filename alone. The old way listed every README.md against every other README.md, and its pure-Python pair loop could take hours on a source tree.
* For video and audio, duration replaces file size in the score. A re-encode changes the size a lot and the duration not at all.
* Exact copies are collapsed to one before similar matching, so a near-copy is listed once instead of once per exact copy.
* Similar matching runs on threads instead of a process pool. numpy releases the GIL for the heavy part, and there is nothing to spawn, pickle, or break inside a frozen app. Candidate pairs are capped so thousands of near-identical files cannot run the machine out of memory.
* Resource watchdog: pauses the scan when free memory runs low, stops it if memory stays critical, and names files that hang a decoder. The status bar shows RAM, CPU and free memory during a scan. Optional background CPU priority for scans.
* Incremental scanning by default: each folder set gets its own index in the app data folder, and rescans only read new and changed files. The old behaviour wiped the index on every scan and asked to delete it on every exit.
* Re-match without rescanning (F6), so threshold changes apply in seconds. Similarity threshold is now adjustable in the toolbar.
* Scan options dialog: which kinds to compare, exact matching of all other file types, skip hidden, exclude patterns, minimum size, CPU priority, watchdog, slow-file limit, delete confirmation.
* Previews load in the background, so selecting a large photo or a video no longer freezes the window. Text files show their first lines. Formats Qt cannot read fall back to Pillow.
* Results list rebuilt on a model with a kind/type filter and path search. Large result sets no longer create a widget per row.
* Keyboard shortcuts for scanning and reviewing, drag-and-drop folders, per-folder remove buttons, Show in Folder, CSV and JSON export, reclaimable space summary, ETA during hashing.
* Files With Issues lists the reason for each file and can copy or export it.
* Auto-prune shows every file it will move and the copy it keeps before doing anything, and reports files that could not be moved.
* Deleting a file removes it from the index too.
* Command line mode: `FuzzyDuplicateFinder scan DIR...` with CSV/JSON output, sharing the GUI's index.
* Log file in the app data folder, reachable from Help.
* Light and dark themes kept, plus System. Spin boxes now render their step arrows in both themes; buttons show a visible keyboard focus.
* Linux build is now an AppImage. macOS build is a one-folder .app in a zip (one-file .app bundles are deprecated in PyInstaller 6). Windows stays a single EXE. UPX removed.
* `opencv-python` replaced by `opencv-python-headless`, which avoids bundling a second Qt that can clash with PyQt6. Added `psutil`, `soundfile` and `soxr` as direct dependencies. `requirements.txt` uses minimum versions instead of exact pins.
* New pytest suite (fingerprints, scanner, index upgrades, matcher, watchdog, pipeline, CLI, offscreen GUI, release notes script) running on Linux, Windows and macOS in CI, plus ruff and shellcheck.
* New release workflow: run by hand on a tag that has a release, tests, builds all three platforms, smoke-tests each build, attaches the files and SHA256SUMS, and writes a changelog of everything since the previous tag into the release notes.
* Removed the duplicate root `build.yaml`, `debug/junk.py` (replaced by test fixtures) and the stray print statements.
* `tools/install_linux_desktop_entry.sh` rewritten to handle paths with special characters and to work with the AppImage.

Reasons for changes: bring the project up to a maintainable standard with tests and automated releases, make scans faster and safer on large libraries, keep memory use in check, make audio and text matching actually find similar files, and make the review workflow quicker.

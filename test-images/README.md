# Fuzzy Duplicate Finder test images

200 images for testing Fuzzy Duplicate Finder, graded from byte-identical copies down to completely unrelated pictures, with a manifest that says what each file is.

## What is in here

- `images/`: 200 images in 10 groups of 20. Each group starts from one generated 1600x1200 scene (sky, hills, sun, buildings, a little texture).
- `manifest.csv`: one row per image, telling you which group it belongs to and what was done to it.
- `generate_test_images.py`: the script that made all of this, so you can rebuild or change the set.

File names are random (`img_1e7ea419.jpg` and so on), so the app's filename signal cannot give away which files belong together. Use the manifest to check the results.

## The 20 files in each group

From most to least similar to the group's original:

| Step | Variant | Rough human similarity |
|------|---------|------------------------|
| 0 | original | 100% |
| 1 | byte-identical copy | 100% |
| 2 | re-saved JPEG quality 95 | 98% |
| 3 | saved as PNG | 97% |
| 4 | saved as WebP | 96% |
| 5 | re-saved JPEG quality 60 | 95% |
| 6 | re-saved JPEG quality 25 | 92% |
| 7 | resized to 50% | 92% |
| 8 | rotated 90 degrees with an EXIF tag, so it displays upright | 90% |
| 9 | brightness +12% | 88% |
| 10 | contrast +25% | 85% |
| 11 | gaussian blur radius 3 | 82% |
| 12 | heavy noise, sigma 25 | 78% |
| 13 | grayscale | 75% |
| 14 | cropped 8% and scaled back up | 70% |
| 15 | black caption bar over the bottom 18% | 60% |
| 16 | rotated 4 degrees | 50% |
| 17 | cropped 25% and scaled back up | 40% |
| 18 | mirrored left to right | 25% |
| 19 | a different scene | 0% |

The percentages are my estimate of how alike a person would call them, not anything the app computes.

## Manifest columns

| Column | Meaning |
|--------|---------|
| `file` | file name in `images/` |
| `group` | 1 to 10 |
| `step` | 0 to 19, as in the table above |
| `variant` | what was done |
| `human_similarity_estimate` | the rough percentage from the table |
| `base` | the group's original |
| `phash_distance_to_original` | bits that differ out of 64, using the app's own image fingerprint |
| `app_score_vs_original` | the score the app gives this file against the original, or `exact` for byte-identical files |

## Using it

1. Extract the tar anywhere.
2. In Fuzzy Duplicate Finder, add the `images` folder and start a scan.
3. Compare what it lists against `manifest.csv`. A spreadsheet sorted by `group` then `step` makes this easy.

Or from the command line:

```
FuzzyDuplicateFinder scan /path/to/FuzzyDuplicateFinder-test-images/images --csv results.csv
```

## What 1.6.0 finds at the default 70% threshold

- **Steps 1 to 13** (copy through grayscale): found in almost every group. 10 of 10 except JPEG q25 (9), contrast +25% (8) and heavy noise (9). The misses scored in the high 60s.
- **Steps 14 to 19** (crops, caption bar, small rotation, mirror, different scene): never found. pHash is not built to survive crops, rotations or mirroring, so this is expected.
- **Between groups:** no false matches at all.
- **All 10 groups:** the byte-identical copy shows up as an exact duplicate.

Note that pixel-identical files with different names score well under 100: re-saves and format changes land around 73 to 89. That is because the score also weighs filename and file size, and here the names are random and the sizes differ by format. PNG and WebP copies sit just above the threshold, around 73 to 79.

## Rebuilding

Prerequisites: Python 3.10+ with numpy and Pillow, plus a checkout of the app for the two measured columns.

1. `python3 generate_test_images.py OUTDIR /path/to/FuzzyDuplicateFinder`
2. Leave off the repo path to skip the measured columns.

The set is deterministic: the same script gives the same scenes and file names every time. Byte-for-byte output can still shift a little between Pillow versions.

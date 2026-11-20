"""
Build a graded image test set for Fuzzy Duplicate Finder: 10 base scenes,
20 files each, from the original and a byte-identical copy down to an
unrelated scene. 200 images in OUTDIR/images, plus OUTDIR/manifest.csv
(kept outside images/ so a scan of that folder does not pick it up).

    python3 generate_test_images.py OUTDIR [REPO_DIR]

With REPO_DIR (a checkout of the app), the manifest also gets the pHash
distance and the app's own score for every file against its group's original.
"""
import csv
import hashlib
import os
import random
import shutil
import sys

import numpy as np
from PIL import Image, ImageDraw, ImageEnhance, ImageFilter, ImageFont, ImageOps

if len(sys.argv) not in (2, 3):
    sys.exit(__doc__)
OUT = sys.argv[1]
REPO = sys.argv[2] if len(sys.argv) == 3 else None
W, H = 1600, 1200

def scene(seed):
    r = random.Random(seed)
    img = Image.new("RGB", (W, H))
    top = [r.randint(40, 200) for _ in range(3)]
    bot = [r.randint(40, 255) for _ in range(3)]
    g = np.linspace(0, 1, H)[:, None, None]
    arr = (np.array(top) * (1 - g) + np.array(bot) * g).repeat(W, axis=1)
    img = Image.fromarray(arr.astype("uint8"))
    d = ImageDraw.Draw(img)
    # hills
    for _layer in range(3):
        base = r.randint(H // 2, H - 100)
        pts = [(0, H)]
        for x in range(0, W + 100, 100):
            pts.append((x, base + r.randint(-120, 120)))
        pts.append((W, H))
        d.polygon(pts, fill=tuple(r.randint(0, 200) for _ in range(3)))
    # sun / circles
    for _ in range(r.randint(2, 5)):
        cx, cy, rad = r.randint(0, W), r.randint(0, H), r.randint(40, 260)
        d.ellipse((cx - rad, cy - rad, cx + rad, cy + rad),
                  fill=tuple(r.randint(0, 255) for _ in range(3)))
    # buildings / boxes
    for _ in range(r.randint(4, 10)):
        x, y = r.randint(0, W - 200), r.randint(H // 3, H - 50)
        d.rectangle((x, y, x + r.randint(60, 300), H),
                    fill=tuple(r.randint(0, 255) for _ in range(3)))
    # fine texture so it is not flat vector art
    noise = np.random.default_rng(seed).normal(0, 6, (H, W, 3))
    arr = np.clip(np.asarray(img, dtype=np.float32) + noise, 0, 255).astype("uint8")
    return Image.fromarray(arr).filter(ImageFilter.GaussianBlur(0.8))

def crop(img, frac):
    dx, dy = int(W * frac / 2), int(H * frac / 2)
    return img.crop((dx, dy, W - dx, H - dy)).resize((W, H))

def noise(img, sigma):
    a = np.asarray(img, dtype=np.float32)
    a += np.random.default_rng(1).normal(0, sigma, a.shape)
    return Image.fromarray(np.clip(a, 0, 255).astype("uint8"))

def overlay(img, text):
    img = img.copy()
    d = ImageDraw.Draw(img)
    try:
        font = ImageFont.truetype("DejaVuSans-Bold.ttf", 110)
    except OSError:
        font = ImageFont.load_default(size=110)
    d.rectangle((0, H - 220, W, H), fill=(0, 0, 0))
    d.text((60, H - 190), text, fill=(255, 255, 255), font=font)
    return img

def half_swap(img, other):
    img = img.copy()
    img.paste(other.crop((W // 2, 0, W, H)), (W // 2, 0))
    return img

# (description, human similarity estimate %, how to make it, how to save it)
# The callable gets (base_image, other_scene) and returns an image or the
# special value "COPY" for a byte-identical file.
STEPS = [
    ("byte-identical copy", 100, lambda b, o: "COPY", ".jpg"),
    ("re-saved JPEG q95", 98, lambda b, o: b, "q95"),
    ("saved as PNG", 97, lambda b, o: b, ".png"),
    ("saved as WebP", 96, lambda b, o: b, ".webp"),
    ("re-saved JPEG q60", 95, lambda b, o: b, "q60"),
    ("re-saved JPEG q25", 92, lambda b, o: b, "q25"),
    ("resized to 50%", 92, lambda b, o: b.resize((W // 2, H // 2), Image.LANCZOS), "q90"),
    ("rotated 90 deg with EXIF tag (displays upright)", 90, None, "exif"),
    ("brightness +12%", 88, lambda b, o: ImageEnhance.Brightness(b).enhance(1.12), "q90"),
    ("contrast +25%", 85, lambda b, o: ImageEnhance.Contrast(b).enhance(1.25), "q90"),
    ("gaussian blur r=3", 82, lambda b, o: b.filter(ImageFilter.GaussianBlur(3)), "q90"),
    ("heavy noise sigma=25", 78, lambda b, o: noise(b, 25), "q90"),
    ("grayscale", 75, lambda b, o: ImageOps.grayscale(b).convert("RGB"), "q90"),
    ("cropped 8% and rescaled", 70, lambda b, o: crop(b, 0.08), "q90"),
    ("caption bar over bottom 18%", 60, lambda b, o: overlay(b, "SUMMER 2024"), "q90"),
    ("rotated 4 deg", 50,
     lambda b, o: b.rotate(4, resample=Image.BICUBIC, fillcolor=(0, 0, 0)), "q90"),
    ("cropped 25% and rescaled", 40, lambda b, o: crop(b, 0.25), "q90"),
    ("mirrored left-right", 25, lambda b, o: ImageOps.mirror(b), "q90"),
    ("right half replaced by another scene", 15, lambda b, o: half_swap(b, o), "q90"),
    ("unrelated scene", 0, lambda b, o: o, "q90"),
]
assert len(STEPS) == 20

IMG = os.path.join(OUT, "images")
os.makedirs(IMG, exist_ok=True)
rng = random.Random(2026)
rows = []
for group in range(10):
    base_img = scene(1000 + group)
    other = scene(5000 + group)
    base_name = f"img_{rng.getrandbits(32):08x}.jpg"
    base_path = os.path.join(IMG, base_name)
    base_img.save(base_path, quality=92)
    for step, (desc, pct, make, kind) in enumerate(STEPS):
        if step == 0:
            name, path = base_name, base_path
            # step 0 row is the original; the byte copy gets its own file
            rows.append(dict(file=name, group=group + 1, step=0, variant="original",
                             human_similarity_estimate=100, base=base_name))
            name = f"img_{rng.getrandbits(32):08x}.jpg"
            shutil.copyfile(base_path, os.path.join(IMG, name))
            rows.append(dict(file=name, group=group + 1, step=1, variant=desc,
                             human_similarity_estimate=pct, base=base_name))
            continue
        ext = kind if kind.startswith(".") else ".jpg"
        name = f"img_{rng.getrandbits(32):08x}{ext}"
        path = os.path.join(IMG, name)
        if kind == "exif":
            exif = Image.Exif()
            exif[0x0112] = 6    # orientation: display rotated 90 deg clockwise
            base_img.rotate(90, expand=True).save(path, quality=90, exif=exif)
        else:
            img = make(base_img, other)
            if kind.startswith("q"):
                img.save(path, quality=int(kind[1:]))
            else:
                img.save(path)
        rows.append(dict(file=name, group=group + 1, step=step + 1, variant=desc,
                         human_similarity_estimate=pct, base=base_name))

# Drop the last step of each group so the total is exactly 200 files:
# original + byte copy + 18 variants = 20 per group. The unrelated scene is
# kept; the 'right half replaced' step goes.
keep = []
for r in rows:
    if r["variant"] == "right half replaced by another scene":
        os.remove(os.path.join(IMG, r["file"]))
        continue
    keep.append(r)
rows = keep

def write_manifest():
    with open(os.path.join(OUT, "manifest.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


if REPO is None:
    write_manifest()
    print(len(rows), "images written; pass the app's repo folder to add measured scores")
    sys.exit(0)

# Measure with the app's own fingerprint and scoring.
sys.path.insert(0, os.path.abspath(REPO))
from fdf import fingerprints as fp  # noqa: E402
from fdf.matcher import FileRec, score_pair  # noqa: E402


def md5(path):
    with open(path, "rb") as handle:
        return hashlib.md5(handle.read()).digest()


def rec(path):
    st = os.stat(path)
    return FileRec(path=path, name=os.path.basename(path), ext=os.path.splitext(path)[1].lower(),
                   size=st.st_size, exact=None, duration=None, inode=None)

for r in rows:
    p = os.path.join(IMG, r["file"])
    b = os.path.join(IMG, r["base"])
    same = md5(p) == md5(b)
    d = bin(int(fp.image_hash(p), 16) ^ int(fp.image_hash(b), 16)).count("1")
    r["phash_distance_to_original"] = d
    r["app_score_vs_original"] = "exact" if same else score_pair("image", d, rec(p), rec(b))

write_manifest()
print(len(rows), "rows;", len(os.listdir(IMG)), "images")

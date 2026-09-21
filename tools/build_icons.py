#!/usr/bin/env python3
"""
Regenerate every raster icon from the SVG sources in assets/.

    pip install cairosvg Pillow
    python tools/build_icons.py

assets/icon.svg        full-detail artwork, used for 48 px and up
assets/icon-small.svg  simplified artwork, used for 16-32 px where the
                       blur and photo glyph would turn to noise

Outputs:
    assets/icons/icon-<size>.png   runtime window/taskbar icon set
    assets/icon.ico                Windows executable icon
    assets/icon.icns               macOS bundle icon
"""

import io
import os
import sys

try:
    import cairosvg
    from PIL import Image
except ImportError:
    sys.exit("Requires: pip install cairosvg Pillow")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ASSETS = os.path.join(ROOT, "assets")
ICONS = os.path.join(ASSETS, "icons")

FULL_SVG = os.path.join(ASSETS, "icon.svg")
SMALL_SVG = os.path.join(ASSETS, "icon-small.svg")

SMALL_SIZES = (16, 24, 32)
LARGE_SIZES = (48, 64, 128, 256, 512, 1024)


def render(svg_path, size):
    png = cairosvg.svg2png(url=svg_path, output_width=size, output_height=size)
    return Image.open(io.BytesIO(png)).convert("RGBA")


def main():
    os.makedirs(ICONS, exist_ok=True)

    frames = {}
    for size in SMALL_SIZES:
        frames[size] = render(SMALL_SVG, size)
    for size in LARGE_SIZES:
        frames[size] = render(FULL_SVG, size)

    for size, img in frames.items():
        if size <= 512:
            img.save(os.path.join(ICONS, f"icon-{size}.png"))

    # Windows: embed each size explicitly so small frames use the
    # simplified artwork rather than a downscale of the detailed one.
    ico_sizes = (16, 24, 32, 48, 64, 128, 256)
    frames[256].save(
        os.path.join(ASSETS, "icon.ico"),
        format="ICO",
        sizes=[(s, s) for s in ico_sizes],
        append_images=[frames[s] for s in ico_sizes if s != 256],
    )

    # macOS
    frames[1024].save(os.path.join(ASSETS, "icon.icns"), format="ICNS")

    print("Wrote:")
    for name in sorted(os.listdir(ICONS)):
        print("  assets/icons/" + name)
    print("  assets/icon.ico")
    print("  assets/icon.icns")


if __name__ == "__main__":
    main()

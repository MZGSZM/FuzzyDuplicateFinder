import os
import shutil
import sys

import numpy as np
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")


@pytest.fixture(autouse=True)
def isolated_data_dir(tmp_path, monkeypatch):
    """Keep indexes and logs out of the real user data folder."""
    monkeypatch.setenv("FDF_DATA_DIR", str(tmp_path / "appdata"))


# ---------------------------------------------------------------------------
# Media generators
# ---------------------------------------------------------------------------

def make_image(path, seed=1, size=(1200, 900), quality=90, transform=None):
    from PIL import Image
    rng = np.random.default_rng(seed)
    base = (rng.random((45, 60, 3)) * 255).astype("uint8")
    img = Image.fromarray(base).resize(size, Image.BICUBIC)
    if transform:
        img = transform(img)
    kwargs = {"quality": quality} if str(path).lower().endswith((".jpg", ".jpeg")) else {}
    img.save(path, **kwargs)
    return path


def make_text(path, seed=1, words=2000, edit=None):
    rng = np.random.default_rng(seed)
    text = " ".join(f"w{rng.integers(0, 400)}" for _ in range(words))
    if edit:
        text = edit(text)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(text)
    return path


def song(seed, seconds=30, sr=22050):
    rng = np.random.default_rng(seed)
    t = np.arange(int(sr * 2)) / sr
    parts = []
    for _ in range(seconds // 2):
        root = rng.integers(0, 12)
        freqs = [220 * 2 ** ((root + i) / 12) for i in (0, 4, 7)]
        parts.append(sum(np.sin(2 * np.pi * f * t) for f in freqs) / 3 * 0.5)
    return np.concatenate(parts).astype("float32")


def make_audio(path, seed=5, sr=22050, gain=1.0, seconds=30):
    import soundfile as sf
    sf.write(path, song(seed, seconds, sr) * gain, sr)
    return path


def make_video(path, seed=3, size=(320, 240), frames=60, fps=20):
    """mp4v via OpenCV's bundled writer, so no ffmpeg is needed."""
    import cv2
    rng = np.random.default_rng(seed)
    writer = cv2.VideoWriter(str(path), cv2.VideoWriter_fourcc(*"mp4v"), fps, size)
    if not writer.isOpened():
        pytest.skip("OpenCV cannot write mp4v here")
    blocks = [(rng.random((12, 16, 3)) * 255).astype("uint8") for _ in range(4)]
    for i in range(frames):
        frame = cv2.resize(blocks[(i * 4) // frames], size, interpolation=cv2.INTER_NEAREST)
        writer.write(frame)
    writer.release()
    return path


def have_librosa():
    import importlib.util
    return importlib.util.find_spec("librosa") is not None


requires_librosa = pytest.mark.skipif(not have_librosa(), reason="librosa not installed")
requires_ffmpeg = pytest.mark.skipif(shutil.which("ffmpeg") is None, reason="ffmpeg not on PATH")
posix_only = pytest.mark.skipif(os.name == "nt", reason="POSIX-only behaviour")

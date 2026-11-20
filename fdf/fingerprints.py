"""
Content fingerprints.

Every function here takes a path and returns a hex string (or a tuple with a
duration), or raises. The scanner decides what a failure means.

    exact_hash      MD5 of the whole file
    quick_hash      MD5 of the first and last QUICK_BYTES, a cheap pre-filter
    image_hash      64-bit pHash, EXIF orientation applied
    video_hash      64-bit pHash of a 2x2 montage of four frames, plus duration
    audio_hash      192-bit chroma grid, plus duration
    text_hash       64-bit SimHash over word shingles

FINGERPRINT_VERSION is stored per row. Bump it whenever any fuzzy fingerprint
changes so existing indexes recompute instead of mixing incompatible hashes.
Exact and quick hashes do not depend on it.
"""

from __future__ import annotations

import hashlib
import os
import re
import shutil
import subprocess
import sys

# Must be set before cv2 is imported to have any effect.
os.environ.setdefault("OPENCV_LOG_LEVEL", "OFF")

import cv2
import imagehash
import numpy as np
from PIL import Image, ImageOps

from .exts import SOUNDFILE_EXTS, ext_of

FINGERPRINT_VERSION = 2

HASH_CHUNK = 1024 * 1024
QUICK_BYTES = 64 * 1024

# Allow very large images without disabling the decompression bomb guard
# entirely. This tool gets pointed at arbitrary directories.
Image.MAX_IMAGE_PIXELS = 512_000_000

# Parallelism comes from the scanner's own threads. Letting OpenCV spin up a
# pool per call on top of that oversubscribes the CPU.
cv2.setNumThreads(1)

# JPEG and a few others can decode at 1/2, 1/4 or 1/8 scale. pHash works on a
# 32x32 thumbnail, so decoding a 24 megapixel photo at full size is wasted.
DRAFT_SIZE = 256

VIDEO_POSITIONS = (0.15, 0.38, 0.62, 0.85)
VIDEO_TILE = 64

AUDIO_SECONDS = 45.0
AUDIO_SR = 11025
AUDIO_SEGMENTS = 16
AUDIO_BITS = 12 * AUDIO_SEGMENTS          # 192
AUDIO_WORDS = AUDIO_BITS // 64            # 3

TEXT_READ_BYTES = 512 * 1024
TEXT_MAX_SHINGLES = 60_000
_WORD_RE = re.compile(r"\w+", re.UNICODE)


class FingerprintError(Exception):
    """A file could be read but not fingerprinted. The message is user-facing."""


# ---------------------------------------------------------------------------
# Exact
# ---------------------------------------------------------------------------

def exact_hash(path):
    with open(path, 'rb', buffering=0) as handle:
        file_digest = getattr(hashlib, 'file_digest', None)
        if file_digest is not None:
            return file_digest(handle, hashlib.md5).hexdigest()
        hasher = hashlib.md5()
        for chunk in iter(lambda: handle.read(HASH_CHUNK), b""):
            hasher.update(chunk)
        return hasher.hexdigest()


def quick_hash(path, size):
    """
    Hash of the head and tail of a file. Two files that differ here cannot be
    identical, so only files that collide on (size, quick_hash) need a full
    read. Files no larger than 2 * QUICK_BYTES are read whole, which makes the
    quick hash equal to proof of identity for them.
    """
    hasher = hashlib.md5()
    with open(path, 'rb') as handle:
        if size <= 2 * QUICK_BYTES:
            hasher.update(handle.read())
        else:
            hasher.update(handle.read(QUICK_BYTES))
            handle.seek(-QUICK_BYTES, os.SEEK_END)
            hasher.update(handle.read(QUICK_BYTES))
    return hasher.hexdigest()


# ---------------------------------------------------------------------------
# Images
# ---------------------------------------------------------------------------

def image_hash(path):
    with Image.open(path) as img:
        try:
            img.draft('L', (DRAFT_SIZE, DRAFT_SIZE))
        except Exception:
            pass
        # A phone photo and the same photo saved rotated by an editor should
        # hash the same, so apply the EXIF orientation first.
        oriented = ImageOps.exif_transpose(img)
        try:
            return str(imagehash.phash(oriented))
        finally:
            if oriented is not img:
                oriented.close()


# ---------------------------------------------------------------------------
# Video
# ---------------------------------------------------------------------------

def _read_frame(cap, index):
    if index is not None:
        cap.set(cv2.CAP_PROP_POS_FRAMES, index)
    ok, frame = cap.read()
    return frame if ok and frame is not None else None


def video_hash(path):
    """
    (phash_hex, duration_seconds|None).

    One frame from the middle of a video says little; four frames tiled into a
    montage tell two different clips from the same camera apart much better,
    and the duration catches trimmed or extended cuts.
    """
    cap = cv2.VideoCapture(path)
    try:
        if not cap.isOpened():
            raise FingerprintError("could not open video")
        total = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
        fps = float(cap.get(cv2.CAP_PROP_FPS) or 0.0)
        duration = (total / fps) if (total > 0 and fps > 0) else None

        tiles = []
        if total > 4:
            for pos in VIDEO_POSITIONS:
                frame = _read_frame(cap, int(total * pos))
                if frame is not None:
                    tiles.append(frame)
        if not tiles:
            # Seeking is unreliable for some containers; take what decodes.
            cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
            frame = _read_frame(cap, None)
            if frame is not None:
                tiles.append(frame)
        if not tiles:
            raise FingerprintError("no decodable frames")

        small = [
            cv2.resize(cv2.cvtColor(f, cv2.COLOR_BGR2GRAY), (VIDEO_TILE, VIDEO_TILE),
                       interpolation=cv2.INTER_AREA)
            for f in tiles
        ]
        while len(small) < 4:
            small.append(small[-1])
        montage = np.vstack([np.hstack(small[:2]), np.hstack(small[2:4])])
        with Image.fromarray(montage) as img:
            return str(imagehash.phash(img)), duration
    finally:
        cap.release()


# ---------------------------------------------------------------------------
# Audio
# ---------------------------------------------------------------------------

_librosa = None


def _get_librosa():
    """Import librosa on first use. It pulls in numba and scipy, which adds
    seconds to startup for a feature many scans never touch."""
    global _librosa
    if _librosa is None:
        if getattr(sys, 'frozen', False) and 'NUMBA_CACHE_DIR' not in os.environ:
            # numba cannot locate a cache next to modules inside a frozen
            # bundle and refuses to compile cached functions without one.
            from .paths import cache_dir
            os.environ['NUMBA_CACHE_DIR'] = os.path.join(cache_dir(), 'numba')
        import librosa
        _librosa = librosa
    return _librosa


def audio_installed():
    """Cheap check that does not import librosa."""
    import importlib.util
    return importlib.util.find_spec('librosa') is not None


def _ffmpeg():
    return shutil.which('ffmpeg')


def _decode_with_soundfile(path):
    import soundfile as sf
    with sf.SoundFile(path) as snd:
        sr = snd.samplerate
        frames = snd.frames
        data = snd.read(frames=int(AUDIO_SECONDS * sr), dtype='float32', always_2d=True)
    duration = frames / sr if frames > 0 and sr > 0 else None
    return data.mean(axis=1), sr, duration


def _probe_duration(ffmpeg, path):
    # ffmpeg prints "Duration: HH:MM:SS.xx" on stderr for any input it opens.
    proc = subprocess.run([ffmpeg, '-hide_banner', '-i', path],
                          capture_output=True, text=True, errors='replace',
                          timeout=60, check=False)
    match = re.search(r"Duration:\s*(\d+):(\d+):(\d+(?:\.\d+)?)", proc.stderr)
    if not match:
        return None
    h, m, s = match.groups()
    return int(h) * 3600 + int(m) * 60 + float(s)


def _decode_with_ffmpeg(path):
    ffmpeg = _ffmpeg()
    if not ffmpeg:
        raise FingerprintError("needs ffmpeg on PATH to decode this format")
    proc = subprocess.run(
        [ffmpeg, '-nostdin', '-hide_banner', '-loglevel', 'error', '-i', path,
         '-t', str(AUDIO_SECONDS), '-vn', '-ac', '1', '-ar', str(AUDIO_SR),
         '-f', 'f32le', '-'],
        capture_output=True, timeout=300, check=False,
    )
    if proc.returncode != 0 or not proc.stdout:
        raise FingerprintError("ffmpeg could not decode the audio")
    y = np.frombuffer(proc.stdout, dtype=np.float32)
    return y, AUDIO_SR, _probe_duration(ffmpeg, path)


def decode_audio(path):
    """(mono float32 samples, sample rate, full duration|None)."""
    if ext_of(path) in SOUNDFILE_EXTS:
        try:
            return _decode_with_soundfile(path)
        except FingerprintError:
            raise
        except Exception:
            if not _ffmpeg():
                raise
    return _decode_with_ffmpeg(path)


def chroma_grid_bits(chroma):
    """
    12 x T chroma matrix -> 192 booleans.

    The clip is cut into AUDIO_SEGMENTS time slices; each pitch class in a
    slice becomes one bit, set when it is above that slice's median. The
    result survives re-encoding, bitrate and volume changes, and unrelated
    tracks land around half the bits apart.
    """
    if chroma.shape[1] < AUDIO_SEGMENTS:
        raise FingerprintError("audio too short to fingerprint")
    segments = np.array_split(chroma, AUDIO_SEGMENTS, axis=1)
    grid = np.stack([seg.mean(axis=1) for seg in segments], axis=1)   # 12 x S
    medians = np.median(grid, axis=0, keepdims=True)
    return (grid > medians).T.reshape(-1)                              # S*12


def bits_to_hex(bits):
    value = 0
    for bit in bits:
        value = (value << 1) | int(bool(bit))
    width = (len(bits) + 3) // 4
    return f"{value:0{width}x}"


def audio_hash(path):
    """(chroma_grid_hex, duration_seconds|None)."""
    librosa = _get_librosa()
    y, sr, duration = decode_audio(path)
    if y.size == 0 or float(np.max(np.abs(y))) < 1e-4:
        raise FingerprintError("audio is silent")
    if sr != AUDIO_SR:
        import soxr
        y = soxr.resample(y, sr, AUDIO_SR, quality='LQ')
    # tuning=0 skips librosa's pitch-tracking tuning estimate, which costs
    # more than the chroma itself and adds noise between near-identical files.
    chroma = librosa.feature.chroma_stft(y=y, sr=AUDIO_SR, n_fft=2048,
                                         hop_length=1024, tuning=0.0)
    if duration is None:
        duration = y.size / AUDIO_SR
    return bits_to_hex(chroma_grid_bits(chroma)), duration


# ---------------------------------------------------------------------------
# Text
# ---------------------------------------------------------------------------

_BIT_SHIFTS = np.arange(64, dtype=np.uint64)


def simhash(tokens):
    """64-bit SimHash of a token list, or None when there is nothing to hash."""
    if not tokens:
        return None
    if len(tokens) >= 8:
        shingles = [" ".join(tokens[i:i + 3]) for i in range(len(tokens) - 2)]
    else:
        shingles = tokens
    shingles = shingles[:TEXT_MAX_SHINGLES]
    hashes = np.fromiter(
        (int.from_bytes(hashlib.blake2b(s.encode('utf-8', 'ignore'), digest_size=8).digest(), 'big')
         for s in shingles),
        dtype=np.uint64, count=len(shingles),
    )
    bits = (hashes[:, None] >> _BIT_SHIFTS) & np.uint64(1)
    votes = bits.sum(axis=0, dtype=np.int64) * 2 - len(shingles)
    value = 0
    for position in range(63, -1, -1):
        value = (value << 1) | int(votes[position] > 0)
    return value


def text_hash(path):
    with open(path, 'rb') as handle:
        raw = handle.read(TEXT_READ_BYTES)
    if b"\x00" in raw[:8192]:
        raise FingerprintError("looks binary, not text")
    text = raw.decode('utf-8', errors='ignore').lower()
    value = simhash(_WORD_RE.findall(text))
    if value is None:
        return None
    return f"{value:016x}"

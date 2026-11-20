import hashlib

import pytest
from conftest import (
    make_audio,
    make_image,
    make_text,
    make_video,
    requires_ffmpeg,
    requires_librosa,
)

from fdf import fingerprints as fp
from fdf.exts import AUDIO_EXTS, IMAGE_EXTS, KINDS, TEXT_EXTS, VIDEO_EXTS, exts_for, kind_of


def hamming(a, b):
    return bin(int(a, 16) ^ int(b, 16)).count("1")


def test_extension_tables_do_not_overlap():
    sets = [IMAGE_EXTS, VIDEO_EXTS, AUDIO_EXTS, TEXT_EXTS]
    for i, a in enumerate(sets):
        for b in sets[i + 1:]:
            assert not (a & b)
    assert kind_of(".ts") == "video"
    assert kind_of(".JPG".lower()) == "image"
    assert kind_of(".zip") == "other"
    assert exts_for(set(KINDS)) == IMAGE_EXTS | VIDEO_EXTS | AUDIO_EXTS | TEXT_EXTS


def test_exact_and_quick_hash(tmp_path):
    small = tmp_path / "small.bin"
    small.write_bytes(b"abc" * 100)
    assert fp.exact_hash(str(small)) == hashlib.md5(b"abc" * 100).hexdigest()
    # Small files are read whole, so the quick hash equals the exact hash.
    assert fp.quick_hash(str(small), small.stat().st_size) == fp.exact_hash(str(small))

    big_a = tmp_path / "a.bin"
    big_b = tmp_path / "b.bin"
    payload = bytes(range(256)) * 2048
    big_a.write_bytes(payload)
    big_b.write_bytes(payload[:300_000] + b"X" + payload[300_001:])
    size = big_a.stat().st_size
    # The difference is in the middle, so the quick hash cannot see it...
    assert fp.quick_hash(str(big_a), size) == fp.quick_hash(str(big_b), size)
    # ...and the exact hash must.
    assert fp.exact_hash(str(big_a)) != fp.exact_hash(str(big_b))


def test_image_hash_tolerates_recompression_and_resize(tmp_path):
    a = make_image(tmp_path / "a.jpg", quality=95)
    b = make_image(tmp_path / "b.jpg", quality=50)
    c = make_image(tmp_path / "c.png", size=(600, 450))
    other = make_image(tmp_path / "o.jpg", seed=99)
    ha, hb, hc, ho = (fp.image_hash(str(p)) for p in (a, b, c, other))
    assert hamming(ha, hb) <= 4
    assert hamming(ha, hc) <= 4
    assert hamming(ha, ho) > 12


def test_image_hash_applies_exif_orientation(tmp_path):
    from PIL import Image
    upright = make_image(tmp_path / "upright.jpg")
    with Image.open(upright) as img:
        rotated = img.rotate(90, expand=True)
        exif = Image.Exif()
        exif[0x0112] = 6   # display rotated 90 CW, which undoes the rotate above
        rotated.save(tmp_path / "tagged.jpg", exif=exif, quality=90)
    assert hamming(fp.image_hash(str(upright)), fp.image_hash(str(tmp_path / "tagged.jpg"))) <= 4


def test_image_hash_raises_on_garbage(tmp_path):
    bad = tmp_path / "bad.png"
    bad.write_text("not a png")
    with pytest.raises(OSError):        # PIL.UnidentifiedImageError
        fp.image_hash(str(bad))


def test_simhash_near_and_far(tmp_path):
    a = make_text(tmp_path / "a.txt")
    b = make_text(tmp_path / "b.txt", edit=lambda t: t.replace("w1 ", "zz ", 5) + " tail")
    c = make_text(tmp_path / "c.txt", seed=77)
    ha, hb, hc = (fp.text_hash(str(p)) for p in (a, b, c))
    assert hamming(ha, hb) <= 6
    assert hamming(ha, hc) > 16


def test_text_hash_empty_and_binary(tmp_path):
    empty = tmp_path / "e.txt"
    empty.write_text("  \n\t ")
    assert fp.text_hash(str(empty)) is None
    binary = tmp_path / "b.txt"
    binary.write_bytes(b"\x00\x01\x02" * 100)
    with pytest.raises(fp.FingerprintError):
        fp.text_hash(str(binary))


def test_video_hash_survives_rescale(tmp_path):
    a = make_video(tmp_path / "a.mp4")
    b = make_video(tmp_path / "b.mp4", size=(160, 120))
    c = make_video(tmp_path / "c.mp4", seed=11)
    (ha, da), (hb, db), (hc, _) = (fp.video_hash(str(p)) for p in (a, b, c))
    assert hamming(ha, hb) <= 4
    assert hamming(ha, hc) > 10
    assert da == pytest.approx(3.0, abs=0.2)
    assert db == pytest.approx(da, abs=0.2)


def test_video_hash_rejects_non_video(tmp_path):
    bad = tmp_path / "bad.mp4"
    bad.write_bytes(b"0101010101")
    with pytest.raises(fp.FingerprintError):
        fp.video_hash(str(bad))


def test_chroma_bits_shape():
    import numpy as np
    chroma = np.random.default_rng(0).random((12, 200))
    bits = fp.chroma_grid_bits(chroma)
    assert bits.shape == (fp.AUDIO_BITS,)
    assert len(fp.bits_to_hex(bits)) == fp.AUDIO_BITS // 4
    with pytest.raises(fp.FingerprintError):
        fp.chroma_grid_bits(chroma[:, :5])


@requires_librosa
def test_audio_hash_same_song_different_encoding(tmp_path):
    a = make_audio(tmp_path / "a.wav")
    b = make_audio(tmp_path / "b.flac", sr=44100, gain=0.5)
    c = make_audio(tmp_path / "c.wav", seed=9)
    (ha, da), (hb, db), (hc, _) = (fp.audio_hash(str(p)) for p in (a, b, c))
    assert hamming(ha, hb) <= 12
    assert hamming(ha, hc) > 50
    assert da == pytest.approx(30.0, abs=0.1)
    assert db == pytest.approx(30.0, abs=0.1)


@requires_librosa
def test_audio_hash_rejects_silence(tmp_path):
    import numpy as np
    import soundfile as sf
    path = tmp_path / "silence.wav"
    sf.write(path, np.zeros(22050 * 5, dtype="float32"), 22050)
    with pytest.raises(fp.FingerprintError):
        fp.audio_hash(str(path))


@requires_librosa
@requires_ffmpeg
def test_audio_hash_via_ffmpeg(tmp_path):
    import subprocess
    wav = make_audio(tmp_path / "a.wav")
    m4a = tmp_path / "a.m4a"
    subprocess.run(["ffmpeg", "-loglevel", "error", "-y", "-i", str(wav), "-c:a", "aac",
                    str(m4a)], check=True)
    (ha, _), (hb, db) = fp.audio_hash(str(wav)), fp.audio_hash(str(m4a))
    assert hamming(ha, hb) <= 12
    assert db == pytest.approx(30.0, abs=0.5)

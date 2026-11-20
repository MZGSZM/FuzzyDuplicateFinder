"""
Background preview loading.

Decoding a large photo or seeking into a video used to happen on the UI
thread, which froze the window for a moment on every selection. Previews now
decode on a QThreadPool and arrive by signal; a request token per panel drops
results that a newer selection has already superseded.

QImage is safe to build off the UI thread. QPixmap is not, so the conversion
happens in the receiving slot.
"""

from __future__ import annotations

import os
from datetime import datetime

from PyQt6.QtCore import QObject, QRunnable, QSize, pyqtSignal
from PyQt6.QtGui import QImage, QImageReader

from .exts import SOUNDFILE_EXTS, ext_of, kind_of

PREVIEW_MAX_DIM = 2400
TEXT_PREVIEW_BYTES = 16 * 1024
TEXT_PREVIEW_LINES = 80


class PreviewSignals(QObject):
    # token, panel key, QImage or None, info dict
    ready = pyqtSignal(int, str, object, dict)


def _fmt_duration(seconds):
    seconds = int(round(seconds))
    hours, rest = divmod(seconds, 3600)
    mins, secs = divmod(rest, 60)
    return f"{hours}:{mins:02d}:{secs:02d}" if hours else f"{mins}:{secs:02d}"


def _load_image(path, max_dim):
    reader = QImageReader(path)
    reader.setAutoTransform(True)
    source = reader.size()
    res = f"{source.width()}x{source.height()}" if source.isValid() else ""
    if source.isValid():
        longest = max(source.width(), source.height())
        if longest > max_dim:
            scale = max_dim / longest
            reader.setScaledSize(QSize(max(1, int(source.width() * scale)),
                                       max(1, int(source.height() * scale))))
    image = reader.read()
    if not image.isNull():
        return image, res

    # Qt has no plugin for some formats Pillow reads (PSD, AVIF on some
    # builds). Fall back to Pillow rather than showing nothing.
    from PIL import Image, ImageOps
    with Image.open(path) as img:
        img.draft('RGB', (max_dim, max_dim))
        img = ImageOps.exif_transpose(img)
        res = res or f"{img.width}x{img.height}"
        img.thumbnail((max_dim, max_dim))
        rgba = img.convert('RGBA')
        data = rgba.tobytes('raw', 'RGBA')
        qimg = QImage(data, rgba.width, rgba.height, rgba.width * 4,
                      QImage.Format.Format_RGBA8888)
        return qimg.copy(), res


def _load_video(path, info):
    os.environ.setdefault("OPENCV_LOG_LEVEL", "OFF")
    import cv2
    cap = cv2.VideoCapture(path)
    try:
        if not cap.isOpened():
            info['placeholder'] = "Video File"
            return None
        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        if width and height:
            info['res'] = f"{width}x{height}"
        fps = cap.get(cv2.CAP_PROP_FPS)
        frames = cap.get(cv2.CAP_PROP_FRAME_COUNT)
        if fps > 0 and frames > 0:
            info['extra'] = f"Duration: {_fmt_duration(frames / fps)}"
            cap.set(cv2.CAP_PROP_POS_FRAMES, int(frames // 3))
        ok, frame = cap.read()
        if not ok:
            cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
            ok, frame = cap.read()
        if not ok:
            info['placeholder'] = "No Preview"
            return None
        rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        h, w, channels = rgb.shape
        return QImage(rgb.data, w, h, channels * w, QImage.Format.Format_RGB888).copy()
    finally:
        cap.release()


def _audio_info(path, info):
    info['placeholder'] = "Audio File"
    if ext_of(path) in SOUNDFILE_EXTS:
        try:
            import soundfile as sf
            meta = sf.info(path)
            info['extra'] = (f"Duration: {_fmt_duration(meta.duration)}  |  "
                             f"{meta.samplerate} Hz, {meta.channels} ch")
        except Exception:
            pass


def _text_snippet(path, info):
    with open(path, 'rb') as handle:
        raw = handle.read(TEXT_PREVIEW_BYTES)
    lines = raw.decode('utf-8', errors='replace').splitlines()[:TEXT_PREVIEW_LINES]
    info['text'] = "\n".join(lines)


class PreviewTask(QRunnable):
    def __init__(self, signals, token, key, path, max_dim=PREVIEW_MAX_DIM):
        super().__init__()
        self.signals = signals
        self.token = token
        self.key = key
        self.path = path
        self.max_dim = max_dim
        self.setAutoDelete(True)

    def run(self):
        info = {'path': self.path}
        image = None
        try:
            st = os.stat(self.path)
            info['size'] = st.st_size
            info['mtime'] = datetime.fromtimestamp(st.st_mtime).strftime('%Y-%m-%d %H:%M')
            info['ctime'] = datetime.fromtimestamp(st.st_ctime).strftime('%Y-%m-%d %H:%M')
            ext = ext_of(self.path)
            kind = kind_of(ext)
            info['kind'] = kind
            if kind == 'image':
                image, info['res'] = _load_image(self.path, self.max_dim)
                if image is None or image.isNull():
                    image = None
                    info['placeholder'] = "Image Error"
            elif kind == 'video':
                image = _load_video(self.path, info)
            elif kind == 'audio':
                _audio_info(self.path, info)
            elif kind == 'text':
                _text_snippet(self.path, info)
            else:
                info['placeholder'] = f"{ext.upper().lstrip('.') or 'NO EXT'} File"
        except FileNotFoundError:
            info['missing'] = True
        except Exception as exc:
            info['error'] = str(exc) or type(exc).__name__
        try:
            self.signals.ready.emit(self.token, self.key, image, info)
        except RuntimeError:
            pass    # window closed while decoding

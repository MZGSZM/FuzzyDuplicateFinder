"""
File type tables. This is the only place extensions are defined; the scanner,
matcher and UI all import from here so they cannot drift apart.
"""

from __future__ import annotations

import os

IMAGE_EXTS = frozenset({
    '.jpg', '.jpeg', '.png', '.bmp', '.gif', '.webp', '.tiff', '.tif', '.psd',
})

# Only treated as images when the installed Pillow can actually open them.
OPTIONAL_IMAGE_EXTS = frozenset({'.avif', '.heic', '.heif', '.jxl', '.jp2'})

VIDEO_EXTS = frozenset({
    '.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv', '.m4v', '.webm', '.ts',
    '.mts', '.m2ts', '.3gp', '.mpg', '.mpeg', '.vob',
})

AUDIO_EXTS = frozenset({
    '.mp3', '.wav', '.flac', '.m4a', '.aac', '.ogg', '.oga', '.opus', '.wma',
    '.aiff', '.aif',
})

# Formats libsndfile decodes directly. Everything else in AUDIO_EXTS needs
# ffmpeg on PATH.
SOUNDFILE_EXTS = frozenset({
    '.mp3', '.wav', '.flac', '.ogg', '.oga', '.opus', '.aiff', '.aif',
})

# .ts is deliberately absent: it is MPEG transport stream far more often than
# TypeScript on the machines this tool gets pointed at.
TEXT_EXTS = frozenset({
    '.txt', '.md', '.rst', '.py', '.js', '.json', '.html', '.htm', '.css',
    '.c', '.h', '.cpp', '.hpp', '.cs', '.java', '.kt', '.rs', '.go', '.rb',
    '.php', '.pl', '.lua', '.sh', '.bat', '.ps1', '.sql', '.xml', '.yml',
    '.yaml', '.toml', '.ini', '.cfg', '.conf', '.csv', '.tsv', '.log',
    '.srt', '.vtt', '.tex',
})

KINDS = ('image', 'video', 'audio', 'text')

# Index column holding each kind's fuzzy fingerprint.
KIND_COLUMNS = {'image': 'visual_hash', 'video': 'visual_hash',
                'audio': 'audio_hash', 'text': 'text_hash'}


def _pillow_extras():
    try:
        from PIL import Image
        registered = set(Image.registered_extensions())
    except Exception:
        return frozenset()
    return frozenset(OPTIONAL_IMAGE_EXTS & registered)


IMAGE_EXTS = IMAGE_EXTS | _pillow_extras()

_KIND_BY_EXT = {}
for _ext in IMAGE_EXTS:
    _KIND_BY_EXT[_ext] = 'image'
for _ext in VIDEO_EXTS:
    _KIND_BY_EXT[_ext] = 'video'
for _ext in AUDIO_EXTS:
    _KIND_BY_EXT[_ext] = 'audio'
for _ext in TEXT_EXTS:
    _KIND_BY_EXT[_ext] = 'text'

ALL_MEDIA_EXTS = frozenset(_KIND_BY_EXT)


def ext_of(path):
    return os.path.splitext(path)[1].lower()


def kind_of(ext):
    """'image', 'video', 'audio', 'text', or 'other'."""
    return _KIND_BY_EXT.get(ext, 'other')


def exts_for(kinds):
    """Union of the extension sets for the given kind names."""
    return frozenset(ext for ext, kind in _KIND_BY_EXT.items() if kind in kinds)

#!/usr/bin/env bash
# Wrap the PyInstaller one-folder build in an AppImage.
#
#   bash .github/scripts/build-appimage.sh VERSION
#
# Expects dist/FuzzyDuplicateFinder/ from `pyinstaller FuzzyDuplicateFinder.spec`
# and writes dist/FuzzyDuplicateFinder-VERSION-x86_64.AppImage.
#
# appimagetool comes from its "continuous" release, so builds always use the
# current tool. Set APPIMAGETOOL to a local copy to skip the download.

set -euo pipefail
export LC_ALL=C

APP=FuzzyDuplicateFinder
APP_ID=fuzzy-duplicate-finder
ARCH="${ARCH:-x86_64}"
TOOL_URL="https://github.com/AppImage/appimagetool/releases/download/continuous/appimagetool-${ARCH}.AppImage"
SIZES=(16 24 32 48 64 128 256 512)

die() {
    printf 'build-appimage: %s\n' "$*" >&2
    exit 1
}

need() {
    local missing=() cmd
    for cmd in "$@"; do
        command -v "$cmd" >/dev/null 2>&1 || missing+=("$cmd")
    done
    if [ "${#missing[@]}" -gt 0 ]; then
        die "missing required commands: ${missing[*]}"
    fi
}

main() {
    [ "$#" -eq 1 ] || die "usage: $0 VERSION"
    local version="$1"
    need cp mkdir chmod mktemp rm
    [ -d "dist/$APP" ] || die "dist/$APP not found; run pyinstaller first"

    local work appdir tool
    work=$(mktemp -d "${TMPDIR:-/tmp}/appimage.XXXXXX")
    # shellcheck disable=SC2064  # expand now
    trap "rm -rf -- '$work'" EXIT
    appdir="$work/$APP.AppDir"

    mkdir -p "$appdir/usr/lib" "$appdir/usr/share/applications"
    cp -a "dist/$APP" "$appdir/usr/lib/$APP"
    cp packaging/appimage/AppRun "$appdir/AppRun"
    # The repo's file modes do not survive a web upload, so set them here.
    chmod 755 "$appdir/AppRun" "$appdir/usr/lib/$APP/$APP"

    cp "assets/$APP_ID.desktop" "$appdir/$APP_ID.desktop"
    cp "assets/$APP_ID.desktop" "$appdir/usr/share/applications/$APP_ID.desktop"
    cp assets/icons/icon-256.png "$appdir/$APP_ID.png"
    cp assets/icons/icon-256.png "$appdir/.DirIcon"
    local size
    for size in "${SIZES[@]}"; do
        mkdir -p "$appdir/usr/share/icons/hicolor/${size}x${size}/apps"
        cp "assets/icons/icon-$size.png" \
            "$appdir/usr/share/icons/hicolor/${size}x${size}/apps/$APP_ID.png"
    done
    mkdir -p "$appdir/usr/share/icons/hicolor/scalable/apps"
    cp assets/icon.svg "$appdir/usr/share/icons/hicolor/scalable/apps/$APP_ID.svg"

    if [ -n "${APPIMAGETOOL:-}" ]; then
        tool="$APPIMAGETOOL"
    else
        need curl
        tool="$work/appimagetool"
        curl -fsSL --retry 3 -o "$tool" "$TOOL_URL"
    fi
    chmod 755 "$tool"

    local out="dist/$APP-$version-$ARCH.AppImage"
    rm -f -- "$out"
    # Runners have no FUSE; extract-and-run avoids needing it.
    APPIMAGE_EXTRACT_AND_RUN=1 ARCH="$ARCH" "$tool" --no-appstream "$appdir" "$out"
    chmod 755 "$out"
    printf 'Built %s\n' "$out"
}

main "$@"

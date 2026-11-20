#!/usr/bin/env bash
# Install a menu entry and icons for Fuzzy Duplicate Finder for the current
# user, pointing at the AppImage (or any build of the program) you give it.
#
# Wayland compositors pick a window's taskbar icon from its desktop entry
# (matched by the app id the program sets), not from the icon the program
# embeds, so without this the app shows a generic icon on Wayland.
#
# Usage:
#   tools/install_linux_desktop_entry.sh /path/to/FuzzyDuplicateFinder-x.y.z-x86_64.AppImage
#   tools/install_linux_desktop_entry.sh --uninstall

set -euo pipefail

APP_ID="fuzzy-duplicate-finder"
SIZES=(16 24 32 48 64 128 256 512)

die() {
    printf '%s\n' "$*" >&2
    exit 1
}

need() {
    local missing=() cmd
    for cmd in "$@"; do
        command -v "$cmd" >/dev/null 2>&1 || missing+=("$cmd")
    done
    if [ "${#missing[@]}" -gt 0 ]; then
        die "Missing required commands: ${missing[*]}"
    fi
}

need cp chmod mkdir rm sed dirname basename

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd) || exit 1
ASSETS="$(dirname "$SCRIPT_DIR")/assets"
DATA_HOME="${XDG_DATA_HOME:-$HOME/.local/share}"
APPS_DIR="$DATA_HOME/applications"
ICON_ROOT="$DATA_HOME/icons/hicolor"

refresh_caches() {
    if command -v update-desktop-database >/dev/null 2>&1; then
        update-desktop-database "$APPS_DIR" || true
    fi
    if command -v gtk-update-icon-cache >/dev/null 2>&1; then
        gtk-update-icon-cache -q -t "$ICON_ROOT" || true
    fi
}

uninstall() {
    local size
    rm -f -- "$APPS_DIR/$APP_ID.desktop"
    for size in "${SIZES[@]}"; do
        rm -f -- "$ICON_ROOT/${size}x${size}/apps/$APP_ID.png"
    done
    rm -f -- "$ICON_ROOT/scalable/apps/$APP_ID.svg"
    refresh_caches
    printf 'Removed the desktop entry and icons.\n'
}

install_entry() {
    local target="$1" dir binary size
    [ -f "$target" ] || die "Not a file: $target"
    dir=$(cd "$(dirname "$target")" && pwd) || exit 1
    binary="$dir/$(basename "$target")"
    chmod +x -- "$binary" || die "Cannot make $binary executable"

    mkdir -p "$APPS_DIR" "$ICON_ROOT/scalable/apps"
    for size in "${SIZES[@]}"; do
        mkdir -p "$ICON_ROOT/${size}x${size}/apps"
        cp -- "$ASSETS/icons/icon-${size}.png" "$ICON_ROOT/${size}x${size}/apps/$APP_ID.png"
    done
    cp -- "$ASSETS/icon.svg" "$ICON_ROOT/scalable/apps/$APP_ID.svg"

    # Quote the Exec path per the desktop entry spec in case it has spaces.
    # | is the sed delimiter, so escape any in the path, plus & and \.
    local escaped
    escaped=$(printf '%s' "$binary" | sed -e 's/[\\&|]/\\&/g')
    sed "s|^Exec=.*|Exec=\"$escaped\"|" "$ASSETS/$APP_ID.desktop" >"$APPS_DIR/$APP_ID.desktop"
    chmod 644 "$APPS_DIR/$APP_ID.desktop"

    refresh_caches
    printf 'Installed %s -> %s\n' "$APPS_DIR/$APP_ID.desktop" "$binary"
}

case "${1:-}" in
    --uninstall)
        uninstall
        ;;
    ""|-h|--help)
        die "Usage: $0 /path/to/FuzzyDuplicateFinder.AppImage | --uninstall"
        ;;
    *)
        [ "$#" -eq 1 ] || die "Usage: $0 /path/to/FuzzyDuplicateFinder.AppImage | --uninstall"
        install_entry "$1"
        ;;
esac

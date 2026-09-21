#!/usr/bin/env bash
# Install a desktop entry and icons for Fuzzy Duplicate Finder for the
# current user, so it shows up in the application menu and gets the right
# taskbar icon.
#
# Wayland compositors choose a window's taskbar icon from its desktop entry
# (matched by the app id the program sets), not from the icon the program
# embeds, so without this the binary shows a generic icon on Wayland.
#
# Usage:
#   tools/install_linux_desktop_entry.sh /path/to/FuzzyDuplicateFinder
#   tools/install_linux_desktop_entry.sh --uninstall

set -euo pipefail

APP_ID="fuzzy-duplicate-finder"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
ASSETS="$REPO_DIR/assets"

DATA_HOME="${XDG_DATA_HOME:-$HOME/.local/share}"
APPS_DIR="$DATA_HOME/applications"
ICON_ROOT="$DATA_HOME/icons/hicolor"
SIZES=(16 24 32 48 64 128 256 512)

refresh_caches() {
    command -v update-desktop-database >/dev/null 2>&1 && update-desktop-database "$APPS_DIR" || true
    command -v gtk-update-icon-cache >/dev/null 2>&1 && gtk-update-icon-cache -q -t "$ICON_ROOT" || true
}

if [[ "${1:-}" == "--uninstall" ]]; then
    rm -f "$APPS_DIR/$APP_ID.desktop"
    for size in "${SIZES[@]}"; do
        rm -f "$ICON_ROOT/${size}x${size}/apps/$APP_ID.png"
    done
    rm -f "$ICON_ROOT/scalable/apps/$APP_ID.svg"
    refresh_caches
    echo "Removed desktop entry and icons."
    exit 0
fi

if [[ $# -ne 1 ]]; then
    echo "Usage: $0 /path/to/FuzzyDuplicateFinder | --uninstall" >&2
    exit 1
fi

BINARY="$(realpath "$1")"
if [[ ! -x "$BINARY" ]]; then
    echo "Not an executable file: $BINARY" >&2
    exit 1
fi

mkdir -p "$APPS_DIR" "$ICON_ROOT/scalable/apps"
for size in "${SIZES[@]}"; do
    mkdir -p "$ICON_ROOT/${size}x${size}/apps"
    install -m 644 "$ASSETS/icons/icon-${size}.png" "$ICON_ROOT/${size}x${size}/apps/$APP_ID.png"
done
install -m 644 "$ASSETS/icon.svg" "$ICON_ROOT/scalable/apps/$APP_ID.svg"

# Quote the Exec path per the desktop entry spec in case it contains spaces.
sed "s|^Exec=.*|Exec=\"$BINARY\"|" "$ASSETS/$APP_ID.desktop" > "$APPS_DIR/$APP_ID.desktop"
chmod 644 "$APPS_DIR/$APP_ID.desktop"

refresh_caches
echo "Installed $APPS_DIR/$APP_ID.desktop -> $BINARY"

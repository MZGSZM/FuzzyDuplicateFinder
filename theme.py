import sys

from PyQt6.QtCore import QObject, QSettings, Qt, pyqtSignal, pyqtSlot
from PyQt6.QtGui import QColor, QPalette

try:
    from PyQt6.QtDBus import QDBusConnection, QDBusInterface, QDBusMessage
    _HAVE_DBUS = True
except ImportError:  # Windows and macOS wheels
    _HAVE_DBUS = False


MODES = ("system", "light", "dark")
MODE_LABELS = {"system": "System", "light": "Light", "dark": "Dark"}

THEMES = {
    "dark": {
        "window":         "#1e1e1e",
        "surface":        "#252526",
        "surface_alt":    "#2b2b2c",
        "input":          "#1f1f1f",
        "preview":        "#141414",
        "border":         "#3c3c3c",
        "divider":        "#303030",
        "text":           "#e6e6e6",
        "text_muted":     "#b3b3b3",
        "text_faint":     "#8c8c8c",
        "accent":         "#007acc",
        "accent_hover":   "#006bb3",
        "accent_pressed": "#005a96",
        "on_accent":      "#ffffff",
        "danger":         "#d32f2f",
        "danger_hover":   "#bd2828",
        "danger_pressed": "#a02222",
        "on_danger":      "#ffffff",
        "neutral":        "#3a3d41",
        "neutral_hover":  "#4a4e54",
        "neutral_pressed": "#2f3236",
        "on_neutral":     "#f0f0f0",
        "disabled_bg":    "#2d2d2d",
        "disabled_text":  "#6e6e6e",
        "success":        "#5cc162",
        "warning_text":   "#ff6b61",
        "selection":      "#37373d",
        "link":           "#4aa8ff",
        "progress_track": "#2a2a2a",
        "tooltip":        "#333337",
    },
    "light": {
        "window":         "#eef0f3",
        "surface":        "#ffffff",
        "surface_alt":    "#f6f7f9",
        "input":          "#ffffff",
        "preview":        "#e6e9ed",
        "border":         "#c5cbd3",
        "divider":        "#e1e4e8",
        "text":           "#1b1f24",
        "text_muted":     "#4b5563",
        "text_faint":     "#5f6773",
        "accent":         "#0063b1",
        "accent_hover":   "#00548f",
        "accent_pressed": "#004575",
        "on_accent":      "#ffffff",
        "danger":         "#c62828",
        "danger_hover":   "#a82121",
        "danger_pressed": "#8e1b1b",
        "on_danger":      "#ffffff",
        "neutral":        "#e3e6ea",
        "neutral_hover":  "#d4d8de",
        "neutral_pressed": "#c6cbd2",
        "on_neutral":     "#1b1f24",
        "disabled_bg":    "#e6e8eb",
        "disabled_text":  "#8d949e",
        "success":        "#2e7d32",
        "warning_text":   "#b3261e",
        "selection":      "#dcebfb",
        "link":           "#0063b1",
        "progress_track": "#dde1e6",
        "tooltip":        "#ffffff",
    },
}


def build_palette(t):
    """QPalette for native-drawn elements: menus, dialogs, scrollbars, focus."""
    p = QPalette()
    c = QColor
    R = QPalette.ColorRole

    p.setColor(R.Window, c(t["window"]))
    p.setColor(R.WindowText, c(t["text"]))
    p.setColor(R.Base, c(t["input"]))
    p.setColor(R.AlternateBase, c(t["surface_alt"]))
    p.setColor(R.ToolTipBase, c(t["tooltip"]))
    p.setColor(R.ToolTipText, c(t["text"]))
    p.setColor(R.PlaceholderText, c(t["text_faint"]))
    p.setColor(R.Text, c(t["text"]))
    p.setColor(R.Button, c(t["neutral"]))
    p.setColor(R.ButtonText, c(t["on_neutral"]))
    p.setColor(R.BrightText, c(t["warning_text"]))
    p.setColor(R.Highlight, c(t["accent"]))
    p.setColor(R.HighlightedText, c(t["on_accent"]))
    p.setColor(R.Link, c(t["link"]))
    p.setColor(R.LinkVisited, c(t["link"]))
    p.setColor(R.Light, c(t["surface"]).lighter(115))
    p.setColor(R.Midlight, c(t["surface_alt"]))
    p.setColor(R.Mid, c(t["border"]))
    p.setColor(R.Dark, c(t["border"]).darker(130))
    p.setColor(R.Shadow, QColor(0, 0, 0))

    disabled = QPalette.ColorGroup.Disabled
    for role in (R.Text, R.WindowText, R.ButtonText):
        p.setColor(disabled, role, c(t["disabled_text"]))
    p.setColor(disabled, R.Button, c(t["disabled_bg"]))
    p.setColor(disabled, R.Highlight, c(t["border"]))
    return p


def build_stylesheet(t):
    return f"""
/* ---- base ---------------------------------------------------------- */
QMainWindow, QDialog {{ background-color: {t['window']}; color: {t['text']}; }}
QWidget {{ color: {t['text']}; }}
QToolTip {{
    background-color: {t['tooltip']}; color: {t['text']};
    border: 1px solid {t['border']}; padding: 4px 6px;
}}

QMenuBar {{ background-color: {t['surface']}; color: {t['text']};
            border-bottom: 1px solid {t['border']}; }}
QMenuBar::item {{ background: transparent; padding: 4px 10px; }}
QMenuBar::item:selected {{ background-color: {t['neutral']}; }}
QMenu {{ background-color: {t['surface']}; color: {t['text']};
         border: 1px solid {t['border']}; padding: 4px 0; }}
QMenu::item {{ padding: 5px 24px 5px 24px; }}
QMenu::item:selected {{ background-color: {t['selection']}; color: {t['text']}; }}
QMenu::separator {{ height: 1px; background: {t['divider']}; margin: 4px 8px; }}

QStatusBar {{ background-color: {t['surface']}; color: {t['text_muted']};
              border-top: 1px solid {t['border']}; }}
QStatusBar::item {{ border: none; }}

QSplitter::handle {{ background-color: {t['border']}; }}

/* ---- toolbar ------------------------------------------------------- */
QFrame#toolbar {{
    background-color: {t['surface']};
    border: 1px solid {t['border']}; border-radius: 6px;
}}

/* ---- buttons --------------------------------------------------------
   Unstyled buttons (message boxes, dialogs) get the neutral treatment so
   they stay recognisable as buttons in both themes. */
QPushButton {{
    background-color: {t['neutral']}; color: {t['on_neutral']};
    border: 1px solid {t['border']}; border-radius: 4px; padding: 5px 14px;
}}
QPushButton:hover {{ background-color: {t['neutral_hover']}; }}
QPushButton:pressed {{ background-color: {t['neutral_pressed']}; }}
QPushButton:disabled {{ background-color: {t['disabled_bg']}; color: {t['disabled_text']};
                        border-color: {t['disabled_bg']}; }}
QPushButton:focus {{ outline: none; }}

QPushButton[variant="primary"], QPushButton[variant="danger"],
QPushButton[variant="neutral"] {{
    padding: 8px 16px; font-weight: bold; border: none;
}}
QPushButton[variant="primary"] {{ background-color: {t['accent']}; color: {t['on_accent']}; }}
QPushButton[variant="primary"]:hover {{ background-color: {t['accent_hover']}; }}
QPushButton[variant="primary"]:pressed {{ background-color: {t['accent_pressed']}; }}

QPushButton[variant="danger"] {{ background-color: {t['danger']}; color: {t['on_danger']}; }}
QPushButton[variant="danger"]:hover {{ background-color: {t['danger_hover']}; }}
QPushButton[variant="danger"]:pressed {{ background-color: {t['danger_pressed']}; }}

QPushButton[variant="neutral"] {{ background-color: {t['neutral']}; color: {t['on_neutral']}; }}
QPushButton[variant="neutral"]:hover {{ background-color: {t['neutral_hover']}; }}
QPushButton[variant="neutral"]:pressed {{ background-color: {t['neutral_pressed']}; }}

QPushButton[variant="primary"]:disabled, QPushButton[variant="danger"]:disabled,
QPushButton[variant="neutral"]:disabled {{
    background-color: {t['disabled_bg']}; color: {t['disabled_text']};
}}

QPushButton[variant="arrow"] {{
    background-color: {t['neutral']}; color: {t['on_neutral']};
    font-size: 9px; font-weight: bold; padding: 0;
    border: 1px solid {t['border']}; border-radius: 3px;
    min-width: 18px; max-width: 18px; min-height: 16px; max-height: 16px;
}}
QPushButton[variant="arrow"]:hover {{ background-color: {t['accent']}; color: {t['on_accent']};
                                      border-color: {t['accent']}; }}
QPushButton[variant="arrow"]:pressed {{ background-color: {t['accent_pressed']};
                                        color: {t['on_accent']}; }}

QPushButton[variant="link"] {{
    background: transparent; color: {t['link']}; border: none;
    padding: 0; text-align: left;
}}
QPushButton[variant="link"]:hover {{ text-decoration: underline; background: transparent; }}

QPushButton[variant="warn-link"] {{
    background: transparent; color: {t['warning_text']}; border: none;
    font-weight: bold; text-decoration: underline; padding: 0 6px; text-align: left;
}}
QPushButton[variant="warn-link"]:hover {{ background: transparent; color: {t['danger_hover']}; }}

/* ---- combo box ----------------------------------------------------- */
QComboBox {{
    background-color: {t['input']}; color: {t['text']};
    border: 1px solid {t['border']}; border-radius: 4px; padding: 3px 8px;
    min-width: 72px;
}}
QComboBox:hover {{ border-color: {t['accent']}; }}
QComboBox QAbstractItemView {{
    background-color: {t['surface']}; color: {t['text']};
    border: 1px solid {t['border']};
    selection-background-color: {t['selection']}; selection-color: {t['text']};
}}

/* ---- labels -------------------------------------------------------- */
QLabel[role="muted"] {{ color: {t['text_muted']}; }}
QLabel[role="status"] {{ color: {t['text_muted']}; font-weight: bold; margin-left: 10px; }}
QLabel[role="version"] {{ color: {t['text_faint']}; font-size: 10px; margin-right: 10px; }}
QLabel[role="version"]:hover {{ color: {t['link']}; }}
QLabel[role="stepper"] {{ color: {t['text']}; font-weight: bold; min-width: 24px; }}
QLabel[role="panelTitle"] {{ color: {t['text_muted']}; font-weight: bold; }}
QLabel[role="filename"] {{ color: {t['text']}; font-size: 14px; font-weight: bold; }}
QLabel[role="path"] {{ color: {t['text_muted']}; font-size: 11px; }}
QLabel[role="details"] {{ color: {t['text']}; font-size: 11px; margin-top: 4px; }}
QLabel[role="dates"] {{ color: {t['text_faint']}; font-size: 11px; }}
QLabel[role="score"] {{ color: {t['success']}; font-size: 28px; font-weight: bold;
                        margin-right: 20px; }}

QLabel#preview {{
    background-color: {t['preview']}; color: {t['text_muted']};
    border: 1px solid {t['border']}; border-radius: 4px; font-size: 16px;
}}
QLabel#preview[kind="audio"] {{ font-size: 28px; }}
QLabel#preview[kind="generic"] {{ font-size: 20px; }}

/* ---- panes --------------------------------------------------------- */
QWidget#comparePane {{ background-color: {t['window']}; }}
QFrame#metaFrame {{ background-color: {t['surface']}; border: 1px solid {t['divider']};
                    border-radius: 4px; margin-top: 10px; }}
QFrame#actionBar {{ background-color: {t['surface_alt']}; border-top: 1px solid {t['border']}; }}

/* ---- lists and tables ---------------------------------------------- */
QListWidget, QTableWidget {{
    background-color: {t['surface']}; color: {t['text']};
    border: 1px solid {t['border']}; outline: none;
}}
QListWidget#matchList {{ font-size: 13px; border: none; }}
QListWidget#matchList::item {{ padding: 8px; border-bottom: 1px solid {t['divider']}; }}
QListWidget#matchList::item:hover {{ background-color: {t['surface_alt']}; }}
QListWidget#matchList::item:selected {{
    background-color: {t['selection']}; color: {t['text']};
    border-left: 3px solid {t['accent']};
}}
QTableWidget {{ gridline-color: {t['divider']}; }}
QTableWidget::item:selected {{ background-color: {t['selection']}; color: {t['text']}; }}
QHeaderView::section {{
    background-color: {t['surface_alt']}; color: {t['text_muted']};
    border: none; border-bottom: 1px solid {t['border']};
    border-right: 1px solid {t['divider']}; padding: 4px 6px; font-weight: bold;
}}
QTableCornerButton::section {{ background-color: {t['surface_alt']}; border: none; }}

/* ---- progress ------------------------------------------------------ */
QProgressBar {{ background-color: {t['progress_track']}; border: none; }}
QProgressBar::chunk {{ background-color: {t['accent']}; }}
"""


class ThemeManager(QObject):
    """
    Owns the active theme and keeps it in sync with the user's choice and,
    in "system" mode, with the desktop.
    """

    mode_changed = pyqtSignal(str)      # "system" | "light" | "dark"
    theme_applied = pyqtSignal(str)     # effective "light" | "dark"

    SETTINGS_KEY = "appearance/theme"

    def __init__(self, app, settings=None):
        super().__init__()
        self.app = app
        self.settings = settings or QSettings("FuzzyDuplicateFinder", "FuzzyDuplicateFinder")

        stored = self.settings.value(self.SETTINGS_KEY, "system")
        self.mode = stored if stored in MODES else "system"
        self.effective = None
        self._portal_scheme = None
        self._applying = False

        # Capture before we replace it; used as the last-resort signal.
        self._platform_palette = QPalette(app.palette())

        # Fusion honours the palette on every platform. The native Windows and
        # macOS styles ignore parts of it, which is how a "light" theme ends up
        # with a dark body or vice versa.
        app.setStyle("Fusion")

        hints = app.styleHints()
        if hasattr(hints, "colorSchemeChanged"):
            hints.colorSchemeChanged.connect(self._on_qt_scheme_changed)

        self._setup_portal()

    # -- detection --------------------------------------------------------

    def system_scheme(self):
        if self._portal_scheme in ("light", "dark"):
            return self._portal_scheme

        hints = self.app.styleHints()
        getter = getattr(hints, "colorScheme", None)
        if getter is not None:
            scheme = getter()
            if scheme == Qt.ColorScheme.Dark:
                return "dark"
            if scheme == Qt.ColorScheme.Light:
                return "light"

        window = self._platform_palette.color(QPalette.ColorRole.Window)
        return "dark" if window.lightness() < 128 else "light"

    def resolve(self):
        return self.system_scheme() if self.mode == "system" else self.mode

    # -- applying ---------------------------------------------------------

    def set_mode(self, mode):
        if mode not in MODES:
            return
        changed = mode != self.mode
        self.mode = mode
        self.settings.setValue(self.SETTINGS_KEY, mode)
        self.apply()
        if changed:
            self.mode_changed.emit(mode)

    def apply(self):
        if self._applying:
            return
        self._applying = True
        try:
            self._sync_platform_hint()
            effective = self.resolve()
            tokens = THEMES[effective]
            self.app.setPalette(build_palette(tokens))
            self.app.setStyleSheet(build_stylesheet(tokens))
            self.effective = effective
            self.theme_applied.emit(effective)
        finally:
            self._applying = False

    def _sync_platform_hint(self):
        """
        Tell Qt about a forced scheme so native chrome (the Windows title bar
        in particular) follows it. Unset it in system mode so Qt goes back to
        reporting the real desktop value.
        """
        hints = self.app.styleHints()
        if self.mode == "system":
            if hasattr(hints, "unsetColorScheme"):
                hints.unsetColorScheme()
        elif hasattr(hints, "setColorScheme"):
            hints.setColorScheme(
                Qt.ColorScheme.Dark if self.mode == "dark" else Qt.ColorScheme.Light
            )

    def _on_qt_scheme_changed(self, *_):
        if self.mode == "system" and not self._applying:
            self.apply()

    # -- freedesktop portal ----------------------------------------------

    _PORTAL_SERVICE = "org.freedesktop.portal.Desktop"
    _PORTAL_PATH = "/org/freedesktop/portal/desktop"
    _PORTAL_IFACE = "org.freedesktop.portal.Settings"
    _APPEARANCE_NS = "org.freedesktop.appearance"

    def _setup_portal(self):
        if not _HAVE_DBUS or not sys.platform.startswith("linux"):
            return
        try:
            bus = QDBusConnection.sessionBus()
            if not bus.isConnected():
                return
            iface = QDBusInterface(self._PORTAL_SERVICE, self._PORTAL_PATH,
                                   self._PORTAL_IFACE, bus)
            if not iface.isValid():
                return

            reply = iface.call("ReadOne", self._APPEARANCE_NS, "color-scheme")
            if reply.type() == QDBusMessage.MessageType.ErrorMessage:
                # ReadOne is portal v2; older portals only have Read, which
                # wraps the value in an extra variant.
                reply = iface.call("Read", self._APPEARANCE_NS, "color-scheme")
            if reply.type() != QDBusMessage.MessageType.ErrorMessage and reply.arguments():
                self._portal_scheme = self._decode_scheme(reply.arguments()[0])

            bus.connect(self._PORTAL_SERVICE, self._PORTAL_PATH, self._PORTAL_IFACE,
                        "SettingChanged", self._on_portal_setting_changed)
        except Exception:
            self._portal_scheme = None

    @staticmethod
    def _decode_scheme(value):
        for _ in range(3):
            if hasattr(value, "variant"):
                value = value.variant()
        try:
            number = int(value)
        except (TypeError, ValueError):
            return None
        # 0 = no preference, 1 = prefer dark, 2 = prefer light
        return {1: "dark", 2: "light"}.get(number)

    if _HAVE_DBUS:
        @pyqtSlot(QDBusMessage)
        def _on_portal_setting_changed(self, message):
            args = message.arguments()
            if len(args) < 3:
                return
            namespace, key, value = args[0], args[1], args[2]
            if namespace != self._APPEARANCE_NS or key != "color-scheme":
                return
            self._portal_scheme = self._decode_scheme(value)
            if self.mode == "system":
                self.apply()

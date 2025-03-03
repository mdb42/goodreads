import sys
import json

from PyQt6.QtCore import Qt
from PyQt6.QtGui import QGuiApplication
from PyQt6.QtWidgets import (
    QApplication,
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QStatusBar,
)

from app.gui.title_bar import TitleBar
from app.gui.data_browser import DataBrowser
\
import qdarkstyle
import qtawesome as qta

class MainWindow(QMainWindow):
    def __init__(self, config):
        super().__init__()
        self.config = config
        self._is_maximized = False
        self.title_bar = TitleBar(self)     

        # Apply theme
        self.dark_mode = self.config["display"]["theme"] == "dark"
        self.apply_styles()

        # Remove frame, enable translucent background
        self.setWindowFlags(Qt.WindowType.FramelessWindowHint)
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)

        # Set window title
        self.setWindowTitle(config["application"]["name"])

        # For edge-resizing
        self._resizing = False
        self._resize_dir = None
        self.border_width = 5

        # Main container
        container = QWidget(self)
        container.setObjectName("mainContainer")
        vlayout = QVBoxLayout(container)
        vlayout.setContentsMargins(0, 0, 0, 0)
        vlayout.setSpacing(0)

        # Title bar
        vlayout.addWidget(self.title_bar)

        # Content area
        self.content = QWidget()
        content_layout = QVBoxLayout(self.content)
        content_layout.setContentsMargins(10, 10, 10, 10)
        
        # Create and add the data browser
        self.data_browser = DataBrowser(self.config["data"]["database"]["path"])
        content_layout.addWidget(self.data_browser)
        
        vlayout.addWidget(self.content, 1)

        # Status bar
        sb = QStatusBar()
        sb.showMessage("Ready")
        vlayout.addWidget(sb)

        self.setCentralWidget(container)
        self.setMinimumSize(800, 600)
        self.resize(1200, 800)

        # Force an initial style update
        self.apply_styles()

    # ========== Theming / Style ==========
    def toggle_theme(self):
        self.dark_mode = not self.dark_mode
        self.apply_styles()

    def apply_styles(self):
        """
        Use QDarkStyle in dark mode, or revert to native in light mode.
        Then append custom corner + color rules for #mainContainer and #titleBar.
        """
        if self.dark_mode:
            super().setStyleSheet(qdarkstyle.load_stylesheet_pyqt6())
            # We want a dark title bar and container
            # We also want to conditionally remove corners if maximized
            css_template = """
            #mainContainer {
                background-color: #212121;
                %BORDER_RADIUS%
            }
            #titleBar {
                background-color: #2b2b2b;
                %TITLE_BAR_RADIUS%
            }
            """
        else:
            # Light => revert to OS native
            super().setStyleSheet("")
            css_template = """
            #mainContainer {
                background-color: #f5f5f5;
                %BORDER_RADIUS%
            }
            #titleBar {
                background-color: #dadada;
                %TITLE_BAR_RADIUS%
            }
            """

        if self._is_maximized:
            css = css_template.replace("%BORDER_RADIUS%", "border-radius: 0px;")
            css = css.replace("%TITLE_BAR_RADIUS%", "border-radius: 0px;")
        else:
            css = css_template.replace("%BORDER_RADIUS%", "border-radius: 10px;")
            css = css.replace(
                "%TITLE_BAR_RADIUS%",
                "border-top-left-radius: 10px; border-top-right-radius: 10px;"
            )

        # Append custom styles to whatever is set
        self.setStyleSheet(self.styleSheet() + css)

        # Update icons in the title bar
        icon_color = "white" if self.dark_mode else "black"
        # Theme button toggles sun/moon
        self.title_bar.theme_btn.setIcon(
            qta.icon('fa5s.sun', color=icon_color) if self.dark_mode
            else qta.icon('fa5s.moon', color=icon_color)
        )
        # Minimize, maximize/restore, close
        self.title_bar.min_btn.setIcon(qta.icon('fa5s.window-minimize', color=icon_color))
        restore_icon = 'fa5s.window-restore' if self._is_maximized else 'fa5s.window-maximize'
        self.title_bar.max_btn.setIcon(qta.icon(restore_icon, color=icon_color))
        self.title_bar.close_btn.setIcon(qta.icon('fa5s.times', color=icon_color))
        # App icon
        self.title_bar.app_icon.setIcon(qta.icon('fa5s.book-reader', color=icon_color))
        # Title text and background color
        self.title_bar.title_label.setStyleSheet(f"background-color: transparent; color: {icon_color}; font-size: 14px; font-weight: bold;")

        # Update icons in the data browser
        if hasattr(self, 'data_browser'):
            self.data_browser.update_icon_colors(self.dark_mode)

    # ========== Maximize / Restore ==========
    def toggle_maximize(self):
        if self._is_maximized:
            # Restore
            self._is_maximized = False
            if self._normal_geometry is not None:
                self.setGeometry(self._normal_geometry)
        else:
            # Maximize
            self._is_maximized = True
            self._normal_geometry = self.geometry()
            screen_geom = QGuiApplication.primaryScreen().availableGeometry()
            self.setGeometry(screen_geom)

        self.apply_styles()

    # ========== Resizing Logic ==========
    def mousePressEvent(self, e):
        if self._is_maximized:
            return super().mousePressEvent(e)
        if e.button() == Qt.MouseButton.LeftButton:
            direction = self.get_resize_direction(e.position().toPoint())
            if direction:
                self._resizing = True
                self._resize_dir = direction
                self.grabMouse()  # Capture all mouse events
                self.setCursor(self.resize_cursor(direction))
                e.accept()
                return
        super().mousePressEvent(e)

    def mouseReleaseEvent(self, e):
        if e.button() == Qt.MouseButton.LeftButton and self._resizing:
            self._resizing = False
            self._resize_dir = None
            self.releaseMouse()  # Release the mouse grab
            self.setCursor(Qt.CursorShape.ArrowCursor)
            e.accept()
            return
        super().mouseReleaseEvent(e)

    def focusOutEvent(self, event):
        # If focus is lost during a resize, cancel the operation
        if self._resizing:
            self._resizing = False
            self._resize_dir = None
            self.releaseMouse()  # Also release the grab here
            self.setCursor(Qt.CursorShape.ArrowCursor)
        super().focusOutEvent(event)

    def mouseMoveEvent(self, e):
        if self._is_maximized:
            return super().mouseMoveEvent(e)

        if self._resizing and self._resize_dir:
            self.do_resize(e.globalPosition().toPoint(), self._resize_dir)
            e.accept()
            return
        else:
            direction = self.get_resize_direction(e.position().toPoint())
            self.setCursor(self.resize_cursor(direction) if direction else Qt.CursorShape.ArrowCursor)
        super().mouseMoveEvent(e)

    def get_resize_direction(self, local_pos):
        """
        Return which edge/corner is being hovered, or None if not near edges.
        """
        rect = self.rect()
        x, y = local_pos.x(), local_pos.y()
        bw = self.border_width

        # Edges
        left = (x <= bw)
        right = (x >= rect.width() - bw)
        top = (y <= bw)
        bottom = (y >= rect.height() - bw)

        if left and top:
            return "top-left"
        if right and top:
            return "top-right"
        if left and bottom:
            return "bottom-left"
        if right and bottom:
            return "bottom-right"
        if left:
            return "left"
        if right:
            return "right"
        if top:
            return "top"
        if bottom:
            return "bottom"
        return None

    def resize_cursor(self, direction):
        if direction in ["left", "right"]:
            return Qt.CursorShape.SizeHorCursor
        if direction in ["top", "bottom"]:
            return Qt.CursorShape.SizeVerCursor
        if direction in ["top-left", "bottom-right"]:
            return Qt.CursorShape.SizeFDiagCursor
        if direction in ["top-right", "bottom-left"]:
            return Qt.CursorShape.SizeBDiagCursor
        return Qt.CursorShape.ArrowCursor

    def do_resize(self, global_pos, direction):
        """
        Adjust geometry based on mouse drag in corner/edge.
        """
        rect = self.geometry()

        if "left" in direction:
            new_left = global_pos.x()
            if rect.right() - new_left >= self.minimumWidth():
                rect.setLeft(new_left)
        if "right" in direction:
            new_right = global_pos.x()
            if new_right - rect.left() >= self.minimumWidth():
                rect.setRight(new_right)
        if "top" in direction:
            new_top = global_pos.y()
            if rect.bottom() - new_top >= self.minimumHeight():
                rect.setTop(new_top)
        if "bottom" in direction:
            new_bottom = global_pos.y()
            if new_bottom - rect.top() >= self.minimumHeight():
                rect.setBottom(new_bottom)

        self.setGeometry(rect)

    def get_resize_direction_for_titlebar(self, pos, titlebar_width, titlebar_height):
        """
        If user clicks top-left or top-right corner in the title bar,
        return a direction so we can ignore drag and let the main window resize.
        """
        bw = self.border_width
        left_corner = (pos.x() <= bw and pos.y() <= bw)
        right_corner = (pos.x() >= titlebar_width - bw and pos.y() <= bw)

        if left_corner:
            return "top-left"
        if right_corner:
            return "top-right"        
        if pos.y() <= bw: return "top"
        return None

    def leaveEvent(self, event):
        """Handle mouse leaving window during resize."""
        if self._resizing:
            # Don't cancel resize operation, but reset cursor
            self.setCursor(Qt.CursorShape.ArrowCursor)
        super().leaveEvent(event)

    def closeEvent(self, event):
        # Save window state/position to config
        self.config["display"]["theme"] = "dark" if self.dark_mode else "light"
        
        # Write config to file
        with open("config.json", "w") as f:
            json.dump(self.config, indent=4, fp=f)
        
        event.accept()

if __name__ == "__main__":
    app = QApplication(sys.argv)

    # Example config
    config = {
        "application": {
            "name": "My Frameless App"
        }
    }
    window = MainWindow(config)
    window.show()
    sys.exit(app.exec())

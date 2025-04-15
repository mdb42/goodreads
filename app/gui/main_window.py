import sys
import json

from app.gui.title_bar import TitleBar
from app.gui.setup.setup_widget import SetupWidget
from app.gui.search_widget import ParametricSearchWidget
from app.gui.results_widget import ResultsWidget
import qdarkstyle
import qtawesome as qta

from PyQt6.QtCore import Qt
from PyQt6.QtGui import QGuiApplication
from PyQt6.QtWidgets import (
    QApplication,
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QStatusBar,
    QStackedWidget
)

class MainWindow(QMainWindow):
    """
    Main application window for the Goodreads Analytics Tool.
    """
    def __init__(self, engine):
        super().__init__()
        self.engine = engine  # Store reference to the analytics engine
        self.config = engine.config
        self.db = engine.db
        self._is_maximized = False
        self.title_bar = TitleBar(self)

        # Determine dark mode based on configuration and apply styles
        self.dark_mode = self.config["display"]["theme"] == "dark"
        self.apply_styles()

        # Set window flags for a frameless, translucent window
        self.setWindowFlags(Qt.WindowType.FramelessWindowHint)
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        self.setWindowTitle(self.config["application"]["name"])

        # Variables for manual edge-resizing
        self._resizing = False
        self._resize_dir = None
        self.border_width = 5

        # Main container setup
        container = QWidget(self)
        container.setObjectName("mainContainer")
        vlayout = QVBoxLayout(container)
        vlayout.setContentsMargins(0, 0, 0, 0)
        vlayout.setSpacing(0)

        # Add the custom title bar
        vlayout.addWidget(self.title_bar)

        # Create the content area
        self.content = QWidget()
        content_layout = QVBoxLayout(self.content)
        self.content.setObjectName("content")  # Add this line
        content_layout.setContentsMargins(10, 10, 10, 10)
        
        # Create the stacked widget for different application states
        self.stacked_widget = QStackedWidget()
        
        # Create setup and home widgets (to be implemented)
        self.setup_widget = SetupWidget(self.engine, self)
        self.search_widget = ParametricSearchWidget(self.engine, self)
        self.results_widget = ResultsWidget(self.engine, self)
        
        self.search_widget.search_requested.connect(self.handle_search)
        self.results_widget.back_to_search_requested.connect(self.show_search_interface)
        
        # Add widgets to the stack
        self.stacked_widget.addWidget(self.setup_widget)  # Index 0: Setup
        self.stacked_widget.addWidget(self.search_widget)  # Index 1: Search
        self.stacked_widget.addWidget(self.results_widget)  # Index 2: Results
        
        # Add stacked widget to the layout
        content_layout.addWidget(self.stacked_widget)
        
        vlayout.addWidget(self.content, 1)

        # Create and add a status bar
        self.status_bar = QStatusBar()
        self.status_bar.showMessage("Ready")
        vlayout.addWidget(self.status_bar)

        self.setCentralWidget(container)
        self.setMinimumSize(800, 600)
        self.resize(800, 600)
        
        # Apply styles after layout setup
        self.apply_styles()

    def show_setup_interface(self):
        """
        Switch to the setup wizard interface.
        """
        self.stacked_widget.setCurrentIndex(0)
        self.status_bar.showMessage("Setup required")
    
    def show_main_interface(self):
        """
        Switch to the main application interface.
        """
        self.stacked_widget.setCurrentIndex(1)
        self.status_bar.showMessage("Ready")

    def show_search_interface(self):
        """Switch to the search interface."""
        self.stacked_widget.setCurrentIndex(1)

    # ========== Theming / Style ==========
    def toggle_theme(self):
        """
        Toggle between dark and light themes and update styles.
        """
        self.dark_mode = not self.dark_mode
        self.apply_styles()

    def apply_styles(self):
        """
        Apply style sheets based on the current theme and window state.
        """
        # Start with the base stylesheet.
        if self.dark_mode:
            # Load dark style base.
            base_css = qdarkstyle.load_stylesheet_pyqt6()
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
            base_css = ""
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

        # Adjust border radius based on window maximized state.
        if self._is_maximized:
            css = css_template.replace("%BORDER_RADIUS%", "border-radius: 0px;")
            css = css.replace("%TITLE_BAR_RADIUS%", "border-radius: 0px;")
        else:
            css = css_template.replace("%BORDER_RADIUS%", "border-radius: 10px;")
            css = css.replace(
                "%TITLE_BAR_RADIUS%",
                "border-top-left-radius: 10px; border-top-right-radius: 10px;"
            )
        
        # Add visible outline when not maximized
        if not self._is_maximized:
            border_color = "#555555" if self.dark_mode else "#999999"
            # Apply border to mainContainer
            css += f"""
            #mainContainer {{
                border: 1px solid {border_color};
            }}
            """
            
            # Add a more distinct styling for the content area
            content_bg = "#262626" if self.dark_mode else "#fafafa"
            content_border = "#444444" if self.dark_mode else "#cccccc"
            css += f"""
            QWidget#content {{
                background-color: {content_bg};
                border-left: 1px solid {content_border};
                border-right: 1px solid {content_border};
                border-bottom: 1px solid {content_border};
                border-bottom-left-radius: 9px;
                border-bottom-right-radius: 9px;
                margin: 0px 1px 1px 1px;
            }}
            """

        # Instead of appending repeatedly, reset the style with base_css + custom CSS.
        full_css = base_css + css
        self.setStyleSheet(full_css)

        # Update icons with appropriate colors based on theme.
        icon_color = "white" if self.dark_mode else "black"
        self.title_bar.theme_btn.setIcon(
            qta.icon('fa5s.sun', color=icon_color) if self.dark_mode
            else qta.icon('fa5s.moon', color=icon_color)
        )
        self.title_bar.min_btn.setIcon(qta.icon('fa5s.window-minimize', color=icon_color))
        restore_icon = 'fa5s.window-restore' if self._is_maximized else 'fa5s.window-maximize'
        self.title_bar.max_btn.setIcon(qta.icon(restore_icon, color=icon_color))
        self.title_bar.close_btn.setIcon(qta.icon('fa5s.times', color=icon_color))
        self.title_bar.app_icon.setIcon(qta.icon('fa5s.book-reader', color=icon_color))
        self.title_bar.title_label.setStyleSheet(
            f"background-color: transparent; color: {icon_color}; font-size: 14px; font-weight: bold;"
        )

        # Update icon colors in other widgets as needed
        if hasattr(self, 'data_browser'):
            self.data_browser.update_icon_colors(self.dark_mode)
        
        if hasattr(self, 'home_widget'):
            self.home_widget.update_icon_colors(self.dark_mode)
        
        if hasattr(self, 'setup_widget'):
            self.setup_widget.update_icon_colors(self.dark_mode)

    # ========== Maximize / Restore ==========
    def toggle_maximize(self):
        """
        Toggle between maximized and restored window states and reapply styles.
        """
        if self._is_maximized:
            # Restore window to its previous geometry.
            self._is_maximized = False
            if hasattr(self, "_normal_geometry") and self._normal_geometry is not None:
                self.setGeometry(self._normal_geometry)
        else:
            # Save current geometry and maximize the window.
            self._is_maximized = True
            self._normal_geometry = self.geometry()
            screen_geom = QGuiApplication.primaryScreen().availableGeometry()
            self.setGeometry(screen_geom)

        self.apply_styles()

    # ========== Resizing Logic ==========
    def mousePressEvent(self, e):
        """
        Initiate manual window resizing if mouse is pressed near an edge.
        """
        if self._is_maximized:
            return super().mousePressEvent(e)
        if e.button() == Qt.MouseButton.LeftButton:
            direction = self.get_resize_direction(e.position().toPoint())
            if direction:
                self._resizing = True
                self._resize_dir = direction
                self.grabMouse()  # Capture all mouse events.
                self.setCursor(self.resize_cursor(direction))
                e.accept()
                return
        super().mousePressEvent(e)

    def mouseReleaseEvent(self, e):
        """
        Finalize the resizing operation when the mouse button is released.
        """
        if e.button() == Qt.MouseButton.LeftButton and self._resizing:
            self._resizing = False
            self._resize_dir = None
            self.releaseMouse()  # Release the mouse grab.
            self.setCursor(Qt.CursorShape.ArrowCursor)
            e.accept()
            return
        super().mouseReleaseEvent(e)

    def focusOutEvent(self, event):
        """
        Cancel resizing if the window loses focus.
        """
        if self._resizing:
            self._resizing = False
            self._resize_dir = None
            self.releaseMouse()  # Also release the grab.
            self.setCursor(Qt.CursorShape.ArrowCursor)
        super().focusOutEvent(event)

    def mouseMoveEvent(self, e):
        """
        Update window size during a manual resize or update cursor if hovering near an edge.
        """
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
        Determine which edge or corner is being hovered for resizing.

        Args:
            local_pos (QPoint): The mouse position relative to the window.

        Returns:
            str or None: A string indicating the edge/corner (e.g., 'top-left', 'right')
                         or None if not near an edge.
        """
        rect = self.rect()
        x, y = local_pos.x(), local_pos.y()
        bw = self.border_width

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
        """
        Return the appropriate cursor shape based on the resize direction.

        Args:
            direction (str): Resize direction (e.g., 'left', 'top-right').

        Returns:
            Qt.CursorShape: The cursor shape to use.
        """
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
        Adjust the window geometry based on the mouse drag in a specific edge or corner.

        Args:
            global_pos (QPoint): The current global mouse position.
            direction (str): The edge/corner direction.
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
        Determine if a resize should be triggered from the title bar.

        Args:
            pos (QPoint): Mouse position relative to the title bar.
            titlebar_width (int): Width of the title bar.
            titlebar_height (int): Height of the title bar.

        Returns:
            str or None: Resize direction if in a corner, otherwise None.
        """
        bw = self.border_width
        left_corner = (pos.x() <= bw and pos.y() <= bw)
        right_corner = (pos.x() >= titlebar_width - bw and pos.y() <= bw)

        if left_corner:
            return "top-left"
        if right_corner:
            return "top-right"
        if pos.y() <= bw:
            return "top"
        return None

    def leaveEvent(self, event):
        """
        Reset the cursor when the mouse leaves the window during resizing.
        """
        if self._resizing:
            self.setCursor(Qt.CursorShape.ArrowCursor)
        super().leaveEvent(event)

    def closeEvent(self, event):
        """
        Save window configuration on close and write settings to config.json.
        """
        self.config["display"]["theme"] = "dark" if self.dark_mode else "light"
        with open("config.json", "w") as f:
            json.dump(self.config, indent=4, fp=f)
        event.accept()

    def handle_search(self, params):
        """Handle search requests from the search widget."""
        try:
            self.status_bar.showMessage("Searching...")
            
            # Call the engine to perform the search
            results = self.engine.find_negative_reviewers_sql(
                min_reviews=params.get('min_reviews', 20),
                max_avg_rating=params.get('max_avg_rating', 2.0),
                keywords=params.get('keywords', []),
                genre=params.get('genre', None)
            )
            
            # Update the results widget with the found data
            self.results_widget.handle_search_results(results)
            
            # Switch to the results widget
            self.stacked_widget.setCurrentIndex(2)  # Show results
            
            self.status_bar.showMessage(f"Found {len(results)} negative reviewers")
        except Exception as e:
            self.status_bar.showMessage(f"Search error: {e}")
            # Log the full error details
            self.engine.logger.error(f"Search error: {e}", exc_info=True)

if __name__ == "__main__":
    app = QApplication(sys.argv)

    # Example configuration for testing.
    config = {
        "application": {
            "name": "My Frameless App"
        },
        "display": {
            "theme": "dark"
        },
        "data": {
            "database": {
                "path": "data/analytics.db"
            }
        }
    }
    window = MainWindow(config)
    window.show()
    sys.exit(app.exec())

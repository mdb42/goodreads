from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import QWidget, QHBoxLayout, QLabel, QPushButton
from PyQt6.QtGui import QMouseEvent

import qtawesome as qta

class TitleBar(QWidget):
    """
    Custom title bar widget for the application window.

    This widget includes the app icon, title text, and window control buttons (theme toggle,
    minimize, maximize/restore, close). It also implements mouse-based dragging for window repositioning
    and intercepts double-click events to toggle maximization.
    """
    def __init__(self, parent_window):
        """
        Initialize the TitleBar with the parent window.

        Args:
            parent_window (QWidget): The main application window to control.
        """
        super().__init__(parent_window)
        self.parent_window = parent_window
        self.setFixedHeight(40)
        self.setObjectName("titleBar")

        # Main layout: divides into left, center (currently empty), and right sections.
        main_layout = QHBoxLayout(self)
        main_layout.setContentsMargins(10, 0, 10, 0)
        main_layout.setSpacing(0)

        # --- Left Section: App icon and title label ---
        left_layout = QHBoxLayout()
        left_layout.setSpacing(8)

        self.app_icon = QPushButton()
        self.app_icon.setFixedSize(24, 24)
        # Transparent button style for the icon.
        self.app_icon.setStyleSheet("border: none; background: transparent;")

        self.title_label = QLabel(self.parent_window.config["application"]["name"])
        # Styling for the title text.
        self.title_label.setStyleSheet("background-color: transparent; font-size: 14px; font-weight: bold;")

        left_layout.addWidget(self.app_icon)
        left_layout.addWidget(self.title_label)
        left_layout.addStretch()  # Push content to the left.

        # --- Center Section: Reserved for future widgets (currently empty) ---
        center_layout = QHBoxLayout()
        center_layout.setSpacing(0)
        center_layout.setAlignment(Qt.AlignmentFlag.AlignCenter)

        # --- Right Section: Window control buttons ---
        right_layout = QHBoxLayout()
        right_layout.setSpacing(8)

        # Theme toggle button.
        self.theme_btn = self.create_title_button('fa5s.moon', self.parent_window.toggle_theme)
        # Minimize button.
        self.min_btn = self.create_title_button('fa5s.window-minimize', self.parent_window.showMinimized)
        # Maximize/Restore button.
        self.max_btn = self.create_title_button('fa5s.window-maximize', self.parent_window.toggle_maximize)
        # Close button.
        self.close_btn = self.create_title_button('fa5s.times', self.parent_window.close)
        # Different hover style for the close button.
        self.close_btn.setStyleSheet("""
            QPushButton:hover {
                background-color: #E81123;
                border-radius: 4px;
            }
        """)

        right_layout.addWidget(self.theme_btn)
        right_layout.addWidget(self.min_btn)
        right_layout.addWidget(self.max_btn)
        right_layout.addWidget(self.close_btn)

        # Add the left, center, and right sections to the main layout.
        main_layout.addLayout(left_layout, 1)
        main_layout.addLayout(center_layout, 2)
        main_layout.addLayout(right_layout, 0)

        # Variable to track the initial position when dragging.
        self._drag_pos = None
        # Install an event filter on the title bar to intercept mouse events.
        self.installEventFilter(self)

    def create_title_button(self, icon_name, callback):
        """
        Create a styled title bar button with an icon and a callback.

        Args:
            icon_name (str): Name of the FontAwesome icon.
            callback (callable): Function to call when the button is clicked.

        Returns:
            QPushButton: The configured button widget.
        """
        btn = QPushButton(qta.icon(icon_name, color='white'), "", self)
        btn.setFixedSize(30, 30)
        btn.setStyleSheet("""
            QPushButton {
                background-color: transparent; 
                border: none;
                padding: 5px;
            }
            QPushButton:hover {
                background-color: #949494;
                border-radius: 4px;
            }
        """)
        btn.clicked.connect(callback)
        return btn

    def eventFilter(self, obj, event):
        """
        Intercept and handle mouse events for dragging and double-click actions.

        Args:
            obj (QObject): The object receiving the event.
            event (QEvent): The event to handle.

        Returns:
            bool: True if the event is handled, otherwise False.
        """
        if obj == self and isinstance(event, QMouseEvent):
            if event.type() == QMouseEvent.Type.MouseButtonDblClick:
                if event.button() == Qt.MouseButton.LeftButton:
                    # Double-click on the title bar toggles maximization.
                    self.parent_window.toggle_maximize()
                    return True

            elif event.type() == QMouseEvent.Type.MouseButtonPress:
                if event.button() == Qt.MouseButton.LeftButton:
                    # Check if click is near the edge for resizing.
                    direction = self.parent_window.get_resize_direction_for_titlebar(
                        event.position().toPoint(), self.width(), self.height()
                    )
                    if direction is not None or self.parent_window._is_maximized:
                        # If near an edge or window is maximized, pass event to parent.
                        self._drag_pos = None
                        event.ignore()
                        return False
                    else:
                        # Otherwise, record position to start dragging.
                        self._drag_pos = event.globalPosition().toPoint()
                        return True

            elif event.type() == QMouseEvent.Type.MouseMove:
                if (event.buttons() & Qt.MouseButton.LeftButton) and self._drag_pos is not None:
                    # Calculate the movement delta and move the main window.
                    delta = event.globalPosition().toPoint() - self._drag_pos
                    self.parent_window.move(
                        self.parent_window.x() + delta.x(),
                        self.parent_window.y() + delta.y()
                    )
                    # Update the drag position.
                    self._drag_pos = event.globalPosition().toPoint()
                    return True

            elif event.type() == QMouseEvent.Type.MouseButtonRelease:
                if event.button() == Qt.MouseButton.LeftButton:
                    # Reset dragging state when the mouse button is released.
                    self._drag_pos = None
                    return True

        return super().eventFilter(obj, event)

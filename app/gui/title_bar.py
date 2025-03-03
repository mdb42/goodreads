from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import QWidget, QHBoxLayout, QLabel, QPushButton
from PyQt6.QtGui import QMouseEvent

import qtawesome as qta

class TitleBar(QWidget):
    def __init__(self, parent_window):
        super().__init__(parent_window)
        self.parent_window = parent_window
        self.setFixedHeight(40)
        self.setObjectName("titleBar")

        # Layout: [Icon + App Name | (future center widgets) | Buttons]
        main_layout = QHBoxLayout(self)
        main_layout.setContentsMargins(10, 0, 10, 0)
        main_layout.setSpacing(0)

        # Left: Icon + Title
        left_layout = QHBoxLayout()
        left_layout.setSpacing(8)

        self.app_icon = QPushButton()
        self.app_icon.setFixedSize(24, 24)
        self.app_icon.setStyleSheet("border: none; background: transparent;")

        self.title_label = QLabel(self.parent_window.config["application"]["name"])
        self.title_label.setStyleSheet("background-color: transparent; font-size: 14px; font-weight: bold;")

        left_layout.addWidget(self.app_icon)
        left_layout.addWidget(self.title_label)
        left_layout.addStretch()

        # Center: (currently empty)
        center_layout = QHBoxLayout()
        center_layout.setSpacing(0)
        center_layout.setAlignment(Qt.AlignmentFlag.AlignCenter)

        # Right: Theme toggle, minimize, maximize/restore, close
        right_layout = QHBoxLayout()
        right_layout.setSpacing(8)

        # Theme toggle
        self.theme_btn = self.create_title_button('fa5s.moon', self.parent_window.toggle_theme)

        # Minimize
        self.min_btn = self.create_title_button('fa5s.window-minimize', self.parent_window.showMinimized)

        # Maximize/Restore
        self.max_btn = self.create_title_button('fa5s.window-maximize', self.parent_window.toggle_maximize)

        # Close
        self.close_btn = self.create_title_button('fa5s.times', self.parent_window.close)
        # Style close differently on hover
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

        main_layout.addLayout(left_layout, 1)
        main_layout.addLayout(center_layout, 2)
        main_layout.addLayout(right_layout, 0)

        self._drag_pos = None
        self.installEventFilter(self)

    def create_title_button(self, icon_name, callback):
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
        if obj == self and isinstance(event, QMouseEvent):
            if event.type() == QMouseEvent.Type.MouseButtonDblClick:
                if event.button() == Qt.MouseButton.LeftButton:
                    # Double-click => Maximize/Restore
                    self.parent_window.toggle_maximize()
                    return True

            elif event.type() == QMouseEvent.Type.MouseButtonPress:
                if event.button() == Qt.MouseButton.LeftButton:
                    # If user clicks in top-left corner => pass to main window for resizing
                    direction = self.parent_window.get_resize_direction_for_titlebar(
                        event.position().toPoint(), self.width(), self.height()
                    )
                    if direction is not None or self.parent_window._is_maximized:
                        # This means "ignore" so main window can handle it
                        self._drag_pos = None
                        event.ignore()
                        return False
                    else:
                        # Start dragging
                        self._drag_pos = event.globalPosition().toPoint()
                        return True

            elif event.type() == QMouseEvent.Type.MouseMove:
                if (event.buttons() & Qt.MouseButton.LeftButton) and self._drag_pos is not None:
                    # Drag the window
                    delta = event.globalPosition().toPoint() - self._drag_pos
                    self.parent_window.move(
                        self.parent_window.x() + delta.x(),
                        self.parent_window.y() + delta.y()
                    )
                    self._drag_pos = event.globalPosition().toPoint()
                    return True

            elif event.type() == QMouseEvent.Type.MouseButtonRelease:
                if event.button() == Qt.MouseButton.LeftButton:
                    self._drag_pos = None
                    return True

        return super().eventFilter(obj, event)


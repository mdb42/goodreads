import qtawesome as qta
from PyQt6.QtWidgets import QWidget, QHBoxLayout, QPushButton

class SetupActionsWidget(QWidget):
    """Widget for the action buttons in the setup wizard."""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setup_ui()

    def setup_ui(self):
        layout = QHBoxLayout(self)

        self.download_btn = QPushButton("Download Missing Files")
        self.download_btn.setIcon(qta.icon("fa5s.download"))

        self.setup_db_btn = QPushButton("Initialize Database")
        self.setup_db_btn.setIcon(qta.icon("fa5s.database"))
        self.setup_db_btn.setEnabled(False)

        # Changed to "Begin Analysis" with book-reader icon
        self.proceed_btn = QPushButton("Begin Analysis")
        self.proceed_btn.setIcon(qta.icon("fa5s.book-reader"))
        self.proceed_btn.setEnabled(False)

        layout.addWidget(self.download_btn)
        layout.addWidget(self.setup_db_btn)
        layout.addWidget(self.proceed_btn)

    def update_icon_colors(self, is_dark_mode: bool):
        """Update icons based on current theme."""
        icon_color = "white" if is_dark_mode else "black"
        self.download_btn.setIcon(qta.icon("fa5s.download", color=icon_color))
        self.setup_db_btn.setIcon(qta.icon("fa5s.database", color=icon_color))
        self.proceed_btn.setIcon(qta.icon("fa5s.book-reader", color=icon_color))


from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import QWidget, QVBoxLayout, QLabel
from PyQt6.QtGui import QFont

class SetupHeaderWidget(QWidget):
    """
    Displays the main title and introductory instructions.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setup_ui()

    def setup_ui(self):
        layout = QVBoxLayout(self)

        title = QLabel("Goodreads Analytics Setup")
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        title.setFont(QFont("Arial", 20, QFont.Weight.Bold))
        layout.addStretch(1)
        layout.addWidget(title)
        layout.addStretch(1)

        instructions = QLabel(
            "Welcome to Goodreads Analytics! Let's get your environment set up.\n"
            "We'll need to check for dataset files and prepare the database."
        )
        instructions.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(instructions)

    def update_icon_colors(self, is_dark_mode: bool):
        """
        (Optional) If your header had icons, you could adjust them here.
        Currently, there are no icons in the header, so this is a no-op.
        """
        pass

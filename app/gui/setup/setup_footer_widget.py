
from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import QWidget, QVBoxLayout
from PyQt6.QtGui import QDesktopServices
from PyQt6.QtWidgets import QPushButton
from PyQt6.QtCore import QUrl
import qtawesome as qta

class SetupFooterWidget(QWidget):
    """
    Displays the main title and introductory instructions.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setup_ui()

    def setup_ui(self):
        layout = QVBoxLayout(self)
        # Add a vertical spacer to push the button to the bottom
        layout.addStretch(1)

        # Create a 'Help' button with an icon
        self.help_button = QPushButton("Help")
        self.help_button.setFlat(True)  # Removes most button borders/visuals
        self.help_button.setStyleSheet("background: transparent; text-decoration: underline;")

        # Add a help icon using qtawesome
        self.help_button.setIcon(qta.icon('fa5s.question-circle', color="white"))
        self.help_button.clicked.connect(self.open_help_link)

        # Center the button horizontally
        layout.addWidget(self.help_button, alignment=Qt.AlignmentFlag.AlignHCenter)

    def open_help_link(self):
        """
        Open the project's README link in the default web browser.
        Replace the placeholder URL with your actual README link.
        """
        readme_url = QUrl("https://github.com/mdb42/goodreads/blob/main/README.md")
        QDesktopServices.openUrl(readme_url)

    def update_icon_colors(self, is_dark_mode: bool):
        """
        (Optional) If your header had icons, you could adjust them here.
        Currently, there are no icons in the header, so this is a no-op.
        """
        icon_color = "white" if is_dark_mode else "black"

        # Update the help button icon color
        self.help_button.setIcon(qta.icon('fa5s.question-circle', color=icon_color))

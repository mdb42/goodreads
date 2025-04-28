from PyQt6.QtWidgets import QWidget, QVBoxLayout, QFrame, QHBoxLayout, QLabel, QLineEdit, QPushButton
from PyQt6.QtGui import QFont
import qtawesome as qta
from PyQt6.QtCore import pyqtSignal
from PyQt6.QtWidgets import QFileDialog
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class SetupFilesWidget(QWidget):
    """
    Displays and manages the file paths for Books, Reviews, Interactions,
    and Database, showing whether each file is present/valid and allowing the user to browse.
    """


    # Let this widget send out a signal when file checks are updated
    filesChecked = pyqtSignal(dict)
    databaseStatusChanged = pyqtSignal(bool)  # is_initialized

    def __init__(self, config, parent=None):
        super().__init__(parent)
        self.config = config
        self.setup_ui()

    def setup_ui(self):
        self.layout = QVBoxLayout(self)

        # Create a status "row" for each file
        self.book_status = self.create_status_item(
            "Books", self.config["data"]["books"], "books"
        )
        self.review_status = self.create_status_item(
            "Reviews", self.config["data"]["reviews"], "reviews"
        )
        self.interactions_status = self.create_status_item(
            "Interactions", self.config["data"]["interactions"], "interactions"
        )
        self.database_status = self.create_status_item(
            "Database", self.config["data"]["database"], "database"
        )

        # Add them to the layout
        self.layout.addWidget(self.book_status["frame"])
        self.layout.addWidget(self.review_status["frame"])
        self.layout.addWidget(self.interactions_status["frame"])
        self.layout.addWidget(self.database_status["frame"])

    def create_status_item(self, label_text, file_path, file_key):
        """Helper to create one row of file status info."""
        frame = QFrame()
        layout = QHBoxLayout(frame)
        layout.setContentsMargins(5, 5, 5, 5)

        label = QLabel(f"{label_text}:")
        label.setMinimumWidth(130)  # Set fixed width for alignment
        label.setFont(QFont("Arial", 10, QFont.Weight.Bold))

        path_label = QLineEdit(file_path)
        path_label.setReadOnly(True)
        path_label.setFrame(False)
        path_label.setCursorPosition(0)

        status_label = QLabel("Checking...")

        # Use different browse function based on type
        browse_btn = QPushButton("Browse")
        browse_btn.setIcon(qta.icon("fa5s.folder-open"))
        
        if file_key == "database":
            browse_btn.clicked.connect(lambda: self.browse_database(path_label))
        else:
            browse_btn.clicked.connect(lambda: self.browse_file(label_text.lower(), path_label))

        layout.addWidget(label)
        layout.addWidget(path_label, 1)
        layout.addWidget(status_label)
        layout.addWidget(browse_btn)

        return {
            "frame": frame,
            "path_label": path_label,
            "status_label": status_label,
            "browse_btn": browse_btn,
            "file_key": file_key
        }

    def check_files(self):
        """
        Check the existence/size of each file, then emit a filesChecked signal
        so that the parent can respond if needed (e.g., enabling buttons).
        """
        status_dict = {}
        try:
            # Check each file
            book_path = Path(self.config["data"]["books"])
            review_path = Path(self.config["data"]["reviews"])
            interactions_path = Path(self.config["data"]["interactions"])

            books_exists = book_path.exists()
            reviews_exists = review_path.exists()
            interactions_exists = interactions_path.exists()

            # Update UI
            self.update_file_status("books", books_exists)
            self.update_file_status("reviews", reviews_exists)
            self.update_file_status("interactions", interactions_exists)

            # Build a dict of statuses
            status_dict = {
                "books": books_exists,
                "reviews": reviews_exists,
                "interactions": interactions_exists
            }
            
            # Also check database status but don't include in the emitted dict
            self.check_database_status()
            
        except Exception as e:
            logger.error(f"Error checking files: {e}")

        self.filesChecked.emit(status_dict)

    def update_file_status(self, file_key, exists):
        """Update the UI status label for a file."""
        status_item = None
        if file_key == "books":
            status_item = self.book_status
        elif file_key == "reviews":
            status_item = self.review_status
        elif file_key == "interactions":
            status_item = self.interactions_status

        if status_item:
            if exists:
                status_item["status_label"].setText("✓ Found")
                status_item["status_label"].setStyleSheet("color: green")
            else:
                status_item["status_label"].setText("✗ Missing")
                status_item["status_label"].setStyleSheet("color: red")

    def browse_file(self, file_type, path_label):
        """Open file browser to select dataset file, update config."""
        file_dialog = QFileDialog()
        file_path, _ = file_dialog.getOpenFileName(
            self,
            f"Select {file_type.capitalize()} Dataset File",
            str(Path.home()),
            "GZipped Files (*.gz);;All Files (*.*)"
        )

        if file_path:
            file_type_key = None
            if "books" in file_type.lower():
                file_type_key = "books"
            elif "reviews" in file_type.lower():
                file_type_key = "reviews"
            elif "interactions" in file_type.lower():
                file_type_key = "interactions"

            if file_type_key:
                self.config["data"][file_type_key] = file_path
                path_label.setText(file_path)

            # Immediately re-check files after user selects a new one
            self.check_files()
            
    def browse_database(self, path_label):
        """Browse for database file location."""
        file_dialog = QFileDialog()
        file_path, _ = file_dialog.getSaveFileName(
            self,
            "Select Database Location",
            str(Path.home()),
            "SQLite Database (*.db);;All Files (*.*)"
        )
        
        if file_path:
            # Add .db extension if not present
            if not file_path.lower().endswith('.db'):
                file_path += '.db'
                
            self.config["data"]["database"] = file_path
            path_label.setText(file_path)
            
            # Update database status
            self.check_database_status()
            
    def check_database_status(self):
        """Check if database exists and is initialized."""
        db_path = Path(self.config["data"]["database"])
        parent_dir = db_path.parent
        
        # Ensure the directory exists
        if not parent_dir.exists():
            try:
                parent_dir.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                logger.error(f"Failed to create directory for database: {e}")
        
        # Check if database exists and is initialized
        is_initialized = False
        if db_path.exists() and db_path.stat().st_size > 0:
            # Could add more checks here to verify it's a valid database
            try:
                from sqlite3 import connect
                conn = connect(str(db_path))
                cursor = conn.cursor()
                # Check for at least one table
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = cursor.fetchall()
                is_initialized = len(tables) > 0
                conn.close()
            except Exception as e:
                logger.error(f"Error checking database: {e}")
        
        # Update status label
        if is_initialized:
            self.database_status["status_label"].setText("✓ Initialized")
            self.database_status["status_label"].setStyleSheet("color: green")
        else:
            self.database_status["status_label"].setText("Not initialized")
            self.database_status["status_label"].setStyleSheet("color: red")
        
        # Emit signal about database status
        self.databaseStatusChanged.emit(is_initialized)

    def update_icon_colors(self, is_dark_mode: bool):
        """Update icons based on theme (dark/light)."""
        icon_color = "white" if is_dark_mode else "black"
        for status_item in [self.book_status, self.review_status, self.interactions_status, self.database_status]:
            status_item["browse_btn"].setIcon(qta.icon("fa5s.folder-open", color=icon_color))
            
    # Public method for other classes to check if DB is initialized
    def is_database_initialized(self):
        """Return whether database is initialized."""
        return self.database_status["status_label"].text() == "✓ Initialized"
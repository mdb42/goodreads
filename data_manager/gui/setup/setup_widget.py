# app/gui/setup_widget.py

from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QFrame
)
from PyQt6.QtCore import QTimer, pyqtSignal
from pathlib import Path
import logging
from gui.setup.setup_header_widget import SetupHeaderWidget
from gui.setup.setup_files_widget import SetupFilesWidget
from gui.setup.setup_progress_widget import SetupProgressWidget
from gui.setup.setup_actions_widget import SetupActionsWidget
from gui.setup.setup_footer_widget import SetupFooterWidget
from gui.setup.setup_worker import SetupWorker

logger = logging.getLogger(__name__)

class SetupWidget(QWidget):
    """Setup wizard for the application."""
    setup_completed = pyqtSignal()

    def __init__(self, engine, parent=None):
        super().__init__(parent)
        self.engine = engine
        self.config = engine.config
        self.parent = parent

        # Initialize the setup worker
        self.setup_worker = SetupWorker(self.engine)

        self.setup_ui()
        self.connect_worker_signals()

        # Kick off a file check as soon as the UI is ready
        QTimer.singleShot(500, self.check_initial_state)

    def setup_ui(self):
        main_layout = QVBoxLayout(self)

        # --- Create sub-widgets ---
        self.header_widget = SetupHeaderWidget() # Contains title, subtitle, instructions
        self.file_status_widget = SetupFilesWidget(self.config) # Shows file paths and status
        self.progress_widget = SetupProgressWidget() # Progress indicator with instructions/progress bar
        self.actions_widget = SetupActionsWidget() # Buttons to download files, setup DB
        self.footer_widget = SetupFooterWidget() # Footer with help button

        # --- Layout everything ---
        main_layout.addWidget(self.header_widget)

        # A frame to hold status info (files + progress)
        status_frame = QFrame()
        status_frame.setFrameShape(QFrame.Shape.StyledPanel)
        status_layout = QVBoxLayout(status_frame)

        status_layout.addWidget(self.file_status_widget)
        status_layout.addWidget(self.progress_widget)

        main_layout.addWidget(status_frame)

        main_layout.addWidget(self.actions_widget)
        main_layout.addWidget(self.footer_widget)

        # --- Connect signals from sub-widgets ---
        self.file_status_widget.filesChecked.connect(self.handle_file_check_result)
        self.file_status_widget.databaseStatusChanged.connect(self.handle_database_status_change)
        self.actions_widget.download_btn.clicked.connect(self.start_download)
        self.actions_widget.setup_db_btn.clicked.connect(self.initialize_database)
        self.actions_widget.proceed_btn.clicked.connect(self.proceed_to_analysis)

    def connect_worker_signals(self):
        """Connect signals from the SetupWorker to UI update methods."""
        self.setup_worker.signals.status_message.connect(self.update_status)
        self.setup_worker.signals.error.connect(self.handle_error)
        self.setup_worker.signals.finished.connect(self.setup_finished)
        self.setup_worker.signals.download_progress.connect(self.update_download_progress)
        self.setup_worker.signals.import_progress.connect(self.update_import_progress)
        self.setup_worker.signals.download_finished.connect(self.handle_download_finished)

    def check_initial_state(self):
        """Check files and database status on startup and update UI accordingly."""
        # Check files first
        self.file_status_widget.check_files()
        
        # Check if all files already exist and disable download button if they do
        file_status = self.get_file_status()
        all_files_exist = all(file_status.values())
        if all_files_exist:
            self.actions_widget.download_btn.setEnabled(False)
    
        # Set initial instruction in progress widget
        self.update_progress_instruction()
        
    def update_progress_instruction(self):
        """Update the instruction message in the progress widget based on current state."""
        # Get file status and database status
        file_status = self.get_file_status()
        db_initialized = self.is_database_initialized()
        
        all_files_present = all(file_status.values())
        
        if not all_files_present:
            missing = [k for k, v in file_status.items() if not v]
            self.progress_widget.set_instruction(
                f"Missing files: {', '.join(missing)}. Click 'Download Missing Files'."
            )
        elif not db_initialized:
            self.progress_widget.set_instruction(
                "All files ready. Click 'Initialize Database' to continue."
            )
        else:
            self.progress_widget.set_instruction(
                "Setup complete! Click 'Begin Analysis' to start."
            )

    def get_file_status(self):
        """Get the current status of dataset files."""
        book_path = Path(self.config["data"]["books"])
        review_path = Path(self.config["data"]["reviews"])
        interactions_path = Path(self.config["data"]["interactions"])
        
        return {
            "books": book_path.exists(),
            "reviews": review_path.exists(),
            "interactions": interactions_path.exists()
        }
        
    def is_database_initialized(self):
        """Check if database is initialized."""
        return self.file_status_widget.is_database_initialized()

    # ----------------------------------------------------------------------
    # Responding to FileStatusWidget
    # ----------------------------------------------------------------------
    def handle_file_check_result(self, status_dict):
        """
        Respond when FileStatusWidget finishes checking the files.
        """
        all_files_exist = all(status_dict.values())
        db_initialized = self.is_database_initialized()
        
        # Update action buttons
        self.actions_widget.setup_db_btn.setEnabled(all_files_exist)
        self.actions_widget.proceed_btn.setEnabled(all_files_exist and db_initialized)

        # Show status message
        if all_files_exist:
            if db_initialized:
                self.update_status("All dataset files found and database initialized. Ready to proceed.")
            else:
                self.update_status("All dataset files found. Ready to initialize database.")
        else:
            missing = [k for k, v in status_dict.items() if not v]
            self.update_status(f"Missing files: {', '.join(missing)}. Please download or locate files.")
        
        # Update progress widget instruction
        self.update_progress_instruction()
        
    def handle_database_status_change(self, is_initialized):
        """Handle changes to database initialization status."""
        all_files_exist = all(self.get_file_status().values())
        
        # Update proceed button
        self.actions_widget.proceed_btn.setEnabled(all_files_exist and is_initialized)
        
        # Update progress widget instruction
        self.update_progress_instruction()
        
        # Update status bar
        if is_initialized:
            self.update_status("Database initialized successfully.")
        else:
            self.update_status("Database not initialized.")

    # ----------------------------------------------------------------------
    # Button actions
    # ----------------------------------------------------------------------
    def start_download(self):
        """Start downloading missing files using the worker thread."""
        # Figure out which files are missing
        missing_files = []

        for file_key in ["books", "reviews", "interactions"]:
            file_path = Path(self.config["data"][file_key])
            if not file_path.exists():
                missing_files.append(file_key)

        if not missing_files:
            self.update_status("No files need to be downloaded.")
            return

        self.actions_widget.download_btn.setEnabled(False)
        self.actions_widget.setup_db_btn.setEnabled(False)

        files_list = ", ".join(missing_files)
        self.update_status(f"Starting download of missing files: {files_list}")
        
        # Show progress widget with appropriate operation name
        self.progress_widget.set_operation(f"Downloading {', '.join(missing_files)}")

        # Use the worker to download
        self.setup_worker.start_operation("download_files", files_to_download=missing_files)

    def handle_download_finished(self, file_key, success):
        """Handle completion of a file download."""
        if file_key != "all":
            # Individual file finished
            if success:
                self.update_status(f"Download complete: {file_key}")
                self.file_status_widget.update_file_status(file_key, True)
            else:
                self.update_status(f"Download failed: {file_key}")
        else:
            # All downloads finished
            if success:
                self.update_status("All downloads completed successfully.")
                
                # Force re-check all files instead of using cached status
                self.file_status_widget.check_files()
                
                # Wait briefly then directly set button state
                QTimer.singleShot(100, self.update_button_states_after_download)
            else:
                self.update_status("Download process failed or was cancelled.")
                self.actions_widget.download_btn.setEnabled(True)

    def update_button_states_after_download(self):
        """Update button states based on current file status."""
        file_status = self.get_file_status()
        all_files_exist = all(file_status.values())
        
        self.actions_widget.download_btn.setEnabled(not all_files_exist)
        self.actions_widget.setup_db_btn.setEnabled(all_files_exist)
        
        # Also update progress instruction
        self.update_progress_instruction()


    def initialize_database(self):
        """Initialize the database schema."""
        self.actions_widget.setup_db_btn.setEnabled(False)
        self.update_status("Initializing database schema...")
        
        self.progress_widget.set_operation("Initializing database...")
        
        self.setup_worker.start_operation("initialize_db")
        
    def proceed_to_analysis(self):
        """Switch to the main analysis interface."""
        self.update_status("Loading analysis interface...")
        # Switch to main interface in parent window
        if self.parent:
            self.parent.show_main_interface()
        # Emit signal for any other listeners
        self.setup_completed.emit()

    # ----------------------------------------------------------------------
    # Worker callbacks
    # ----------------------------------------------------------------------
    def update_download_progress(self, file_key, percent, bytes_dl, total_bytes, file_index, total_files):
        """Update UI with download progress."""
        if total_bytes > 0:
            size_mb = total_bytes / (1024 * 1024)
            downloaded_mb = bytes_dl / (1024 * 1024)
            status = f"Downloading {file_key}: {percent}% ({downloaded_mb:.1f}/{size_mb:.1f} MB)"
            self.update_status(status)
            
            # Just update the operation label to show the current file
            self.progress_widget.set_operation(f"Downloading {file_key}")
            
            # Update operation progress
            self.progress_widget.set_operation_progress(percent)
            
            # If download is complete for this file, update its status
            if percent == 100:
                self.file_status_widget.update_file_status(file_key, True)
                self.update_status(f"Download complete: {file_key}")
                
                # Check if this enables the DB setup button
                self.file_status_widget.check_files()

    def update_import_progress(self, stage, percent):
        """Update UI with import progress."""
        stage_name = {
            "schema": "database schema",
            "books": "books",
            "reviews": "reviews",
            "all": "data"
        }.get(stage, stage)

        self.update_status(f"Importing {stage_name}: {percent}%")
        self.progress_widget.set_operation_progress(percent)

    def setup_finished(self):
        """Handle completion of setup work in the worker."""
        # Complete the operation in progress widget
        self.progress_widget.complete_operation()
        
        # Re-enable buttons and check file status
        self.actions_widget.download_btn.setEnabled(True)
        self.file_status_widget.check_files()
        
        # Update progress instruction based on new state
        self.update_progress_instruction()

    def handle_error(self, error_message):
        """Handle error from the worker thread."""
        self.update_status(f"Error: {error_message}")
        self.actions_widget.download_btn.setEnabled(True)
        self.actions_widget.setup_db_btn.setEnabled(True)
        
        # Return to instruction state
        self.progress_widget.complete_operation()
        self.update_progress_instruction()

    # ----------------------------------------------------------------------
    # Utility
    # ----------------------------------------------------------------------
    def update_status(self, message):
        """Update the parent's status bar (if present) or otherwise do nothing."""
        if self.parent and hasattr(self.parent, 'status_bar'):
            self.parent.status_bar.showMessage(message)

    def update_icon_colors(self, is_dark_mode: bool):
        """
        Update icons in child widgets based on current theme.
        """
        # Sub-widgets
        self.header_widget.update_icon_colors(is_dark_mode)
        self.file_status_widget.update_icon_colors(is_dark_mode)
        self.actions_widget.update_icon_colors(is_dark_mode)
        self.footer_widget.update_icon_colors(is_dark_mode)
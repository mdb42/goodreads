# app/core/analytics.py
from enum import Enum, auto
from pathlib import Path
import logging
from app.gui.main_window import MainWindow
from app.db.database import Database
from PyQt6.QtCore import pyqtSignal
import gzip
import json

class AppState(Enum):
    """
    Enumeration for application states to manage window and resource transitions.
    """
    INITIALIZING = auto()
    SETUP_NEEDED = auto()   # Database setup required
    DOWNLOADING = auto()    # Downloading dataset files
    IMPORTING = auto()      # Importing data into database
    READY = auto()          # Ready for analysis
    CLOSING = auto()        # Application shutting down

class AnalyticsEngine:
    """
    Core analytics engine for the Goodreads Analytics Tool.
    
    Startup Flow:
    1. Initialize database connection
    2. Check database status
       - If database file doesn't exist, set state to SETUP_NEEDED
       - If database exists but isn't initialized, set state to SETUP_NEEDED
       - If database is ready, set state to READY
    3. Initialize appropriate UI based on state
       - If SETUP_NEEDED, show setup wizard
       - If READY, show main analysis interface
    """
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.state = AppState.INITIALIZING
        self.state_changed = pyqtSignal(AppState)
        self.app_window = None

        # Initialize database
        self._initialize_database()
        
        # Show appropriate window based on application state
        self._initialize_ui()

    def set_state(self, new_state):
        """Update the application state and emit signal."""
        self.state = new_state
        self.state_changed.emit(new_state)
        
    # Simplified _initialize_database method
    def _initialize_database(self):
        """
        Initialize database connection and check database status.
        """
        try:
            db_path = Path(self.config["data"]["database"])
            
            # Create the database directory if it doesn't exist
            db_path.parent.mkdir(exist_ok=True, parents=True)
            
            # Initialize database with the config
            self.db = Database(self.config)
            
            # Check if database needs setup
            if not db_path.exists() or not self.db.is_initialized():
                self.logger.info("Database setup required")
                self.state = AppState.SETUP_NEEDED
            else:
                self.logger.info("Database is ready")
                self.state = AppState.READY
                
        except Exception as e:
            self.logger.error(f"Database initialization error: {e}")
            self.state = AppState.SETUP_NEEDED

        print(self.db.get_database_report())
        # self.print_file_samples()
    
    def print_file_samples(self):
        """Print sample records from each dataset file to understand structure."""
        for file_key in ["books", "reviews", "interactions"]:
            file_path = Path(self.config["data"][file_key])
            if file_path.exists():
                print(f"\n=== Sample from {file_key} ===")
                try:
                    with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                        # Print first 2 records
                        for i, line in enumerate(f):
                            if i >= 2:
                                break
                            record = json.loads(line)
                            # For books, specifically check author structure
                            if file_key == "books" and "authors" in record:
                                print(f"Author data structure: {record['authors']}")
                            print(json.dumps(record, indent=2))
                except Exception as e:
                    print(f"Error reading {file_path}: {e}")

    def _initialize_ui(self):
        """
        Initialize the appropriate UI based on application state.
        """
        try:
            # Create main window passing the engine reference so it can access the database
            self.app_window = MainWindow(self)
            
            # If setup is needed, show setup interface
            if self.state == AppState.SETUP_NEEDED:
                self.logger.info("Showing setup interface")
                self.app_window.show_setup_interface()
            else:
                self.logger.info("Showing main interface")
                self.app_window.show_main_interface()
                
            # Show the window
            self.app_window.show()
            
        except Exception as e:
            self.logger.error(f"UI initialization error: {e}")
            raise

    def close(self):
        """
        Initiate application shutdown procedures.

        This method changes the application state to CLOSING and performs cleanup of resources.
        """
        self.logger.info("Initiating application shutdown...")
        self.state = AppState.CLOSING
        self._cleanup()
    
    def _cleanup(self):
        """
        Perform final cleanup operations.

        Closes the main window and the database connection, ensuring that all resources are freed.
        """
        if self.app_window:
            try:
                self.app_window.close()
                self.logger.info("Main window closed successfully")
            except Exception as e:
                self.logger.error(f"Error closing main window: {e}")
            finally:
                self.app_window = None
        
        # Close the database connection if it exists.
        if hasattr(self, 'db'):
            try:
                self.db.close()
                self.logger.info("Database connection closed successfully")
            except Exception as e:
                self.logger.error(f"Error closing database connection: {e}")
        
        self.logger.info("Application cleanup complete")

    def update_state(self, new_state, status_message=None):
        """
        Update application state and log the change.
        """
        self.state = new_state
        self.logger.info(f"Application state changed to: {new_state.name}")
        
        # Propagate status message if provided
        if status_message and hasattr(self, 'app_window') and self.app_window:
            if hasattr(self.app_window, 'status_bar'):
                self.app_window.status_bar.showMessage(status_message)
                
    # Add state transition methods            
    def start_download(self, files_to_download):
        """
        Start downloading missing files.
        """
        self.update_state(AppState.DOWNLOADING, f"Downloading files: {', '.join(files_to_download)}")

    def start_import(self):
        """
        Start importing data.
        """
        self.update_state(AppState.IMPORTING, "Importing data into database")
        
    def setup_complete(self):
        """
        Mark setup as complete.
        """
        self.update_state(AppState.READY, "Setup complete! Ready for analysis.")
# app/gui/setup_worker.py
from PyQt6.QtCore import QObject, pyqtSignal, QThread, QMutex
import logging
import time
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class SetupWorkerSignals(QObject):
    """Signals for the setup worker thread."""
    
    # General signals
    started = pyqtSignal()
    finished = pyqtSignal()
    error = pyqtSignal(str)
    file_check_result = pyqtSignal(dict)  # Dictionary of file keys and existence status
    
    # Download signals
    download_started = pyqtSignal(str)  # file_key
    download_progress = pyqtSignal(str, int, int, int, int, int)  # file_key, percent, bytes, total, file_index, total_files
    download_finished = pyqtSignal(str, bool)  # file_key, success
    
    # Import signals
    import_started = pyqtSignal(str)  # stage
    import_progress = pyqtSignal(str, int)  # stage, percent
    import_finished = pyqtSignal(str, bool)  # stage, success
    
    # Overall progress
    overall_progress = pyqtSignal(int)  # percent
    
    # Status message
    status_message = pyqtSignal(str)  # message

class SetupWorker(QThread):
    """Worker thread for setup operations."""
    
    def __init__(self, engine, parent=None):
        super().__init__(parent)
        self.engine = engine
        self.signals = SetupWorkerSignals()
        self.db = None
        self.loader = None
        self.mutex = QMutex()
        self.cancelled = False
        self.operation = None
        self.operation_args = {}
        
    def run(self):
        """Run the assigned operation."""
        self.signals.started.emit()
        self.cancelled = False
        
        try:
            if self.operation == "check_files":
                self._check_files()
            elif self.operation == "download_files":
                self._download_files()
            elif self.operation == "initialize_db":
                self._initialize_db()
            elif self.operation == "import_data":
                self._import_data()
            elif self.operation == "full_setup":
                self._full_setup()
            else:
                self.signals.error.emit(f"Unknown operation: {self.operation}")
        except Exception as e:
            logger.error(f"Error in setup worker: {e}", exc_info=True)
            self.signals.error.emit(str(e))
        finally:
            self.signals.finished.emit()
    
    def cancel(self):
        """Cancel the current operation."""
        self.mutex.lock()
        self.cancelled = True
        self.mutex.unlock()
        
    def check_cancelled(self) -> bool:
        """Check if the operation has been cancelled."""
        self.mutex.lock()
        result = self.cancelled
        self.mutex.unlock()
        return result
    
    def start_operation(self, operation, **kwargs):
        """
        Set up and start an operation.
        
        Args:
            operation: Operation to perform
            **kwargs: Operation arguments
        """
        # Wait for any running operation to finish
        if self.isRunning():
            self.wait()
            
        self.operation = operation
        self.operation_args = kwargs
        self.start()
    
    def _check_files(self):
        """Check if dataset files exist."""
        self.signals.status_message.emit("Checking for dataset files...")
        
        try:
            # Create loader
            self.loader = self.engine.db.get_data_loader()
            
            # Check files and emit results
            status = self.loader.check_files_exist()
            
            # Calculate overall file status percentage
            found_count = sum(1 for exists in status.values() if exists)
            total_count = len(status)
            percent = int((found_count / max(1, total_count)) * 100)
            
            self.signals.overall_progress.emit(percent)
            
            # Success message
            if all(status.values()):
                self.signals.status_message.emit("All dataset files found!")
            else:
                # This is where the error is occurring
                # missing = [k for k, v in status.values() if not v]  # Wrong!
                missing = [k for k, v in status.items() if not v]  # Correct!
                self.signals.status_message.emit(f"Missing files: {', '.join(missing)}")
            
        except Exception as e:
            logger.error(f"Error checking files: {e}", exc_info=True)
            self.signals.error.emit(f"Error checking files: {str(e)}")
        
        self.signals.file_check_result.emit(status)
    
    def _download_files(self):
        """Download missing dataset files."""
        files_to_download = self.operation_args.get("files_to_download", [])
        self.signals.status_message.emit(f"Downloading missing files: {', '.join(files_to_download)}...")
        
        try:
            # Create loader if needed
            if not self.loader:
                self.loader = self.engine.db.get_file_downloader()
            
            # Get total files count
            total_files = len(files_to_download)
            overall_success = True
            
            # Download each file separately
            for file_index, file_key in enumerate(files_to_download, 1):
                self.signals.status_message.emit(f"Downloading {file_key} ({file_index}/{total_files})...")
                
                # Setup progress callback for this file
                def progress_callback(percent, bytes_dl, total_bytes):
                    if self.check_cancelled():
                        return
                    self.signals.download_progress.emit(
                        file_key, percent, bytes_dl, total_bytes, 
                        file_index, total_files
                    )
                
                # Download this file
                success = self.loader.download_file(file_key, progress_callback)
                
                # Emit completion for this specific file
                self.signals.download_finished.emit(file_key, success)
                
                if not success:
                    overall_success = False
                    self.signals.status_message.emit(f"Download failed for {file_key}.")
                    break
            
            # All files completed
            self.signals.download_finished.emit("all", overall_success)
            
            if overall_success:
                self.signals.status_message.emit("All downloads completed successfully!")
            else:
                self.signals.status_message.emit("Some downloads failed or were cancelled.")
                
        except Exception as e:
            logger.error(f"Error in download process: {e}", exc_info=True)
            self.signals.error.emit(f"Error downloading files: {str(e)}")
            self.signals.download_finished.emit("all", False)
    
    def _initialize_db(self):
        """Initialize the database schema and import data."""
        self.signals.status_message.emit("Initializing database...")
        self.signals.import_started.emit("schema")
        
        try:
            # Create a new database connection in this thread
            from app.db.database import Database
            db = Database(self.engine.config)
            
            # Initialize schema
            from app.db.models import initialize_database
            success = initialize_database(db)
            
            if success:
                self.signals.status_message.emit("Database schema created. Starting data import...")
                
                # Import data
                from app.db.importer import DatasetImporter
                importer = DatasetImporter(self.engine.config, db)
                
                # Define progress callback
                def progress_callback(stage, percent):
                    if self.check_cancelled():
                        return
                    self.signals.import_progress.emit(stage, percent)
                
                # Start import
                import_success = importer.import_all(progress_callback)
                
                # Get stats
                stats = db.get_database_stats()
                stats_str = ", ".join([f"{k}: {v}" for k, v in stats.items()])
                self.signals.status_message.emit(f"Import complete. Database stats: {stats_str}")
                
                success = success and import_success
            
            # Close this thread's connection
            db.close()
            
            self.signals.import_finished.emit("schema", success)
            
            if success:
                self.signals.status_message.emit("Database initialization and import complete!")
                self.signals.overall_progress.emit(100)
            else:
                self.signals.status_message.emit("Database initialization or import failed.")
                self.signals.overall_progress.emit(0)
            
        except Exception as e:
            logger.error(f"Error initializing database: {e}", exc_info=True)
            self.signals.error.emit(f"Error initializing database: {str(e)}")
            self.signals.import_finished.emit("schema", False)
    
    def _import_data(self):
        """Import data into the database."""
        limit = self.operation_args.get("limit", None)
        
        self.signals.status_message.emit("Importing dataset into database...")
        
        try:
            # Create a new database connection for this thread
            from app.db.database import Database
            db = Database(self.engine.config)
            
            # Create importer
            from app.db.importer import DatasetImporter
            importer = DatasetImporter(self.engine.config, db)
            
            # Define progress callback
            def progress_callback(stage, percent):
                if self.check_cancelled():
                    return
                self.signals.import_progress.emit(stage, percent)
                
                # Map stage progress to overall progress (books = 30%, reviews = 70%)
                if stage == "books":
                    overall = int(percent * 0.3)
                elif stage == "reviews":
                    overall = 30 + int(percent * 0.7)
                else:
                    overall = percent
                self.signals.overall_progress.emit(overall)
            
            # Start import - this will take a while
            success = importer.import_all(db, limit, progress_callback)
            
            # Close the database connection
            db.close()
            
            self.signals.import_finished.emit("all", success)
            
            if success:
                self.signals.status_message.emit("Dataset import completed successfully!")
                self.signals.overall_progress.emit(100)
            else:
                self.signals.status_message.emit("Dataset import failed or was cancelled.")
                    
        except Exception as e:
            logger.error(f"Error importing data: {e}", exc_info=True)
            self.signals.error.emit(f"Error importing data: {str(e)}")
            self.signals.import_finished.emit("all", False)
    
    def _full_setup(self):
        """Perform complete setup including download, initialization, and import."""
        limit = self.operation_args.get("limit", None)
        
        # 1. Check files
        self.signals.status_message.emit("Checking for dataset files...")
        
        try:
            # Get database connection and loader
            db = self.engine.db
            self.loader = db.get_data_loader()
            
            # Check which files exist
            file_status = self.loader.check_files_exist()
            
            # 2. Download missing files if needed
            if not all(file_status.values()):
                missing = [k for k, v in file_status.items() if not v]
                self.signals.status_message.emit(f"Downloading missing files: {', '.join(missing)}...")
                
                # Setup download progress callback
                def download_callback(file_key, percent, bytes_dl, total_bytes, file_index, total_files):
                    if self.check_cancelled():
                        return
                    self.signals.download_progress.emit(file_key, percent, bytes_dl, total_bytes, file_index, total_files)
                    
                    # Download is 20% of total process
                    overall_percent = int(((file_index * 100) + percent) / total_files * 0.2)
                    self.signals.overall_progress.emit(overall_percent)
                
                # Start download
                download_success = self.loader.download_all_missing(download_callback)
                
                if not download_success:
                    self.signals.status_message.emit("Download failed - cannot continue setup.")
                    self.signals.overall_progress.emit(10)  # Show some progress
                    return
            else:
                self.signals.status_message.emit("All dataset files already present.")
                self.signals.overall_progress.emit(20)  # Skip download stage
            
            # 3. Initialize database schema
            self.signals.status_message.emit("Initializing database schema...")
            
            # Initialize schema
            schema_success = db.initialize_schema()
            
            if not schema_success:
                self.signals.status_message.emit("Schema initialization failed - cannot continue.")
                self.signals.overall_progress.emit(25)  # Show some progress
                return
                
            self.signals.overall_progress.emit(30)  # Schema is 10% of process
            
            # 4. Import data
            self.signals.status_message.emit("Importing dataset into database...")
            
            # Define import progress callback
            def import_callback(stage, percent):
                if self.check_cancelled():
                    return
                self.signals.import_progress.emit(stage, percent)
                
                # Import is 70% of total process (30% to 100%)
                if stage == "books":
                    overall = 30 + int(percent * 0.2)  # Books are 20% of remaining 70%
                elif stage == "reviews":
                    overall = 50 + int(percent * 0.5)  # Reviews are 50% of remaining 70%
                else:
                    overall = 30 + int(percent * 0.7)  # Generic fallback
                self.signals.overall_progress.emit(overall)
            
            # Start import
            import_success = self.loader.import_all(db, limit, import_callback)
            
            if import_success:
                self.signals.status_message.emit("Setup completed successfully!")
                self.signals.overall_progress.emit(100)
            else:
                self.signals.status_message.emit("Import failed or was cancelled.")
            
        except Exception as e:
            logger.error(f"Error during full setup: {e}", exc_info=True)
            self.signals.error.emit(f"Error during setup: {str(e)}")
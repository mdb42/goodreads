from enum import Enum, auto
from pathlib import Path
import logging
from app.gui.main_window import MainWindow
import importlib.resources as resources
from app.src.analytics_db import AnalyticsDB
import app.src.dataset_loader as dataset_loader

class AppState(Enum):
    """Application states for window and resource management"""
    INITIALIZING = auto()
    MAIN = auto()
    CLOSING = auto()

class AnalyticsEngine:
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.state = AppState.INITIALIZING
        self.app_window = None
        
        # Initialize core components
        self._setup_directories()
        self._setup_database()
        
        # Run diagnostics to report on database state
        self._run_database_diagnostics()
        
        self._show_main_window()
    
    def _setup_directories(self):
        """Create necessary application directories"""
        self.data_dir = Path("data")
        self.data_dir.mkdir(exist_ok=True)

    def _setup_database(self):
        """Initialize database connection"""
        self.db = AnalyticsDB(self.data_dir / "analytics.db")
    
    def _run_database_diagnostics(self):
        """Run diagnostics on the database to report its state"""
        self.logger.info("===== DATABASE DIAGNOSTICS =====")
        
        try:
            # Get all tables
            tables = self.get_all_tables()
            self.logger.info(f"Found {len(tables)} tables in database: {', '.join(tables)}")
            
            # For each table, get structure and record count
            for table in tables:
                try:
                    # Get table structure
                    structure = self.execute_query(f"PRAGMA table_info({table})")
                    columns = [f"{col[1]} ({col[2]})" for col in structure['data']]
                    
                    # Get record count
                    count_result = self.execute_query(f"SELECT COUNT(*) FROM {table}")
                    record_count = count_result['data'][0][0]
                    
                    # Get primary key
                    pk_info = [col[1] for col in structure['data'] if col[5] == 1]  # Column 5 is pk flag
                    pk_str = f"Primary Key: {', '.join(pk_info)}" if pk_info else "No Primary Key"
                    
                    # Report
                    self.logger.info(f"Table: {table}")
                    self.logger.info(f"  Records: {record_count}")
                    self.logger.info(f"  {pk_str}")
                    self.logger.info(f"  Columns: {', '.join(columns)}")
                    
                    # If table has records, show a sample
                    if record_count > 0:
                        sample = self.execute_query(f"SELECT * FROM {table} LIMIT 1")
                        sample_data = sample['data'][0]
                        sample_str = []
                        for i, col in enumerate(sample['columns']):
                            val = sample_data[i]
                            if val is not None:
                                sample_str.append(f"{col}={val}")
                        
                        self.logger.info(f"  Sample Record: {', '.join(sample_str)}")
                    
                    # Check for foreign keys
                    fk_result = self.execute_query(f"PRAGMA foreign_key_list({table})")
                    if fk_result['data']:
                        fk_info = []
                        for fk in fk_result['data']:
                            fk_info.append(f"{fk[3]} -> {fk[2]}.{fk[4]}")
                        self.logger.info(f"  Foreign Keys: {', '.join(fk_info)}")
                    
                except Exception as e:
                    self.logger.error(f"Error getting info for table {table}: {e}")
            
            # Check for potential issues
            self._check_for_database_issues()
            
        except Exception as e:
            self.logger.error(f"Error running database diagnostics: {e}")
        
        self.logger.info("===== END DATABASE DIAGNOSTICS =====")
    
    def _check_for_database_issues(self):
        """Check for common database structure issues"""
        try:
            # Check if we have proper relationships (some book_authors entries)
            tables = self.get_all_tables()
            
            if 'book' in tables and 'book_authors' in tables:
                book_count = self.execute_query("SELECT COUNT(*) FROM book")['data'][0][0]
                relationship_count = self.execute_query("SELECT COUNT(*) FROM book_authors")['data'][0][0]
                
                if book_count > 0 and relationship_count == 0:
                    self.logger.warning("Potential issue: Books exist but no book-author relationships found")
            
            # Check foreign key integrity - this requires foreign_keys to be enabled
            self.execute_query("PRAGMA foreign_key_check")
            integrity_issues = self.execute_query("PRAGMA foreign_key_check")
            
            if integrity_issues['data']:
                self.logger.warning(f"Foreign key integrity issues found: {integrity_issues['data']}")
            
            # Check for tables with no records
            for table in tables:
                count_result = self.execute_query(f"SELECT COUNT(*) FROM {table}")
                if count_result['data'][0][0] == 0:
                    self.logger.warning(f"Table '{table}' exists but contains no records")
            
        except Exception as e:
            self.logger.error(f"Error checking for database issues: {e}")
    
    def _show_main_window(self):
        """Initialize and show main application window"""
        self.app_window = MainWindow(self.config)
        self.app_window.show()

    def _download_data(self):
        """Load data from the database and display in the UI"""
        self.logger.info("Loading data from the Kaggle API...")
        data = dataset_loader.main()
        self.logger.info("Data loaded successfully")

    def get_all_tables(self):
        """Get a list of all tables in the database"""
        try:
            cursor = self.db.conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            tables = [table[0] for table in cursor.fetchall() if not table[0].startswith('sqlite_')]
            return tables
        except Exception as e:
            self.logger.error(f"Error fetching tables: {e}")
            return []

    def execute_query(self, query, params=None):
        """Execute an SQL query and return results"""
        try:
            cursor = self.db.conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            # Check if this is a SELECT query (returns data)
            if query.strip().upper().startswith("SELECT") or query.strip().upper().startswith("PRAGMA"):
                columns = [description[0] for description in cursor.description] if cursor.description else []
                results = cursor.fetchall()
                return {"columns": columns, "data": results}
            else:
                # For non-SELECT queries, commit changes and return row count
                self.db.conn.commit()
                return {"rows_affected": cursor.rowcount}
        except Exception as e:
            self.logger.error(f"Error executing query: {e}")
            raise
    
    def close(self):
        """Handle application shutdown"""
        self.logger.info("Initiating application shutdown...")
        
        # Set state to closing to prevent further operations
        self.state = AppState.CLOSING
        
        # Call cleanup to handle resources
        self._cleanup()
    
    def _cleanup(self):
        """Perform final cleanup operations"""
        if self.app_window:
            self.app_window.close()
            self.app_window = None
        
        # Close database connection
        if hasattr(self, 'db'):
            self.db.close()
        
        self.logger.info("Application cleanup complete")
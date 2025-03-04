from enum import Enum, auto
from pathlib import Path
import logging
from app.gui.main_window import MainWindow
from app.src.analytics_db import AnalyticsDB

class AppState(Enum):
    """
    Enumeration for application states to manage window and resource transitions.
    
    States:
        INITIALIZING: Application is starting up.
        MAIN: Main window is active.
        CLOSING: Application is shutting down.
    """
    INITIALIZING = auto()
    MAIN = auto()
    CLOSING = auto()

class AnalyticsEngine:
    """
    Core analytics engine for the Goodreads Analytics Tool.

    This class handles initializing core components, setting up the database,
    running diagnostics, and launching the main application window.
    """
    def __init__(self, config):
        """
        Initialize the analytics engine with the given configuration.

        Args:
            config (dict): Configuration parameters for the application.
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.state = AppState.INITIALIZING
        self.app_window = None
        
        # Initialize core components.
        self._setup_directories()
        self._setup_database()
        
        # Run diagnostics on the database to report its state.
        self._run_database_diagnostics()
        
        # Launch the main GUI window.
        self._show_main_window()
        
        # Update state to MAIN
        self.state = AppState.MAIN
    
    def _setup_directories(self):
        """
        Create necessary application directories.

        Notes:
            - Currently, only the 'data' directory is ensured to exist.
            - This can be extended if additional directories are needed.
        """
        self.data_dir = Path("data")
        self.data_dir.mkdir(exist_ok=True)

    def _setup_database(self):
        """
        Initialize the database connection.

        Notes:
            - Creates an instance of AnalyticsDB pointing to a database file within the 'data' directory.
        """
        try:
            db_path = self.config["data"]["database"].get("path", "data/analytics.db")
            self.db = AnalyticsDB(db_path)
            self.logger.info(f"Database connection established at {db_path}")
        except Exception as e:
            self.logger.error(f"Failed to initialize database: {e}")
            raise RuntimeError(f"Could not connect to database: {e}")
    
    def _run_database_diagnostics(self):
        """
        Run diagnostics on the database and log its state.

        This method:
            - Retrieves all user-defined tables.
            - Logs the structure, record counts, primary keys, and sample data for each table.
            - Checks for foreign key relationships and common integrity issues.
        """
        self.logger.info("===== DATABASE DIAGNOSTICS =====")
        
        try:
            # Retrieve all tables in the database.
            tables = self.get_all_tables()
            self.logger.info(f"Found {len(tables)} tables in database: {', '.join(tables)}")
            
            # Iterate over each table and log details.
            for table in tables:
                try:
                    # Retrieve table structure via PRAGMA.
                    structure = self.execute_query(f"PRAGMA table_info({table})")
                    # Build column info string: column name (data type).
                    columns = [f"{col[1]} ({col[2]})" for col in structure['data']]
                    
                    # Get the number of records in the table.
                    count_result = self.execute_query(f"SELECT COUNT(*) FROM {table}")
                    record_count = count_result['data'][0][0]
                    
                    # Determine primary key columns (flag at index 5).
                    pk_info = [col[1] for col in structure['data'] if col[5] == 1]
                    pk_str = f"Primary Key: {', '.join(pk_info)}" if pk_info else "No Primary Key"
                    
                    # Log details for the table.
                    self.logger.info(f"Table: {table}")
                    self.logger.info(f"  Records: {record_count}")
                    self.logger.info(f"  {pk_str}")
                    self.logger.info(f"  Columns: {', '.join(columns)}")
                    
                    # If records exist, log a sample record.
                    if record_count > 0:
                        sample = self.execute_query(f"SELECT * FROM {table} LIMIT 1")
                        sample_data = sample['data'][0]
                        sample_str = []
                        for i, col in enumerate(sample['columns']):
                            val = sample_data[i]
                            if val is not None:
                                sample_str.append(f"{col}={val}")
                        self.logger.info(f"  Sample Record: {', '.join(sample_str)}")
                    
                    # Check for foreign keys in the table.
                    fk_result = self.execute_query(f"PRAGMA foreign_key_list({table})")
                    if fk_result['data']:
                        fk_info = []
                        for fk in fk_result['data']:
                            # fk[3] is the column in the current table, fk[2] is the referenced table, fk[4] is the referenced column.
                            fk_info.append(f"{fk[3]} -> {fk[2]}.{fk[4]}")
                        self.logger.info(f"  Foreign Keys: {', '.join(fk_info)}")
                    
                except Exception as e:
                    self.logger.error(f"Error getting info for table {table}: {e}")
            
            # Run additional integrity checks.
            self._check_for_database_issues()
            
        except Exception as e:
            self.logger.error(f"Error running database diagnostics: {e}")
        
        self.logger.info("===== END DATABASE DIAGNOSTICS =====")
    
    def _check_for_database_issues(self):
        """
        Check for common issues in the database structure and relationships.

        This includes:
            - Verifying that book-author relationships exist if books are present.
            - Checking foreign key integrity.
            - Warning about tables that exist without any records.
        """
        try:
            tables = self.get_all_tables()
            
            # Check for expected relationships between books and authors.
            if 'book' in tables and 'book_authors' in tables:
                book_count = self.execute_query("SELECT COUNT(*) FROM book")['data'][0][0]
                relationship_count = self.execute_query("SELECT COUNT(*) FROM book_authors")['data'][0][0]
                if book_count > 0 and relationship_count == 0:
                    self.logger.warning("Potential issue: Books exist but no book-author relationships found")
            
            # Check for foreign key integrity.
            integrity_issues = self.execute_query("PRAGMA foreign_key_check")
            if integrity_issues['data']:
                self.logger.warning(f"Foreign key integrity issues found: {integrity_issues['data']}")
            
            # Warn about tables that have no records.
            for table in tables:
                count_result = self.execute_query(f"SELECT COUNT(*) FROM {table}")
                if count_result['data'][0][0] == 0:
                    self.logger.warning(f"Table '{table}' exists but contains no records")
            
        except Exception as e:
            self.logger.error(f"Error checking for database issues: {e}")
    
    def _show_main_window(self):
        """
        Initialize and display the main application window.

        Notes:
            - This method instantiates the MainWindow with the configuration and then shows it.
        """
        try:
            self.app_window = MainWindow(self.config)
            self.app_window.show()
            self.logger.info("Main application window initialized and displayed")
        except Exception as e:
            self.logger.error(f"Error initializing main window: {e}")
            raise

    def get_all_tables(self):
        """
        Retrieve a list of all non-system tables from the database.

        Returns:
            list: A list of table names.

        Notes:
            - Tables starting with 'sqlite_' are ignored as they are system tables.
            - Returns an empty list if an error occurs.
        """
        try:
            cursor = self.db.conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            # Filter out SQLite internal tables.
            tables = [table[0] for table in cursor.fetchall() if not table[0].startswith('sqlite_')]
            return tables
        except Exception as e:
            self.logger.error(f"Error fetching tables: {e}")
            return []

    def execute_query(self, query, params=None):
        """
        Execute an SQL query and return the results.

        Args:
            query (str): The SQL query to execute.
            params (tuple, optional): Parameters to pass to the query.

        Returns:
            dict: Contains either:
                  - {"columns": list, "data": list} for SELECT/PRAGMA queries, or
                  - {"rows_affected": int} for non-SELECT queries.

        Raises:
            Exception: Propagates any errors encountered during query execution.
        """
        try:
            cursor = self.db.conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            # If the query is a SELECT or PRAGMA statement, fetch and return the data.
            if query.strip().upper().startswith("SELECT") or query.strip().upper().startswith("PRAGMA"):
                columns = [description[0] for description in cursor.description] if cursor.description else []
                results = cursor.fetchall()
                return {"columns": columns, "data": results}
            else:
                # For non-SELECT queries, commit changes and return the number of rows affected.
                self.db.conn.commit()
                return {"rows_affected": cursor.rowcount}
        except Exception as e:
            self.logger.error(f"Error executing query: {e}")
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
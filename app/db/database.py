# app/db/database.py
import sqlite3
import logging
from pathlib import Path
import threading

class Database:
    _local = threading.local()  # Thread-local storage
    
    def __init__(self, config=None):
        self.config = config
        self.db_path = config["data"]["database"]
        self.logger = logging.getLogger(__name__)
        self._connect()

    def _connect(self):
        """Establish connection to the SQLite database."""
        try:
            # Ensure the parent directory exists
            Path(self.db_path).parent.mkdir(exist_ok=True, parents=True)
            
            # Connect to the database with extended options
            self._local.conn = sqlite3.connect(
                self.db_path, 
                detect_types=sqlite3.PARSE_DECLTYPES
            )
            self._local.conn.row_factory = sqlite3.Row  # Allow column name access
            self._local.cursor = self._local.conn.cursor()
            
            # Enable foreign key constraints
            self._local.cursor.execute("PRAGMA foreign_keys = ON")
            
            self.logger.info(f"Database connection established: {self.db_path}")
        except sqlite3.Error as e:
            self.logger.error(f"Database connection error: {e}")
            raise
            
    @property
    def conn(self):
        """Get the thread-local connection."""
        if not hasattr(self._local, 'conn'):
            self._connect()
        return self._local.conn
        
    @property
    def cursor(self):
        """Get the thread-local cursor."""
        if not hasattr(self._local, 'cursor'):
            self._connect()
        return self._local.cursor
    
    def is_initialized(self):
        """
        Check if the database is initialized with the required schema.
        """
        try:
            self.cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
            tables = [row[0] for row in self.cursor.fetchall()]

            required_tables = [
                'book', 'author', 'book_authors', 
                'user', 'review', 'genre', 'book_genre'
            ]
            return all(table in tables for table in required_tables)
        except sqlite3.Error as e:
            self.logger.error(f"Error checking database initialization: {e}")
            return False

    def execute(self, query, params=None):
        """
        Execute an SQL query.
        """
        try:
            if params:
                return self.cursor.execute(query, params)
            else:
                return self.cursor.execute(query)
        except sqlite3.Error as e:
            self.logger.error(f"Query execution error: {e}")
            self.logger.debug(f"Query: {query}")
            raise

    def executemany(self, query, params_list):
        """
        Execute an SQL query with multiple parameter sets.
        """
        try:
            return self.cursor.executemany(query, params_list)
        except sqlite3.Error as e:
            self.logger.error(f"Batch query execution error: {e}")
            raise

    def commit(self):
        """Commit the current transaction."""
        self.conn.commit()

    def rollback(self):
        """Roll back the current transaction."""
        self.conn.rollback()

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            self.logger.info("Database connection closed")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup."""
        self.close()

    def optimize_for_bulk_import(self):
        """
        Configure SQLite settings to optimize performance for bulk data import.
        """
        try:
            self.execute("PRAGMA synchronous = OFF")
            self.execute("PRAGMA journal_mode = MEMORY")
            self.execute("PRAGMA temp_store = MEMORY")
            self.execute("PRAGMA cache_size = 100000")
            self.execute("PRAGMA foreign_keys = OFF")
            self.commit()
            self.logger.info("Database optimized for bulk import")
        except sqlite3.Error as e:
            self.logger.error(f"Error optimizing database for bulk import: {e}")
            raise

    def restore_normal_settings(self):
        """
        Restore normal SQLite settings after completing a bulk import.
        """
        try:
            self.execute("PRAGMA synchronous = NORMAL")
            self.execute("PRAGMA journal_mode = DELETE")
            self.execute("PRAGMA foreign_keys = ON")
            self.commit()
            self.logger.info("Database settings restored to normal")
        except sqlite3.Error as e:
            self.logger.error(f"Error restoring normal database settings: {e}")
            raise

    def get_file_downloader(self):
        """
        Get a FileDownloader instance (for dataset checks & downloads).
        """
        from app.db.downloader import FileDownloader
        return FileDownloader(self.config)

    def get_dataset_importer(self):
        """
        Get a DatasetImporter instance (for importing data into this database).
        """
        from app.db.importer import DatasetImporter
        return DatasetImporter(self.config, self)

    def initialize_schema(self):
        """Initialize the database schema with tables and initial data."""
        try:
            from app.db.models import initialize_database
            success = initialize_database(self)
            if success:
                self.logger.info("Database schema initialized successfully")
            return success
        except Exception as e:
            self.logger.error(f"Error initializing schema: {e}")
            return False
        
    def batch_insert_books(self, book_records):
        """Insert multiple book records efficiently."""
        try:
            self.executemany(
                """INSERT OR IGNORE INTO book 
                (book_id, title, description, isbn, isbn13, 
                    publisher, publication_date, language_code, pages,
                    average_rating, ratings_count, text_reviews_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", 
                book_records
            )
            self.commit()
            return True
        except sqlite3.Error as e:
            self.logger.error(f"Error inserting books: {e}")
            self.rollback()
            return False

    def batch_insert_authors(self, author_records):
        """Insert multiple author records efficiently."""
        try:
            self.executemany(
                """INSERT OR IGNORE INTO author 
                (author_id, name, role, profile_url)
                VALUES (?, ?, ?, ?)""", 
                author_records
            )
            self.commit()
            return True
        except sqlite3.Error as e:
            self.logger.error(f"Error inserting authors: {e}")
            self.rollback()
            return False
        
    def get_database_stats(self):
        """Get statistics about the database contents."""
        try:
            stats = {}
            
            # Count books
            self.execute("SELECT COUNT(*) FROM book")
            stats['books'] = self.cursor.fetchone()[0]
            
            # Count authors
            self.execute("SELECT COUNT(*) FROM author")
            stats['authors'] = self.cursor.fetchone()[0]
            
            # Count users
            self.execute("SELECT COUNT(*) FROM user")
            stats['users'] = self.cursor.fetchone()[0]
            
            # Count reviews
            self.execute("SELECT COUNT(*) FROM review")
            stats['reviews'] = self.cursor.fetchone()[0]
            
            return stats
        except sqlite3.Error as e:
            self.logger.error(f"Error getting database stats: {e}")
            return {"error": str(e)}
        
    def get_database_report(self):
        """Get a detailed report about the database contents.
        Should include a listing of all tables and a count of rows in each."""
        try:
            report = {}
            
            # Get table names
            self.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
            tables = [row[0] for row in self.cursor.fetchall()]
            
            # Get row counts for each table
            for table in tables:
                self.execute(f"SELECT COUNT(*) FROM {table}")
                report[table] = self.cursor.fetchone()[0]
            
            return report
        except sqlite3.Error as e:
            self.logger.error(f"Error getting database report: {e}")
            return {"error": str(e)}
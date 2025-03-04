import sqlite3
from datetime import datetime
import logging
from pathlib import Path
import os
import json

class AnalyticsDB:
    """
    Manages the SQLite database for the Goodreads Analytics Tool.

    This class handles connecting to the SQLite database, creating tables,
    performing data insertions (both single and batch), and applying optimizations
    for bulk data import.
    """
    def __init__(self, db_path="data/analytics.db"):
        """
        Initialize the AnalyticsDB instance.

        Args:
            db_path (str): Path to the SQLite database file.
        
        Actions:
            - Ensures that the parent directory for the database exists.
            - Establishes a connection to the SQLite database.
            - Creates the necessary tables if they do not exist.
        """
        self.logger = logging.getLogger(__name__)
        self.db_path = Path(db_path)
        
        try:
            # Ensure that the parent directory exists.
            self.db_path.parent.mkdir(exist_ok=True)
            
            # Connect to the database with extended options
            self.conn = sqlite3.connect(str(self.db_path), detect_types=sqlite3.PARSE_DECLTYPES)
            self.conn.row_factory = sqlite3.Row  # Allow column name access
            self.cursor = self.conn.cursor()
            
            # Enable foreign key constraints
            self.cursor.execute("PRAGMA foreign_keys = ON")
            
            # Create tables for the integrated schema.
            self.create_tables()
            
            # Store database metadata
            self._initialize_metadata()
            
            self.logger.info(f"Database connection established: {db_path}")
        except sqlite3.Error as e:
            self.logger.error(f"Database connection error: {e}")
            raise

    ########################################################
    # Metadata methods
    ########################################################
    
    def _initialize_metadata(self):
        """
        Initialize and load database metadata.
        
        Creates a metadata table if it doesn't exist and loads the current metadata.
        This metadata includes information about imported datasets and database version.
        """
        try:
            # Create metadata table if it doesn't exist
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS database_metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            self.conn.commit()
            
            # Load current metadata
            self.metadata = self._load_metadata()
            
            # If no version is set, initialize with version 1.0
            if 'version' not in self.metadata:
                self.set_metadata('version', '1.0')
                
            # Initialize datasets list if not present
            if 'datasets' not in self.metadata:
                self.set_metadata('datasets', json.dumps([]))
                
        except sqlite3.Error as e:
            self.logger.error(f"Error initializing metadata: {e}")
            raise
    
    def _load_metadata(self):
        """
        Load metadata from the database.
        
        Returns:
            dict: Dictionary containing all metadata key-value pairs.
        """
        metadata = {}
        try:
            self.cursor.execute("SELECT key, value FROM database_metadata")
            for row in self.cursor.fetchall():
                key = row[0]
                value = row[1]
                metadata[key] = value
            return metadata
        except sqlite3.Error as e:
            self.logger.error(f"Error loading metadata: {e}")
            return metadata
            
    def set_metadata(self, key, value):
        """
        Set or update a metadata value.
        
        Args:
            key (str): The metadata key.
            value (str): The metadata value.
        """
        try:
            self.cursor.execute("""
                INSERT OR REPLACE INTO database_metadata (key, value, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
            """, (key, value))
            self.conn.commit()
            self.metadata[key] = value
        except sqlite3.Error as e:
            self.logger.error(f"Error setting metadata {key}: {e}")
            
    def register_dataset(self, dataset_name, version=None, source=None):
        """
        Register a dataset as imported in the database.
        
        Args:
            dataset_name (str): Name of the dataset.
            version (str, optional): Version of the dataset.
            source (str, optional): Source of the dataset (e.g., 'kaggle').
            
        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            # Load current datasets
            datasets = self.get_registered_datasets()
            
            # Check if dataset already exists
            for i, dataset in enumerate(datasets):
                if dataset.get('name') == dataset_name:
                    # Update existing dataset
                    datasets[i] = {
                        'name': dataset_name,
                        'version': version or dataset.get('version'),
                        'source': source or dataset.get('source'),
                        'imported_at': datetime.now().isoformat()
                    }
                    self.set_metadata('datasets', json.dumps(datasets))
                    return True
            
            # Add new dataset
            datasets.append({
                'name': dataset_name,
                'version': version,
                'source': source,
                'imported_at': datetime.now().isoformat()
            })
            
            self.set_metadata('datasets', json.dumps(datasets))
            return True
        except Exception as e:
            self.logger.error(f"Error registering dataset {dataset_name}: {e}")
            return False
            
    def get_registered_datasets(self):
        """
        Get all registered datasets.
        
        Returns:
            list: List of dictionaries with dataset information.
        """
        try:
            datasets_json = self.metadata.get('datasets', '[]')
            return json.loads(datasets_json)
        except Exception as e:
            self.logger.error(f"Error parsing datasets metadata: {e}")
            return []
            
    def is_dataset_registered(self, dataset_name):
        """
        Check if a dataset is registered in the database.
        
        Args:
            dataset_name (str): Name of the dataset.
            
        Returns:
            bool: True if the dataset is registered, False otherwise.
        """
        datasets = self.get_registered_datasets()
        return any(d.get('name') == dataset_name for d in datasets)

    ########################################################
    # Table creation methods
    ########################################################

    def create_tables(self):
        """
        Create all necessary tables in the database.

        Tables created:
            - author: Stores author details with influence metrics and bio.
            - book: Stores book details including external ID and extended metadata.
            - book_authors: Many-to-many relationship between books and authors.
            - user: Stores reviewer details including review behavior metadata.
            - review: Stores review text and associated metadata.
            - genre: Categorizes books.
            - book_genre: Many-to-many relationship between books and genres.
            - ratings_history: Stores historical rating trends for books.
        """
        try:
            # Start a single transaction for all table creations
            self.conn.execute("BEGIN TRANSACTION")
            
            # Create the 'author' table.
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS author (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    birthdate DATE,
                    country TEXT,
                    influence_score REAL,
                    bio TEXT
                )
            """)

            # Create the 'book' table.
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS book (
                    id INTEGER PRIMARY KEY,
                    external_id INTEGER,
                    title TEXT NOT NULL,
                    subtitle TEXT,
                    publisher TEXT,
                    publication_date DATE,
                    language TEXT,
                    pages INTEGER,
                    description TEXT,
                    average_rating REAL,
                    ratings_count INTEGER,
                    text_reviews_count INTEGER,
                    dataset_source TEXT,
                    UNIQUE(external_id, dataset_source)
                )
            """)

            # Create the 'book_authors' table for many-to-many relationships.
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS book_authors (
                    book_id INTEGER,
                    author_id INTEGER,
                    PRIMARY KEY (book_id, author_id),
                    FOREIGN KEY (book_id) REFERENCES book (id) ON DELETE CASCADE,
                    FOREIGN KEY (author_id) REFERENCES author (id) ON DELETE CASCADE
                )
            """)

            # Create the 'user' table.
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS user (
                    id INTEGER PRIMARY KEY,
                    external_id INTEGER,
                    username TEXT UNIQUE,
                    review_count INTEGER DEFAULT 0,
                    avg_rating_given REAL,
                    demographics TEXT,
                    dataset_source TEXT,
                    UNIQUE(external_id, dataset_source)
                )
            """)

            # Create the 'review' table.
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS review (
                    id INTEGER PRIMARY KEY,
                    book_id INTEGER,
                    user_id INTEGER,
                    rating INTEGER,
                    review_text TEXT,
                    review_date DATE,
                    helpful_count INTEGER DEFAULT 0,
                    spoiler_flag BOOLEAN DEFAULT 0,
                    sentiment_score REAL,
                    dataset_source TEXT,
                    FOREIGN KEY (book_id) REFERENCES book (id) ON DELETE CASCADE,
                    FOREIGN KEY (user_id) REFERENCES user (id) ON DELETE CASCADE
                )
            """)

            # Create the 'genre' table.
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS genre (
                    id INTEGER PRIMARY KEY,
                    name TEXT UNIQUE,
                    description TEXT,
                    parent_genre_id INTEGER,
                    FOREIGN KEY (parent_genre_id) REFERENCES genre (id) ON DELETE SET NULL
                )
            """)

            # Create the 'book_genre' table for many-to-many relationships.
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS book_genre (
                    book_id INTEGER,
                    genre_id INTEGER,
                    relevance_score REAL,
                    PRIMARY KEY (book_id, genre_id),
                    FOREIGN KEY (book_id) REFERENCES book (id) ON DELETE CASCADE,
                    FOREIGN KEY (genre_id) REFERENCES genre (id) ON DELETE CASCADE
                )
            """)

            # Create the 'ratings_history' table.
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS ratings_history (
                    id INTEGER PRIMARY KEY,
                    book_id INTEGER,
                    snapshot_date DATE,
                    cum_ratings_count INTEGER,
                    avg_rating REAL,
                    FOREIGN KEY (book_id) REFERENCES book (id) ON DELETE CASCADE
                )
            """)
            
            # Commit the transaction
            self.conn.commit()
            self.logger.debug("Database tables created successfully")
        except sqlite3.Error as e:
            self.conn.rollback()
            self.logger.error(f"Error creating tables: {e}")
            raise

    ########################################################
    # Schema management methods
    ########################################################
    
    def get_schema_version(self):
        """
        Get the current schema version from the metadata.
        
        Returns:
            str: The current schema version.
        """
        return self.metadata.get('version', '1.0')
        
    def upgrade_schema(self, target_version=None):
        """
        Upgrade the database schema to the target version.
        
        Args:
            target_version (str, optional): The target version to upgrade to.
                If None, upgrade to the latest version.
                
        Returns:
            bool: True if upgrade was successful, False otherwise.
        """
        current_version = self.get_schema_version()
        
        # Define the upgrade path based on versions
        upgrades = {
            '1.0': self._upgrade_to_1_1,
            '1.1': self._upgrade_to_1_2,
            # Add more upgrade functions as needed
        }
        
        try:
            # Determine upgrades to apply
            versions = sorted(list(upgrades.keys()) + ['1.0'])
            current_idx = versions.index(current_version)
            
            if target_version and target_version in versions:
                target_idx = versions.index(target_version)
                if target_idx <= current_idx:
                    self.logger.info(f"Current version {current_version} is already at or newer than target {target_version}")
                    return True
            else:
                target_idx = len(versions) - 1
                
            # Apply each upgrade in sequence
            for i in range(current_idx, target_idx):
                version = versions[i]
                next_version = versions[i + 1]
                
                if version in upgrades:
                    self.logger.info(f"Upgrading schema from {version} to {next_version}")
                    upgrade_func = upgrades[version]
                    if not upgrade_func():
                        self.logger.error(f"Failed to upgrade from {version} to {next_version}")
                        return False
                    
                    # Update version in metadata
                    self.set_metadata('version', next_version)
                    
            return True
        except Exception as e:
            self.logger.error(f"Error upgrading schema: {e}")
            return False
            
    def _upgrade_to_1_1(self):
        """
        Upgrade schema from 1.0 to 1.1.
        
        Version 1.1 adds dataset_source columns to relevant tables.
        
        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            self.conn.execute("BEGIN TRANSACTION")
            
            # Add dataset_source column to book table if not exists
            self.cursor.execute("PRAGMA table_info(book)")
            columns = [info[1] for info in self.cursor.fetchall()]
            if 'dataset_source' not in columns:
                self.cursor.execute("ALTER TABLE book ADD COLUMN dataset_source TEXT")
                
            # Add dataset_source column to user table if not exists
            self.cursor.execute("PRAGMA table_info(user)")
            columns = [info[1] for info in self.cursor.fetchall()]
            if 'dataset_source' not in columns:
                self.cursor.execute("ALTER TABLE user ADD COLUMN dataset_source TEXT")
                
            # Add dataset_source column to review table if not exists
            self.cursor.execute("PRAGMA table_info(review)")
            columns = [info[1] for info in self.cursor.fetchall()]
            if 'dataset_source' not in columns:
                self.cursor.execute("ALTER TABLE review ADD COLUMN dataset_source TEXT")
                
            self.conn.commit()
            return True
        except sqlite3.Error as e:
            self.conn.rollback()
            self.logger.error(f"Error upgrading to 1.1: {e}")
            return False
            
    def _upgrade_to_1_2(self):
        """
        Upgrade schema from 1.1 to 1.2.
        
        Version 1.2 adds a genre hierarchy and relevance scores.
        
        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            self.conn.execute("BEGIN TRANSACTION")
            
            # Add description and parent_genre_id to genre table if not exists
            self.cursor.execute("PRAGMA table_info(genre)")
            columns = [info[1] for info in self.cursor.fetchall()]
            
            if 'description' not in columns:
                self.cursor.execute("ALTER TABLE genre ADD COLUMN description TEXT")
                
            if 'parent_genre_id' not in columns:
                self.cursor.execute("""
                    ALTER TABLE genre ADD COLUMN parent_genre_id INTEGER 
                    REFERENCES genre(id) ON DELETE SET NULL
                """)
                
            # Add relevance_score to book_genre table if not exists
            self.cursor.execute("PRAGMA table_info(book_genre)")
            columns = [info[1] for info in self.cursor.fetchall()]
            
            if 'relevance_score' not in columns:
                self.cursor.execute("ALTER TABLE book_genre ADD COLUMN relevance_score REAL")
                
            self.conn.commit()
            return True
        except sqlite3.Error as e:
            self.conn.rollback()
            self.logger.error(f"Error upgrading to 1.2: {e}")
            return False

    ########################################################
    # Data insertion methods
    ########################################################

    def insert_author(self, name, birthdate=None, country=None, influence_score=None, bio=None):
        """
        Insert a new author record into the database.

        Args:
            name (str): Author's name.
            birthdate (str, optional): Author's birthdate.
            country (str, optional): Country of the author.
            influence_score (float, optional): Calculated influence score.
            bio (str, optional): A short biography.

        Returns:
            int: The row ID of the inserted author.
        """
        try:
            self.cursor.execute("""
                INSERT INTO author (name, birthdate, country, influence_score, bio)
                VALUES (?, ?, ?, ?, ?)
            """, (name, birthdate, country, influence_score, bio))
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            self.conn.rollback()
            self.logger.error(f"Error inserting author: {e}")
            raise

    def insert_book(self, external_id=None, title=None, subtitle=None, publisher=None, 
                    publication_date=None, language=None, pages=None, description=None, 
                    average_rating=None, ratings_count=None, text_reviews_count=None,
                    dataset_source=None):
        """
        Insert a new book record into the database.

        Args:
            external_id (int, optional): External identifier for the book.
            title (str): Title of the book.
            subtitle (str, optional): Subtitle of the book.
            publisher (str, optional): Publisher name.
            publication_date (str, optional): Date of publication.
            language (str, optional): Language of the book.
            pages (int, optional): Number of pages.
            description (str, optional): Book description.
            average_rating (float, optional): Average rating.
            ratings_count (int, optional): Number of ratings.
            text_reviews_count (int, optional): Number of text reviews.
            dataset_source (str, optional): Source dataset of this book.

        Returns:
            int: The row ID of the inserted book.
        """
        try:
            self.cursor.execute("""
                INSERT INTO book (external_id, title, subtitle, publisher, publication_date, 
                                language, pages, description, average_rating, 
                                ratings_count, text_reviews_count, dataset_source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (external_id, title, subtitle, publisher, publication_date, language, 
                pages, description, average_rating, ratings_count, text_reviews_count, dataset_source))
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            self.conn.rollback()
            self.logger.error(f"Error inserting book: {e}")
            raise

    def get_book_id_by_external_id(self, external_id, dataset_source=None):
        """
        Retrieve the internal book ID using an external ID.

        Args:
            external_id (int): External identifier of the book.
            dataset_source (str, optional): Source dataset of this book.

        Returns:
            int or None: The internal book ID if found, else None.
        """
        try:
            if dataset_source:
                self.cursor.execute(
                    "SELECT id FROM book WHERE external_id = ? AND dataset_source = ?", 
                    (external_id, dataset_source)
                )
            else:
                self.cursor.execute("SELECT id FROM book WHERE external_id = ?", (external_id,))
                
            result = self.cursor.fetchone()
            return result[0] if result else None
        except sqlite3.Error as e:
            self.logger.error(f"Error getting book ID: {e}")
            return None

    def insert_book_author(self, book_id, author_id):
        """
        Link a book and an author to establish a many-to-many relationship.

        Args:
            book_id (int): Internal ID of the book.
            author_id (int): Internal ID of the author.
        """
        try:
            self.cursor.execute("""
                INSERT OR IGNORE INTO book_authors (book_id, author_id)
                VALUES (?, ?)
            """, (book_id, author_id))
            self.conn.commit()
        except sqlite3.Error as e:
            self.conn.rollback()
            self.logger.error(f"Error inserting book-author relationship: {e}")
            raise

    def insert_user(self, external_id=None, username=None, review_count=0, 
                    avg_rating_given=None, demographics=None, dataset_source=None):
        """
        Insert a new user (reviewer) record into the database.

        Args:
            external_id (int, optional): External identifier for the user.
            username (str): Unique username.
            review_count (int, optional): Number of reviews written.
            avg_rating_given (float, optional): Average rating given by the user.
            demographics (str, optional): Demographic information.
            dataset_source (str, optional): Source dataset of this user.

        Returns:
            int: The row ID of the inserted user.
        """
        try:
            self.cursor.execute("""
                INSERT OR IGNORE INTO user 
                (external_id, username, review_count, avg_rating_given, demographics, dataset_source)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (external_id, username, review_count, avg_rating_given, demographics, dataset_source))
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            self.conn.rollback()
            self.logger.error(f"Error inserting user: {e}")
            raise

    def get_user_id_by_external_id(self, external_id, dataset_source=None):
        """
        Retrieve the internal user ID using an external ID.

        Args:
            external_id (int): External identifier of the user.
            dataset_source (str, optional): Source dataset of this user.

        Returns:
            int or None: The internal user ID if found, else None.
        """
        try:
            if dataset_source:
                self.cursor.execute(
                    "SELECT id FROM user WHERE external_id = ? AND dataset_source = ?", 
                    (external_id, dataset_source)
                )
            else:
                self.cursor.execute("SELECT id FROM user WHERE external_id = ?", (external_id,))
                
            result = self.cursor.fetchone()
            return result[0] if result else None
        except sqlite3.Error as e:
            self.logger.error(f"Error getting user ID: {e}")
            return None

    def insert_review(self, book_id, user_id, rating, review_text=None, review_date=None,
                      helpful_count=0, spoiler_flag=0, sentiment_score=None, dataset_source=None):
        """
        Insert a new review record into the database.

        Args:
            book_id (int): Internal book ID.
            user_id (int): Internal user ID.
            rating (int): Rating given in the review.
            review_text (str, optional): Full text of the review.
            review_date (str, optional): Date of the review.
            helpful_count (int, optional): Number of helpful votes.
            spoiler_flag (bool/int, optional): Indicates if the review contains spoilers.
            sentiment_score (float, optional): Calculated sentiment score.
            dataset_source (str, optional): Source dataset of this review.

        Returns:
            int: The row ID of the inserted review.
        """
        try:
            self.cursor.execute("""
                INSERT INTO review 
                (book_id, user_id, rating, review_text, review_date, 
                helpful_count, spoiler_flag, sentiment_score, dataset_source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (book_id, user_id, rating, review_text, review_date, 
                helpful_count, spoiler_flag, sentiment_score, dataset_source))
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            self.conn.rollback()
            self.logger.error(f"Error inserting review: {e}")
            raise

    def insert_rating_history(self, book_id, snapshot_date, cum_ratings_count, avg_rating):
        """
        Insert a new historical rating record for a book.

        Args:
            book_id (int): Internal book ID.
            snapshot_date (str): Date of the rating snapshot.
            cum_ratings_count (int): Cumulative number of ratings at the snapshot.
            avg_rating (float): Average rating at the snapshot.

        Returns:
            int: The row ID of the inserted record.
        """
        try:
            self.cursor.execute("""
                INSERT INTO ratings_history (book_id, snapshot_date, cum_ratings_count, avg_rating)
                VALUES (?, ?, ?, ?)
            """, (book_id, snapshot_date, cum_ratings_count, avg_rating))
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            self.conn.rollback()
            self.logger.error(f"Error inserting rating history: {e}")
            raise

    ########################################################
    # Closing the database connection
    ########################################################

    def close(self):
        """
        Close the database connection.

        This method should be called when the database is no longer needed to ensure
        that all resources are freed properly.
        """
        if hasattr(self, 'conn') and self.conn:
            try:
                self.conn.close()
                self.logger.info("Database connection closed")
            except sqlite3.Error as e:
                self.logger.error(f"Error closing database connection: {e}")

    def __del__(self):
        # Ensure that the database connection is closed when the object is deleted.
        try:
            self.close()
        except Exception:
            # Suppress any exception to avoid issues during interpreter shutdown.
            pass

    ########################################################
    # Optimization and batch import methods
    ########################################################

    def optimize_for_bulk_import(self):
        """
        Configure SQLite settings to optimize performance for bulk data import.

        Changes include:
            - Disabling synchronous writes.
            - Setting the journal mode to MEMORY.
            - Storing temporary data in memory.
            - Increasing the cache size.
            - Disabling foreign key checks.
        """
        try:
            self.cursor.execute("PRAGMA synchronous = OFF")
            self.cursor.execute("PRAGMA journal_mode = MEMORY")
            self.cursor.execute("PRAGMA temp_store = MEMORY")
            self.cursor.execute("PRAGMA cache_size = 100000")
            self.cursor.execute("PRAGMA foreign_keys = OFF")
            self.conn.commit()
            self.logger.info("Database optimized for bulk import")
        except sqlite3.Error as e:
            self.logger.error(f"Error optimizing database for bulk import: {e}")
            raise
        
    def restore_normal_settings(self):
        """
        Restore normal SQLite settings after completing a bulk import.

        Changes include:
            - Restoring synchronous writes to NORMAL.
            - Restoring the journal mode to DELETE.
            - Enabling foreign key checks.
        """
        try:
            self.cursor.execute("PRAGMA synchronous = NORMAL")
            self.cursor.execute("PRAGMA journal_mode = DELETE")
            self.cursor.execute("PRAGMA foreign_keys = ON")
            self.conn.commit()
            self.logger.info("Database settings restored to normal")
        except sqlite3.Error as e:
            self.logger.error(f"Error restoring normal database settings: {e}")
            raise
        
    def batch_insert_books(self, books_data):
        """
        Batch insert multiple book records for improved performance.

        Args:
            books_data (list of tuple): Each tuple contains parameters corresponding
                                        to the columns of the 'book' table.

        Returns:
            bool: True if the batch insert is successful, otherwise False.
        """
        if not books_data:
            return True
            
        # Start a transaction.
        try:
            self.conn.execute("BEGIN TRANSACTION")
            
            sql = """
                INSERT OR IGNORE INTO book 
                (external_id, title, subtitle, publisher, publication_date, language, pages, 
                description, average_rating, ratings_count, text_reviews_count, dataset_source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            self.cursor.executemany(sql, books_data)
            self.conn.commit()
            self.logger.info(f"Batch inserted {len(books_data)} books")
            return True
        except sqlite3.Error as e:
            self.conn.rollback()
            self.logger.error(f"Error in batch book insert: {e}")
            return False
            
    def batch_insert_authors(self, authors_data):
        """
        Batch insert multiple author records.

        Args:
            authors_data (list of tuple): Each tuple contains parameters corresponding
                                          to the columns of the 'author' table.

        Returns:
            bool: True if successful, otherwise False.
        """
        if not authors_data:
            return True
            
        try:
            self.conn.execute("BEGIN TRANSACTION")
            
            sql = """
                INSERT OR IGNORE INTO author
                (name, birthdate, country, influence_score, bio)
                VALUES (?, ?, ?, ?, ?)
            """
            self.cursor.executemany(sql, authors_data)
            self.conn.commit()
            self.logger.info(f"Batch inserted {len(authors_data)} authors")
            return True
        except sqlite3.Error as e:
            self.conn.rollback()
            self.logger.error(f"Error in batch author insert: {e}")
            return False
            
    def batch_insert_book_authors(self, book_author_data):
        """
        Batch insert multiple book-author relationship records.

        Args:
            book_author_data (list of tuple): Each tuple contains (book_id, author_id).

        Returns:
            bool: True if successful, otherwise False.
        """
        if not book_author_data:
            return True
            
        try:
            self.conn.execute("BEGIN TRANSACTION")
            
            sql = """
                INSERT OR IGNORE INTO book_authors
                (book_id, author_id)
                VALUES (?, ?)
            """
            self.cursor.executemany(sql, book_author_data)
            self.conn.commit()
            self.logger.info(f"Batch inserted {len(book_author_data)} book-author relationships")
            return True
        except sqlite3.Error as e:
            self.conn.rollback()
            self.logger.error(f"Error in batch book-author insert: {e}")
            return False

    def batch_insert_users(self, users_data):
        """
        Batch insert multiple user records.

        Args:
            users_data (list of tuple): Each tuple contains parameters corresponding
                                        to the columns of the 'user' table.

        Returns:
            bool: True if successful, otherwise False.
        """
        if not users_data:
            return True
            
        try:
            self.conn.execute("BEGIN TRANSACTION")
            
            sql = """
                INSERT OR IGNORE INTO user
                (external_id, username, review_count, avg_rating_given, demographics, dataset_source)
                VALUES (?, ?, ?, ?, ?, ?)
            """
            self.cursor.executemany(sql, users_data)
            self.conn.commit()
            self.logger.info(f"Batch inserted {len(users_data)} users")
            return True
        except sqlite3.Error as e:
            self.conn.rollback()
            self.logger.error(f"Error in batch user insert: {e}")
            return False

    def batch_insert_reviews(self, reviews_data):
        """
        Batch insert multiple review records.

        Args:
            reviews_data (list of tuple): Each tuple contains parameters corresponding
                                          to the columns of the 'review' table.

        Returns:
            bool: True if successful, otherwise False.
        """
        if not reviews_data:
            return True
            
        try:
            self.conn.execute("BEGIN TRANSACTION")
            
            sql = """
                INSERT OR IGNORE INTO review
                (book_id, user_id, rating, review_text, review_date, 
                helpful_count, spoiler_flag, sentiment_score, dataset_source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            self.cursor.executemany(sql, reviews_data)
            self.conn.commit()
            self.logger.info(f"Batch inserted {len(reviews_data)} reviews")
            return True
        except sqlite3.Error as e:
            self.conn.rollback()
            self.logger.error(f"Error in batch review insert: {e}")
            return False
            
    def verify_database_contents(self):
        """
        Verify the contents of the database by summarizing record counts for each table.

        Returns:
            dict: A summary where keys are table names and values are the number of records.
        """
        tables = ["book", "author", "user", "review", "book_authors", "genre", "book_genre", "ratings_history"]
        summary = {}
        
        self.logger.info("Database verification:")
        for table in tables:
            try:
                self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = self.cursor.fetchone()[0]
                summary[table] = count
                self.logger.info(f"  {table}: {count} records")
            except Exception as e:
                self.logger.error(f"  Error verifying {table}: {e}")
                summary[table] = f"Error: {e}"
                
        return summary
    
    ########################################################
    # Dataset checking methods
    ########################################################
    
    def check_dataset_exists(self, dataset_name, required_tables=None, min_records=None):
        """
        Check if the required dataset is already loaded into the database.
        
        Args:
            dataset_name (str): Name of the dataset to check.
            required_tables (list, optional): List of tables that must exist and have records.
                                             Defaults to book, review, and genre tables.
            min_records (dict, optional): Dictionary mapping table names to minimum record counts.
                                         Defaults to reasonable minimums for each table.
                                         
        Returns:
            bool: True if the dataset exists and meets the criteria, False otherwise.
        """
        # First check if dataset is registered in metadata
        if self.is_dataset_registered(dataset_name):
            self.logger.info(f"Dataset '{dataset_name}' is registered in database metadata")
        else:
            self.logger.info(f"Dataset '{dataset_name}' is not registered in database metadata")
            # If a dataset isn't registered, check tables to see if it might exist anyway
        
        if required_tables is None:
            required_tables = ["book", "review", "genre"]
            
        if min_records is None:
            min_records = {
                "book": 1000,
                "review": 5000,
                "genre": 10
            }
            
        try:
            # Check if all required tables exist and have records associated with this dataset
            for table in required_tables:
                if table in ["book", "user", "review"]:
                    # These tables can include dataset_source
                    sql = f"SELECT COUNT(*) FROM {table} WHERE dataset_source = ?"
                    self.cursor.execute(sql, (dataset_name,))
                    count = self.cursor.fetchone()[0]
                    
                    min_count = min_records.get(table, 1)
                    if count < min_count:
                        self.logger.info(f"Table '{table}' has only {count} records for dataset '{dataset_name}' (minimum {min_count} needed)")
                        return False
                else:
                    # For tables without dataset_source, just check general record count
                    sql = f"SELECT COUNT(*) FROM {table}"
                    self.cursor.execute(sql)
                    count = self.cursor.fetchone()[0]
                    
                    min_count = min_records.get(table, 1)
                    if count < min_count:
                        self.logger.info(f"Table '{table}' has only {count} records (minimum {min_count} needed)")
                        return False
            
            self.logger.info(f"Dataset '{dataset_name}' verification successful - all required data is present")
            return True
            
        except sqlite3.Error as e:
            self.logger.error(f"Error checking dataset existence: {e}")
            return False
    
    ########################################################
    # Query methods
    ########################################################
    
    def execute_query(self, query, params=None):
        """
        Execute an SQL query and return the results.
        
        Args:
            query (str): SQL query to execute.
            params (tuple, optional): Parameters for the query.
            
        Returns:
            dict: For SELECT queries, returns a dictionary with 'columns' and 'data' keys.
                 For other queries, returns a dictionary with 'rows_affected' key.
                 
        Raises:
            Exception: If the query execution fails.
        """
        try:
            cursor = self.conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
                
            # If SELECT or PRAGMA query, return results
            if query.strip().upper().startswith("SELECT") or query.strip().upper().startswith("PRAGMA"):
                columns = [description[0] for description in cursor.description] if cursor.description else []
                results = cursor.fetchall()
                return {"columns": columns, "data": results}
            else:
                # For non-SELECT queries, commit and return affected rows
                self.conn.commit()
                return {"rows_affected": cursor.rowcount}
        except sqlite3.Error as e:
            if not query.strip().upper().startswith("SELECT") and not query.strip().upper().startswith("PRAGMA"):
                self.conn.rollback()
            self.logger.error(f"Error executing query: {e}")
            raise
    
    def search_books(self, query, limit=50, offset=0):
        """
        Search for books using a text query.
        
        Args:
            query (str): Search query.
            limit (int, optional): Maximum number of results.
            offset (int, optional): Offset for pagination.
            
        Returns:
            list: List of book dictionaries.
        """
        try:
            search_terms = query.split()
            sql_conditions = []
            params = []
            
            # Build search conditions
            for term in search_terms:
                sql_conditions.append("(title LIKE ? OR authors.name LIKE ?)")
                params.extend([f"%{term}%", f"%{term}%"])
                
            sql = """
                SELECT book.id, book.title, book.publication_date, book.average_rating,
                       GROUP_CONCAT(DISTINCT author.name) AS authors,
                       GROUP_CONCAT(DISTINCT genre.name) AS genres
                FROM book
                LEFT JOIN book_authors ON book.id = book_authors.book_id
                LEFT JOIN author ON book_authors.author_id = author.id
                LEFT JOIN book_genre ON book.id = book_genre.book_id
                LEFT JOIN genre ON book_genre.genre_id = genre.id
                WHERE {}
                GROUP BY book.id
                ORDER BY book.average_rating DESC
                LIMIT ? OFFSET ?
            """.format(" OR ".join(sql_conditions))
            
            params.extend([limit, offset])
            
            self.cursor.execute(sql, params)
            
            # Convert to list of dictionaries
            columns = [column[0] for column in self.cursor.description]
            results = []
            for row in self.cursor.fetchall():
                result = dict(zip(columns, row))
                results.append(result)
                
            return results
        except sqlite3.Error as e:
            self.logger.error(f"Error searching books: {e}")
            return []
            
    def get_book_details(self, book_id):
        """
        Get detailed information about a book.
        
        Args:
            book_id (int): Internal ID of the book.
            
        Returns:
            dict: Book details including authors, genres, and review summary.
        """
        try:
            # Get book info
            self.cursor.execute("""
                SELECT b.*, GROUP_CONCAT(DISTINCT a.name) AS authors
                FROM book b
                LEFT JOIN book_authors ba ON b.id = ba.book_id
                LEFT JOIN author a ON ba.author_id = a.id
                WHERE b.id = ?
                GROUP BY b.id
            """, (book_id,))
            
            book_row = self.cursor.fetchone()
            if not book_row:
                return None
                
            book = dict(book_row)
            
            # Get genres
            self.cursor.execute("""
                SELECT g.name
                FROM genre g
                JOIN book_genre bg ON g.id = bg.genre_id
                WHERE bg.book_id = ?
            """, (book_id,))
            
            book['genres'] = [row[0] for row in self.cursor.fetchall()]
            
            # Get review summary
            self.cursor.execute("""
                SELECT COUNT(*) AS review_count, AVG(rating) AS avg_rating
                FROM review
                WHERE book_id = ?
            """, (book_id,))
            
            review_summary = dict(self.cursor.fetchone())
            book.update(review_summary)
            
            # Get recent reviews
            self.cursor.execute("""
                SELECT r.id, r.rating, r.review_text, r.review_date, u.username
                FROM review r
                JOIN user u ON r.user_id = u.id
                WHERE r.book_id = ?
                ORDER BY r.review_date DESC
                LIMIT 5
            """, (book_id,))
            
            columns = [column[0] for column in self.cursor.description]
            book['recent_reviews'] = [dict(zip(columns, row)) for row in self.cursor.fetchall()]
            
            return book
        except sqlite3.Error as e:
            self.logger.error(f"Error getting book details: {e}")
            return None
    
    def get_analytics_summary(self):
        """
        Get a summary of database analytics for dashboard display.
        
        Returns:
            dict: Summary statistics about the database.
        """
        try:
            summary = {}
            
            # Get total counts
            tables = ["book", "author", "user", "review", "genre"]
            for table in tables:
                self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
                summary[f"total_{table}s"] = self.cursor.fetchone()[0]
                
            # Get average rating
            self.cursor.execute("SELECT AVG(rating) FROM review")
            summary["average_rating"] = self.cursor.fetchone()[0]
            
            # Get top genres
            self.cursor.execute("""
                SELECT g.name, COUNT(bg.book_id) AS book_count
                FROM genre g
                JOIN book_genre bg ON g.id = bg.genre_id
                GROUP BY g.id
                ORDER BY book_count DESC
                LIMIT 5
            """)
            
            summary["top_genres"] = [{"name": row[0], "count": row[1]} 
                                    for row in self.cursor.fetchall()]
            
            # Get top authors
            self.cursor.execute("""
                SELECT a.name, COUNT(ba.book_id) AS book_count, AVG(b.average_rating) AS avg_rating
                FROM author a
                JOIN book_authors ba ON a.id = ba.author_id
                JOIN book b ON ba.book_id = b.id
                GROUP BY a.id
                ORDER BY book_count DESC
                LIMIT 5
            """)
            
            summary["top_authors"] = [{"name": row[0], "book_count": row[1], "avg_rating": row[2]} 
                                      for row in self.cursor.fetchall()]
            
            # Get dataset info
            summary["datasets"] = self.get_registered_datasets()
            
            return summary
        except sqlite3.Error as e:
            self.logger.error(f"Error getting analytics summary: {e}")
            return {}
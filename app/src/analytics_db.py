import sqlite3
from datetime import datetime
import logging
from pathlib import Path

class AnalyticsDB:
    def __init__(self, db_path="data/analytics.db"):
        self.logger = logging.getLogger(__name__)
        self.db_path = Path(db_path)
        # Ensure parent directory exists
        self.db_path.parent.mkdir(exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path))
        self.cursor = self.conn.cursor()
        # Create tables for the integrated schema
        self.create_tables()
        self.logger.info(f"Database connection established: {db_path}")

    ########################################################
    # Table creation methods
    ########################################################

    def create_tables(self):
        # Author table: extended with influence metrics and bio.
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
        self.conn.commit()

        # Book table: extended metadata for each book.
        # Note: We allow external_id to be explicitly set for imported data
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
                UNIQUE(external_id)
            )
        """)
        self.conn.commit()

        # Many-to-many relationship between books and authors.
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS book_authors (
                book_id INTEGER,
                author_id INTEGER,
                PRIMARY KEY (book_id, author_id),
                FOREIGN KEY (book_id) REFERENCES book (id),
                FOREIGN KEY (author_id) REFERENCES author (id)
            )
        """)
        self.conn.commit()

        # User table for reviewers: includes review behavior metadata.
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS user (
                id INTEGER PRIMARY KEY,
                external_id INTEGER,
                username TEXT UNIQUE,
                review_count INTEGER DEFAULT 0,
                avg_rating_given REAL,
                demographics TEXT,
                UNIQUE(external_id)
            )
        """)
        self.conn.commit()

        # Reviews table: stores full review text, timestamps, helpful counts, spoiler flags, and sentiment.
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
                FOREIGN KEY (book_id) REFERENCES book (id),
                FOREIGN KEY (user_id) REFERENCES user (id)
            )
        """)
        self.conn.commit()

        # Genre table for categorizing books.
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS genre (
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE
            )
        """)
        self.conn.commit()

        # Many-to-many relationship between books and genres.
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS book_genre (
                book_id INTEGER,
                genre_id INTEGER,
                PRIMARY KEY (book_id, genre_id),
                FOREIGN KEY (book_id) REFERENCES book (id),
                FOREIGN KEY (genre_id) REFERENCES genre (id)
            )
        """)
        self.conn.commit()

        # RatingsHistory table for storing historical rating trends.
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS ratings_history (
                id INTEGER PRIMARY KEY,
                book_id INTEGER,
                snapshot_date DATE,
                cum_ratings_count INTEGER,
                avg_rating REAL,
                FOREIGN KEY (book_id) REFERENCES book (id)
            )
        """)
        self.conn.commit()

    ########################################################
    # Data insertion methods
    ########################################################

    def insert_author(self, name, birthdate=None, country=None, influence_score=None, bio=None):
        """Insert a new author into the database."""
        self.cursor.execute("""
            INSERT INTO author (name, birthdate, country, influence_score, bio)
            VALUES (?, ?, ?, ?, ?)
        """, (name, birthdate, country, influence_score, bio))
        self.conn.commit()
        return self.cursor.lastrowid

    def insert_book(self, external_id=None, title=None, subtitle=None, publisher=None, 
                    publication_date=None, language=None, pages=None, description=None, 
                    average_rating=None, ratings_count=None, text_reviews_count=None):
        """Insert a new book into the database with optional external ID."""
        self.cursor.execute("""
            INSERT INTO book (external_id, title, subtitle, publisher, publication_date, 
                             language, pages, description, average_rating, 
                             ratings_count, text_reviews_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (external_id, title, subtitle, publisher, publication_date, language, 
              pages, description, average_rating, ratings_count, text_reviews_count))
        self.conn.commit()
        return self.cursor.lastrowid

    def get_book_id_by_external_id(self, external_id):
        """Get internal book ID from external ID."""
        self.cursor.execute("SELECT id FROM book WHERE external_id = ?", (external_id,))
        result = self.cursor.fetchone()
        return result[0] if result else None

    def insert_book_author(self, book_id, author_id):
        """Link a book and an author (many-to-many relationship)."""
        self.cursor.execute("""
            INSERT OR IGNORE INTO book_authors (book_id, author_id)
            VALUES (?, ?)
        """, (book_id, author_id))
        self.conn.commit()

    def insert_user(self, external_id=None, username=None, review_count=0, 
                  avg_rating_given=None, demographics=None):
        """Insert a new user into the database with optional external ID."""
        self.cursor.execute("""
            INSERT OR IGNORE INTO user (external_id, username, review_count, avg_rating_given, demographics)
            VALUES (?, ?, ?, ?, ?)
        """, (external_id, username, review_count, avg_rating_given, demographics))
        self.conn.commit()
        return self.cursor.lastrowid

    def get_user_id_by_external_id(self, external_id):
        """Get internal user ID from external ID."""
        self.cursor.execute("SELECT id FROM user WHERE external_id = ?", (external_id,))
        result = self.cursor.fetchone()
        return result[0] if result else None

    def insert_review(self, book_id, user_id, rating, review_text=None, review_date=None,
                      helpful_count=0, spoiler_flag=0, sentiment_score=None):
        """Insert a new review into the database."""
        self.cursor.execute("""
            INSERT INTO review (book_id, user_id, rating, review_text, review_date, 
                               helpful_count, spoiler_flag, sentiment_score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (book_id, user_id, rating, review_text, review_date, 
              helpful_count, spoiler_flag, sentiment_score))
        self.conn.commit()
        return self.cursor.lastrowid

    def insert_rating_history(self, book_id, snapshot_date, cum_ratings_count, avg_rating):
        """Insert a new historical rating record for a book."""
        self.cursor.execute("""
            INSERT INTO ratings_history (book_id, snapshot_date, cum_ratings_count, avg_rating)
            VALUES (?, ?, ?, ?)
        """, (book_id, snapshot_date, cum_ratings_count, avg_rating))
        self.conn.commit()
        return self.cursor.lastrowid

    ########################################################
    # Closing the database connection
    ########################################################

    def close(self):
        """Close the database connection."""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
            self.logger.info("Database connection closed")

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    ########################################################
    # Optimization and batch import methods
    ########################################################

    def optimize_for_bulk_import(self):
        """Configure SQLite settings for optimal bulk import performance"""
        self.cursor.execute("PRAGMA synchronous = OFF")
        self.cursor.execute("PRAGMA journal_mode = MEMORY")
        self.cursor.execute("PRAGMA temp_store = MEMORY")
        self.cursor.execute("PRAGMA cache_size = 100000")
        self.cursor.execute("PRAGMA foreign_keys = OFF")
        self.conn.commit()
        self.logger.info("Database optimized for bulk import")
        
    def restore_normal_settings(self):
        """Restore normal SQLite settings after bulk import"""
        self.cursor.execute("PRAGMA synchronous = NORMAL")
        self.cursor.execute("PRAGMA journal_mode = DELETE")
        self.cursor.execute("PRAGMA foreign_keys = ON")
        self.conn.commit()
        self.logger.info("Database settings restored to normal")
        
    def batch_insert_books(self, books_data):
        """Insert multiple books at once for better performance
        
        Args:
            books_data: List of tuples (external_id, title, subtitle, etc.) matching
                    the parameters of insert_book method
        """
        if not books_data:
            return
            
        # Start a transaction
        self.conn.execute("BEGIN TRANSACTION")
        
        try:
            # Prepare the SQL statement
            sql = """
                INSERT OR IGNORE INTO book 
                (external_id, title, subtitle, publisher, publication_date, language, pages, 
                description, average_rating, ratings_count, text_reviews_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            # Execute for multiple records
            self.cursor.executemany(sql, books_data)
            self.conn.commit()
            self.logger.info(f"Batch inserted {len(books_data)} books")
            return True
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Error in batch book insert: {e}")
            return False
            
    def batch_insert_authors(self, authors_data):
        """Insert multiple authors at once
        
        Args:
            authors_data: List of tuples (name, birthdate, country, etc.) matching 
                        the parameters of insert_author method
        """
        if not authors_data:
            return
            
        # Start a transaction
        self.conn.execute("BEGIN TRANSACTION")
        
        try:
            # Prepare the SQL statement
            sql = """
                INSERT OR IGNORE INTO author
                (name, birthdate, country, influence_score, bio)
                VALUES (?, ?, ?, ?, ?)
            """
            
            # Execute for multiple records
            self.cursor.executemany(sql, authors_data)
            self.conn.commit()
            self.logger.info(f"Batch inserted {len(authors_data)} authors")
            return True
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Error in batch author insert: {e}")
            return False
            
    def batch_insert_book_authors(self, book_author_data):
        """Insert multiple book-author relationships at once
        
        Args:
            book_author_data: List of tuples (book_id, author_id)
        """
        if not book_author_data:
            return
            
        # Start a transaction
        self.conn.execute("BEGIN TRANSACTION")
        
        try:
            # Prepare the SQL statement
            sql = """
                INSERT OR IGNORE INTO book_authors
                (book_id, author_id)
                VALUES (?, ?)
            """
            
            # Execute for multiple records
            self.cursor.executemany(sql, book_author_data)
            self.conn.commit()
            self.logger.info(f"Batch inserted {len(book_author_data)} book-author relationships")
            return True
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Error in batch book-author insert: {e}")
            return False

    def batch_insert_users(self, users_data):
        """Insert multiple users at once
        
        Args:
            users_data: List of tuples (external_id, username, review_count, etc.)
        """
        if not users_data:
            return
            
        # Start a transaction
        self.conn.execute("BEGIN TRANSACTION")
        
        try:
            # Prepare the SQL statement
            sql = """
                INSERT OR IGNORE INTO user
                (external_id, username, review_count, avg_rating_given, demographics)
                VALUES (?, ?, ?, ?, ?)
            """
            
            # Execute for multiple records
            self.cursor.executemany(sql, users_data)
            self.conn.commit()
            self.logger.info(f"Batch inserted {len(users_data)} users")
            return True
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Error in batch user insert: {e}")
            return False

    def batch_insert_reviews(self, reviews_data):
        """Insert multiple reviews at once
        
        Args:
            reviews_data: List of tuples (book_id, user_id, rating, review_text, etc.)
        """
        if not reviews_data:
            return
            
        # Start a transaction
        self.conn.execute("BEGIN TRANSACTION")
        
        try:
            # Prepare the SQL statement
            sql = """
                INSERT OR IGNORE INTO review
                (book_id, user_id, rating, review_text, review_date, 
                helpful_count, spoiler_flag, sentiment_score)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            # Execute for multiple records
            self.cursor.executemany(sql, reviews_data)
            self.conn.commit()
            self.logger.info(f"Batch inserted {len(reviews_data)} reviews")
            return True
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Error in batch review insert: {e}")
            return False
            
    def verify_database_contents(self):
        """Print summary of database contents for verification."""
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
                
        return summary
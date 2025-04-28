# app/db/models.py
"""Database schema for the Goodreads Analytics application.

This module defines the database schema and provides functions to create
and initialize the database tables.
"""

import sqlite3
import logging

logger = logging.getLogger(__name__)

def create_tables(db):
    """
    Create all necessary tables in the database.
    
    Args:
        db (Database): Database connection instance
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Start a transaction for all table creations
        db.execute("BEGIN TRANSACTION")
        
        # Create the 'author' table
        db.execute("""
            CREATE TABLE IF NOT EXISTS author (
                id INTEGER PRIMARY KEY,
                author_id TEXT,
                name TEXT NOT NULL,
                role TEXT,
                profile_url TEXT,
                UNIQUE(author_id)
            )
        """)
        
        # Create the 'book' table
        db.execute("""
            CREATE TABLE IF NOT EXISTS book (
                id INTEGER PRIMARY KEY,
                book_id TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                isbn TEXT,
                isbn13 TEXT,
                publisher TEXT,
                publication_date TEXT,
                language_code TEXT,
                pages INTEGER,
                average_rating REAL,
                ratings_count INTEGER,
                text_reviews_count INTEGER,
                image_url TEXT,
                UNIQUE(book_id)
            )
        """)
        
        # Create the 'book_authors' table (many-to-many relationship)
        db.execute("""
            CREATE TABLE IF NOT EXISTS book_authors (
                book_id INTEGER,
                author_id INTEGER,
                PRIMARY KEY (book_id, author_id),
                FOREIGN KEY (book_id) REFERENCES book (id) ON DELETE CASCADE,
                FOREIGN KEY (author_id) REFERENCES author (id) ON DELETE CASCADE
            )
        """)
        
        # Create the 'user' table
        db.execute("""
            CREATE TABLE IF NOT EXISTS user (
                id INTEGER PRIMARY KEY,
                user_id TEXT NOT NULL,
                username TEXT,
                review_count INTEGER DEFAULT 0,
                rating_avg REAL,
                rating_stddev REAL,
                UNIQUE(user_id)
            )
        """)
        
        # Create the 'review' table
        db.execute("""
            CREATE TABLE IF NOT EXISTS review (
                id INTEGER PRIMARY KEY,
                review_id TEXT,
                book_id INTEGER,
                user_id INTEGER,
                rating INTEGER,
                review_text TEXT,
                date_added TEXT,
                is_spoiler BOOLEAN DEFAULT 0,
                has_sentiment BOOLEAN DEFAULT 0,
                sentiment_score REAL,
                sentiment_magnitude REAL,
                helpful_votes INTEGER DEFAULT 0,
                FOREIGN KEY (book_id) REFERENCES book (id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES user (id) ON DELETE CASCADE
            )
        """)
        
        # Create the 'genre' table
        # In the create_tables function, update the genre table definition
        db.execute("""
            CREATE TABLE IF NOT EXISTS genre (
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE,
                description TEXT,
                parent_id INTEGER,
                usage_count INTEGER DEFAULT 0,
                FOREIGN KEY (parent_id) REFERENCES genre (id) ON DELETE SET NULL
            )
        """)
        
        # Create the 'book_genre' table (many-to-many relationship)
        db.execute("""
            CREATE TABLE IF NOT EXISTS book_genre (
                book_id INTEGER,
                genre_id INTEGER,
                confidence_score REAL,
                PRIMARY KEY (book_id, genre_id),
                FOREIGN KEY (book_id) REFERENCES book (id) ON DELETE CASCADE,
                FOREIGN KEY (genre_id) REFERENCES genre (id) ON DELETE CASCADE
            )
        """)
        
        # Create the 'metadata' table for database information
        db.execute("""
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Commit the transaction
        db.commit()
        logger.info("Database tables created successfully")
        return True
    except sqlite3.Error as e:
        db.rollback()
        logger.error(f"Error creating tables: {e}")
        return False
        
def initialize_database(db):
    """
    Initialize the database with required schema and initial data.
    
    Args:
        db (Database): Database connection instance
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Create all tables
        if not create_tables(db):
            return False
            
        # Initialize metadata
        db.execute("""
            INSERT OR REPLACE INTO metadata (key, value, updated_at)
            VALUES ('schema_version', '1.0', CURRENT_TIMESTAMP)
        """)
        
        # Initialize basic genres if they don't exist
        base_genres = [
            ('Fiction', 'Fictional works of literature', None, 0),
            ('Non-Fiction', 'Factual works based on reality', None, 0),
            ('Science Fiction', 'Fictional works incorporating futuristic concepts', 1, 0),
            ('Fantasy', 'Fiction with fantastic elements like magic', 1, 0),
            ('Mystery', 'Fiction focused on solving a puzzle or crime', 1, 0),
            ('Romance', 'Stories primarily focused on romantic relationships', 1, 0),
            ('Biography', 'Account of a person\'s life', 2, 0),
            ('History', 'Account of past events', 2, 0),
            ('Science', 'Works about scientific discoveries and principles', 2, 0),
            ('Self-Help', 'Books intended to help readers improve their lives', 2, 0)
        ]

        db.executemany("""
            INSERT OR IGNORE INTO genre (name, description, parent_id, usage_count)
            VALUES (?, ?, ?, ?)
        """, base_genres)
        
        db.commit()
        logger.info("Database successfully initialized")
        return True
    except sqlite3.Error as e:
        db.rollback()
        logger.error(f"Error initializing database: {e}")
        return False
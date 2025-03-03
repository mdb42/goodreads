#!/usr/bin/env python3
"""
Goodreads Dataset Loader

This module handles downloading and importing Goodreads datasets from Kaggle
into the application's SQLite database.
"""

import os
import sys
import subprocess
import logging
from pathlib import Path
from datetime import datetime
import random

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

try:
    import pandas as pd
    import kaggle
    from tqdm import tqdm
except ImportError:
    logger.info("Installing required packages...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "kaggle", "tqdm", "pandas"])
    import pandas as pd
    import kaggle
    from tqdm import tqdm

# Define dataset-specific constants - focusing only on the main dataset
DATASET_INFO = {
    "kaggle_dataset": "zygmunt/goodbooks-10k",
    "description": "10,000 books with 6 million ratings",
    "files": ["books.csv", "ratings.csv", "book_tags.csv", "tags.csv", "to_read.csv"],
    "min_size_mb": 100
}

DATA_DIR = Path("data")

def check_kaggle_credentials():
    """Verify that Kaggle credentials are properly set up."""
    kaggle_dir = Path.home() / ".kaggle"
    kaggle_json = kaggle_dir / "kaggle.json"
    
    if not kaggle_json.exists():
        logger.error("Kaggle API credentials not found.")
        logger.info("Please set up your Kaggle credentials:")
        logger.info("1. Go to https://www.kaggle.com/ and log in")
        logger.info("2. Go to Account > API > Create New API Token")
        logger.info("3. Move the downloaded kaggle.json to ~/.kaggle/")
        logger.info("4. On Linux/Mac, run: chmod 600 ~/.kaggle/kaggle.json")
        return False
    
    if os.name != 'nt':  # On non-Windows, verify permissions
        permissions = oct(kaggle_json.stat().st_mode)[-3:]
        if permissions != '600':
            logger.warning(f"Your kaggle.json file has permissions {permissions}.")
            logger.warning("For security, it's recommended to run: chmod 600 ~/.kaggle/kaggle.json")
    
    try:
        kaggle.api.authenticate()
        return True
    except Exception as e:
        logger.error(f"Error authenticating with Kaggle API: {e}")
        return False

def dataset_exists():
    """Check if the dataset already exists locally."""
    if not DATA_DIR.exists():
        return False
        
    # Verify each expected file exists and has a minimal size
    for file in DATASET_INFO["files"]:
        file_path = DATA_DIR / file
        if not file_path.exists() or file_path.stat().st_size < 1000:
            return False
            
    # Check overall size for a sanity check
    total_size_mb = sum(f.stat().st_size for f in DATA_DIR.glob("*") if f.is_file()) / (1024 * 1024)
    if total_size_mb < DATASET_INFO["min_size_mb"]:
        logger.warning(f"Dataset files exist but total size ({total_size_mb:.2f} MB) is less than expected ({DATASET_INFO['min_size_mb']} MB)")
        return False
        
    return True

def download_dataset():
    """Download the dataset from Kaggle."""
    logger.info(f"Downloading dataset: {DATASET_INFO['description']}...")
    
    DATA_DIR.mkdir(exist_ok=True)
    
    try:
        kaggle.api.dataset_download_files(
            DATASET_INFO["kaggle_dataset"],
            path=str(DATA_DIR),
            unzip=True
        )
        logger.info(f"Successfully downloaded dataset to {DATA_DIR}")
        return True
    except Exception as e:
        logger.error(f"Error downloading dataset: {e}")
        return False

def import_dataset_to_db(db_path="data/analytics.db", ratings_limit=None):
    """
    Import downloaded dataset CSV files into the SQLite database
    
    Args:
        db_path: Path to SQLite database file
        ratings_limit: Optional limit on the number of ratings to import
    
    Returns:
        bool: True if import was successful, False otherwise
    """
    logger.info(f"Importing dataset into database...")
    
    try:
        from app.src.analytics_db import AnalyticsDB
        import random  # Required for ratings history generation
        
        # Initialize database connection
        db = AnalyticsDB(db_path)
        
        # Apply optimizations for faster imports
        db.optimize_for_bulk_import()
        
        try:
            # Import books data
            if not import_books(db):
                logger.error("Books import failed")
                return False
                
            # Import ratings data with optional limit
            if not import_ratings(db, ratings_limit):
                logger.error("Ratings import failed")
                return False
                
            # Import genres and book-genre relationships
            if not import_genres_and_tags(db):
                logger.error("Genres import failed")
                return False
                
            # Generate ratings history data
            if not generate_ratings_history(db):
                logger.error("Ratings history generation failed")
                return False
                
            # Verify database contents
            db.verify_database_contents()
            logger.info("Dataset import completed successfully")
            return True
                
        except Exception as e:
            logger.error(f"Error during dataset import: {e}", exc_info=True)
            return False
        finally:
            # Always restore normal settings
            db.restore_normal_settings()
            db.close()
            
    except Exception as e:
        logger.error(f"Error initializing database: {e}", exc_info=True)
        return False

def import_books(db):
    """Import books data from CSV file into the database."""
    books_file = DATA_DIR / "books.csv"
    
    if not books_file.exists():
        logger.error(f"Required file not found: {books_file}")
        return False
        
    # Import books
    logger.info(f"Loading books from {books_file}...")
    
    try:
        books_df = pd.read_csv(books_file)
        logger.info(f"Loaded {len(books_df)} books from CSV")
        
        # Extract column names for validation
        columns = books_df.columns.tolist()
        logger.info(f"Book CSV columns: {columns}")
        
        # Prepare book data for batch insertion
        book_records = []
        author_map = {}  # Keep track of authors by name
        book_author_pairs = []  # Track book-author relationships
        
        for _, book in tqdm(books_df.iterrows(), total=len(books_df), desc="Processing books"):
            # Get required book data with validation
            try:
                book_id = int(book['book_id']) if 'book_id' in book and not pd.isna(book['book_id']) else None
                title = str(book['title']) if 'title' in book and not pd.isna(book['title']) else "Unknown Title"
                
                # Optional fields with validation
                language = str(book['language_code']) if 'language_code' in book and not pd.isna(book['language_code']) else None
                avg_rating = float(book['average_rating']) if 'average_rating' in book and not pd.isna(book['average_rating']) else None
                ratings_count = int(book['ratings_count']) if 'ratings_count' in book and not pd.isna(book['ratings_count']) else None
                text_reviews_count = int(book['text_reviews_count']) if 'text_reviews_count' in book and not pd.isna(book['text_reviews_count']) else None
                publisher = str(book['publisher']) if 'publisher' in book and not pd.isna(book['publisher']) else None
                pages = int(book['num_pages']) if 'num_pages' in book and not pd.isna(book['num_pages']) else None
                pub_date = str(book['publication_date']) if 'publication_date' in book and not pd.isna(book['publication_date']) else None
                
                # Add to batch records - note we include the external book_id first
                book_records.append((
                    book_id,  # external_id 
                    title, 
                    None,  # subtitle
                    publisher, 
                    pub_date, 
                    language, 
                    pages, 
                    None,  # description
                    avg_rating, 
                    ratings_count, 
                    text_reviews_count
                ))
                
                # Process authors for this book
                if 'authors' in book and not pd.isna(book['authors']) and book_id is not None:
                    authors_text = str(book['authors'])
                    author_names = [name.strip() for name in authors_text.split(',') if name.strip()]
                    
                    for author_name in author_names:
                        if author_name not in author_map:
                            author_map[author_name] = None  # We'll get IDs after insertion
                        
                        # We'll store the external book_id temporarily
                        book_author_pairs.append((book_id, author_name))
            
            except Exception as e:
                logger.warning(f"Error processing book {book.get('book_id', 'unknown')}: {e}")
                continue
                
        # Batch insert books
        logger.info(f"Batch inserting {len(book_records)} books...")
        if not db.batch_insert_books(book_records):
            logger.error("Book insertion failed")
            return False
            
        # Insert authors and get their IDs
        author_records = [(name, None, None, None, None) for name in author_map.keys()]
        logger.info(f"Batch inserting {len(author_records)} authors...")
        if not db.batch_insert_authors(author_records):
            logger.error("Author insertion failed")
            return False
            
        # Get author IDs after insertion
        cursor = db.conn.cursor()
        cursor.execute("SELECT id, name FROM author")
        for author_id, name in cursor.fetchall():
            author_map[name] = author_id
            
        # Get mapping of external book IDs to internal IDs
        cursor.execute("SELECT id, external_id FROM book WHERE external_id IS NOT NULL")
        book_id_map = {ext_id: int_id for int_id, ext_id in cursor.fetchall()}
        
        # Build book-author relationship records using internal IDs
        book_author_records = []
        for ext_book_id, author_name in book_author_pairs:
            if ext_book_id in book_id_map and author_name in author_map:
                internal_book_id = book_id_map[ext_book_id]
                author_id = author_map[author_name]
                book_author_records.append((internal_book_id, author_id))
            else:
                logger.debug(f"Skip relationship: Book ID {ext_book_id} or author {author_name} not found")
                
        # Insert book-author relationships
        logger.info(f"Batch inserting {len(book_author_records)} book-author relationships...")
        if not db.batch_insert_book_authors(book_author_records):
            logger.error("Book-author relationship insertion failed")
            return False
            
        # Success!
        logger.info("Books import completed successfully")
        return True
            
    except Exception as e:
        logger.error(f"Error during books import: {e}", exc_info=True)
        return False

def import_ratings(db, limit=None):
    """Import ratings data from CSV file."""
    ratings_file = DATA_DIR / "ratings.csv"
    
    if not ratings_file.exists():
        logger.warning(f"Ratings file not found: {ratings_file}")
        return True  # Not a fatal error, continue with other imports
    
    logger.info(f"Processing ratings from {ratings_file}...")
    
    try:
        # Get user IDs first (unique)
        logger.info("Extracting unique user IDs...")
        user_ids = set()
        chunk_size = 100000  # Process in chunks to handle large files
        
        # Count total lines for progress reporting
        total_lines = sum(1 for _ in open(ratings_file))
        logger.info(f"Total ratings in file: {total_lines:,}")
        
        max_chunks = (limit // chunk_size) + 1 if limit else None
        
        chunk_count = 0
        for chunk in pd.read_csv(ratings_file, usecols=['user_id'], chunksize=chunk_size):
            # Convert NumPy int64 to Python int to avoid type mismatches
            for uid in chunk['user_id'].unique():
                user_ids.add(int(uid))
            
            chunk_count += 1
            logger.info(f"Processed chunk {chunk_count} for user IDs, found {len(user_ids)} unique users so far")
            if max_chunks and chunk_count >= max_chunks:
                break
                
        logger.info(f"Found {len(user_ids)} unique users")
        
        # Insert users in batches - explicitly convert to Python int
        user_records = [(int(uid), f"user_{uid}", 0, None, None) for uid in user_ids]
        batch_size = 10000
        for i in range(0, len(user_records), batch_size):
            batch = user_records[i:i+batch_size]
            logger.info(f"Inserting user batch {i//batch_size + 1}/{(len(user_records)-1)//batch_size + 1} ({len(batch)} users)...")
            if not db.batch_insert_users(batch):
                logger.error(f"Failed inserting users batch {i//batch_size + 1}")
                return False
                
        # Get mapping of external user IDs to internal IDs
        cursor = db.conn.cursor()
        cursor.execute("SELECT id, external_id FROM user WHERE external_id IS NOT NULL")
        user_id_map = {int(ext_id): int_id for int_id, ext_id in cursor.fetchall()}
        
        # Debugging info about user ID mapping
        logger.info(f"Created user ID map with {len(user_id_map)} entries")
        if len(user_id_map) > 0:
            sample_keys = list(user_id_map.keys())[:5]
            logger.info(f"Sample user ID map entries: {[(k, user_id_map[k]) for k in sample_keys]}")
        
        # Get mapping of external book IDs to internal IDs
        cursor.execute("SELECT id, external_id FROM book WHERE external_id IS NOT NULL")
        book_id_map = {int(ext_id): int_id for int_id, ext_id in cursor.fetchall()}
        
        # Debugging info about book ID mapping
        logger.info(f"Created book ID map with {len(book_id_map)} entries")
        if len(book_id_map) > 0:
            sample_keys = list(book_id_map.keys())[:5]
            logger.info(f"Sample book ID map entries: {[(k, book_id_map[k]) for k in sample_keys]}")
        
        # Process ratings in chunks
        logger.info("Processing ratings data...")
        ratings_processed = 0
        ratings_skipped = 0
        review_date = datetime.now().strftime("%Y-%m-%d")  # Use current date as placeholder
        
        for chunk_num, chunk in enumerate(pd.read_csv(ratings_file, chunksize=chunk_size)):
            review_records = []
            current_chunk_skipped = 0
            
            for _, rating in chunk.iterrows():
                try:
                    # Convert to native Python int to ensure type consistency
                    ext_book_id = int(rating.get('book_id'))
                    ext_user_id = int(rating.get('user_id'))
                    rating_val = int(rating.get('rating'))
                    
                    # Debug logging for a few records
                    if ratings_processed < 5:
                        logger.debug(f"Processing rating: book_id={ext_book_id}, user_id={ext_user_id}, rating={rating_val}")
                        logger.debug(f"  Book ID in map: {ext_book_id in book_id_map}")
                        logger.debug(f"  User ID in map: {ext_user_id in user_id_map}")
                    
                    # Skip if we don't have mappings for the IDs
                    if ext_book_id not in book_id_map or ext_user_id not in user_id_map:
                        current_chunk_skipped += 1
                        
                        # More detailed logging for troubleshooting
                        if ratings_skipped < 5:
                            if ext_book_id not in book_id_map:
                                logger.debug(f"Skipping rating: book_id {ext_book_id} not found in map")
                            if ext_user_id not in user_id_map:
                                logger.debug(f"Skipping rating: user_id {ext_user_id} not found in map")
                                logger.debug(f"  User ID type: {type(ext_user_id)}")
                                logger.debug(f"  Map key types: {[type(k) for k in list(user_id_map.keys())[:3]]}")
                        
                        continue
                        
                    # Use internal IDs for database relationships
                    internal_book_id = book_id_map[ext_book_id]
                    internal_user_id = user_id_map[ext_user_id]
                    
                    # Create review record
                    review_records.append((
                        internal_book_id,  # book_id
                        internal_user_id,  # user_id
                        rating_val,  # rating
                        "",  # Empty review text
                        review_date,  # Placeholder date
                        0, 0, None  # Default values for remaining fields
                    ))
                except Exception as e:
                    # Skip problematic records
                    logger.debug(f"Error processing rating: {e}")
                    current_chunk_skipped += 1
                    continue
            
            # Insert this batch of ratings
            if review_records:
                logger.info(f"Inserting ratings batch {chunk_num+1} with {len(review_records)} ratings...")
                if not db.batch_insert_reviews(review_records):
                    logger.error(f"Failed inserting ratings batch {chunk_num+1}")
                    return False
                
                ratings_processed += len(review_records)
                
            ratings_skipped += current_chunk_skipped
            logger.info(f"Progress: {ratings_processed:,} ratings imported, {ratings_skipped:,} skipped")
            
            # Check if we've hit the optional limit
            if limit and ratings_processed >= limit:
                logger.info(f"Reached import limit of {limit} ratings")
                break
        
        # Final summary
        logger.info(f"Successfully imported {ratings_processed:,} ratings")
        logger.info(f"Skipped {ratings_skipped:,} ratings (usually due to ID mapping issues)")
        
        return True
    except Exception as e:
        logger.error(f"Error during ratings import: {e}", exc_info=True)
        return False

def import_genres_and_tags(db):
    """Import genres and book-genre relationships from tags data."""
    tags_file = DATA_DIR / "tags.csv"
    book_tags_file = DATA_DIR / "book_tags.csv"
    
    if not tags_file.exists() or not book_tags_file.exists():
        logger.warning(f"Tags files not found: {tags_file} or {book_tags_file}")
        return True  # Not a fatal error, continue with other imports
    
    logger.info(f"Processing genres from {tags_file}...")
    
    try:
        # First, load the tags file to get genre names
        tags_df = pd.read_csv(tags_file)
        logger.info(f"Loaded {len(tags_df)} tags from CSV")
        
        # Extract column names for validation
        columns = tags_df.columns.tolist()
        logger.info(f"Tags CSV columns: {columns}")
        
        # Prepare genre data for batch insertion
        genre_records = []
        tag_id_to_genre_id = {}  # Map from external tag IDs to internal genre IDs
        
        for _, tag in tqdm(tags_df.iterrows(), total=len(tags_df), desc="Processing tags"):
            try:
                tag_id = int(tag['tag_id']) if 'tag_id' in tag and not pd.isna(tag['tag_id']) else None
                tag_name = str(tag['tag_name']) if 'tag_name' in tag and not pd.isna(tag['tag_name']) else None
                
                if tag_id is not None and tag_name:
                    genre_records.append((tag_name,))
                
            except Exception as e:
                logger.warning(f"Error processing tag {tag.get('tag_id', 'unknown')}: {e}")
                continue
        
        # Batch insert genres
        logger.info(f"Inserting {len(genre_records)} genres...")
        
        # Start a transaction
        db.conn.execute("BEGIN TRANSACTION")
        
        try:
            # Prepare the SQL statement for genres
            sql = """
                INSERT OR IGNORE INTO genre (name)
                VALUES (?)
            """
            
            # Execute for multiple records
            db.cursor.executemany(sql, genre_records)
            db.conn.commit()
            logger.info(f"Successfully inserted genres")
            
            # Now get the IDs of the inserted genres
            cursor = db.conn.cursor()
            cursor.execute("SELECT id, name FROM genre")
            for genre_id, name in cursor.fetchall():
                # Find the tag_id that corresponds to this name
                matching_tags = tags_df[tags_df['tag_name'] == name]
                if not matching_tags.empty:
                    tag_id = int(matching_tags.iloc[0]['tag_id'])
                    tag_id_to_genre_id[tag_id] = genre_id
            
            logger.info(f"Created tag-to-genre mapping with {len(tag_id_to_genre_id)} entries")
            
            # Now process the book-tags relationships
            logger.info(f"Processing book-genre relationships from {book_tags_file}...")
            book_tags_df = pd.read_csv(book_tags_file)
            logger.info(f"Loaded {len(book_tags_df)} book-tag relationships from CSV")
            
            # Extract column names for validation
            columns = book_tags_df.columns.tolist()
            logger.info(f"Book-Tags CSV columns: {columns}")
            
            # Get mapping of external book IDs to internal IDs
            cursor.execute("SELECT id, external_id FROM book WHERE external_id IS NOT NULL")
            book_id_map = {int(ext_id): int_id for int_id, ext_id in cursor.fetchall()}
            
            # Prepare book-genre relationships
            book_genre_records = []
            processed = 0
            skipped = 0
            
            for _, book_tag in tqdm(book_tags_df.iterrows(), total=len(book_tags_df), desc="Processing book-genre relationships"):
                try:
                    # Extract and validate data
                    goodreads_book_id = int(book_tag['goodreads_book_id']) if 'goodreads_book_id' in book_tag and not pd.isna(book_tag['goodreads_book_id']) else None
                    tag_id = int(book_tag['tag_id']) if 'tag_id' in book_tag and not pd.isna(book_tag['tag_id']) else None
                    
                    # Only process if we have valid mappings
                    if goodreads_book_id in book_id_map and tag_id in tag_id_to_genre_id:
                        internal_book_id = book_id_map[goodreads_book_id]
                        internal_genre_id = tag_id_to_genre_id[tag_id]
                        book_genre_records.append((internal_book_id, internal_genre_id))
                        processed += 1
                    else:
                        skipped += 1
                        
                except Exception as e:
                    logger.debug(f"Error processing book-tag relationship: {e}")
                    skipped += 1
                    continue
            
            # Insert book-genre relationships
            if book_genre_records:
                logger.info(f"Inserting {len(book_genre_records)} book-genre relationships...")
                
                # Start a new transaction
                db.conn.execute("BEGIN TRANSACTION")
                
                try:
                    # Prepare the SQL statement
                    sql = """
                        INSERT OR IGNORE INTO book_genre (book_id, genre_id)
                        VALUES (?, ?)
                    """
                    
                    # Execute for multiple records
                    db.cursor.executemany(sql, book_genre_records)
                    db.conn.commit()
                    logger.info(f"Successfully inserted book-genre relationships")
                except Exception as e:
                    db.conn.rollback()
                    logger.error(f"Error inserting book-genre relationships: {e}")
                    return False
            
            logger.info(f"Genres import completed: {processed} relationships imported, {skipped} skipped")
            return True
            
        except Exception as e:
            db.conn.rollback()
            logger.error(f"Error inserting genres: {e}")
            return False
            
    except Exception as e:
        logger.error(f"Error during genres import: {e}", exc_info=True)
        return False

def generate_ratings_history(db):
    """Generate ratings history records based on existing books and ratings."""
    logger.info("Generating ratings history records...")
    
    try:
        # Check if we have any existing history records
        cursor = db.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM ratings_history")
        if cursor.fetchone()[0] > 0:
            logger.info("Ratings history already exists, skipping generation")
            return True
        
        # Get all books with ratings count > 0
        cursor.execute("""
            SELECT id, external_id, ratings_count, average_rating 
            FROM book 
            WHERE ratings_count > 0
        """)
        books = cursor.fetchall()
        logger.info(f"Found {len(books)} books with ratings")
        
        if not books:
            logger.warning("No books with ratings found, skipping history generation")
            return True
        
        # Generate snapshot dates (quarterly for the past 3 years)
        today = datetime.now()
        snapshot_dates = []
        for year_offset in range(3):
            for quarter in range(4):
                date = today.replace(
                    year=today.year - year_offset,
                    month=3*quarter + 1,
                    day=1
                )
                snapshot_dates.append(date.strftime("%Y-%m-%d"))
        
        snapshot_dates.reverse()  # Start with oldest
        logger.info(f"Generated {len(snapshot_dates)} historical snapshot dates")
        
        # Generate ratings history records
        history_records = []
        
        for book_id, ext_book_id, final_count, final_rating in tqdm(books, desc="Generating ratings history"):
            # Create a simple growth model for this book
            # We'll assume ratings accumulate over time with some randomness
            
            # Start with a low percentage of final count and grow to 100%
            count_percentages = [
                5 + (95 * ((i+1)/len(snapshot_dates)) * (0.8 + 0.4 * random.random()))
                for i in range(len(snapshot_dates))
            ]
            
            # Make sure the last one is 100%
            count_percentages[-1] = 100.0
            
            # For rating, we'll fluctuate around the final value with a decreasing variance
            for i, date in enumerate(snapshot_dates):
                count_pct = count_percentages[i]
                cum_count = max(1, int((count_pct / 100.0) * final_count))
                
                # Rating starts more volatile and stabilizes over time
                volatility = max(0.05, 0.3 * (1 - (i / len(snapshot_dates))))
                rating = max(1.0, min(5.0, 
                    final_rating + (volatility * (random.random() - 0.5))
                ))
                
                history_records.append((
                    book_id,      # book_id
                    date,         # snapshot_date
                    cum_count,    # cum_ratings_count
                    round(rating, 2)  # avg_rating
                ))
        
        # Insert records in batches
        logger.info(f"Inserting {len(history_records)} rating history records...")
        batch_size = 5000
        for i in range(0, len(history_records), batch_size):
            batch = history_records[i:i+batch_size]
            
            db.conn.execute("BEGIN TRANSACTION")
            try:
                # Prepare the SQL statement
                sql = """
                    INSERT INTO ratings_history 
                    (book_id, snapshot_date, cum_ratings_count, avg_rating)
                    VALUES (?, ?, ?, ?)
                """
                
                # Execute for multiple records
                db.cursor.executemany(sql, batch)
                db.conn.commit()
                logger.info(f"Inserted batch {i//batch_size + 1}/{(len(history_records)-1)//batch_size + 1}")
            except Exception as e:
                db.conn.rollback()
                logger.error(f"Error inserting ratings history batch: {e}")
                return False
        
        logger.info(f"Successfully generated {len(history_records)} ratings history records")
        return True
        
    except Exception as e:
        logger.error(f"Error generating ratings history: {e}", exc_info=True)
        return False

def verify_database(db_path="data/analytics.db"):
    """Verify database contents after import."""
    import sqlite3
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    tables = [
        "book", "author", "user", "review", 
        "book_authors", "genre", "book_genre", "ratings_history"
    ]
    
    logger.info("\nDatabase Verification:")
    results = {}
    
    for table in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            results[table] = count
            logger.info(f"- {table}: {count} records")
            
            # Sample data for non-empty tables
            if count > 0:
                cursor.execute(f"SELECT * FROM {table} LIMIT 1")
                columns = [desc[0] for desc in cursor.description]
                sample = cursor.fetchone()
                logger.info(f"  Columns: {columns}")
                logger.info(f"  Sample: {sample}")
        except sqlite3.Error as e:
            logger.error(f"- {table}: Error - {e}")
            results[table] = f"Error: {e}"
    
    conn.close()
    return results
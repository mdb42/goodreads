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
import shutil
from pathlib import Path
from datetime import datetime
import random

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Attempt to import necessary packages; if not installed, install them.
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

# Dataset-specific constants
DATASET_INFO = {
    "kaggle_dataset": "zygmunt/goodbooks-10k",
    "description": "10,000 books with 6 million ratings",
    "files": ["books.csv", "ratings.csv", "book_tags.csv", "tags.csv", "to_read.csv"],
    "min_size_mb": 40,  # Minimal expected total size in MB for sanity check.
    "name": "goodbooks-10k"
}

UCSD_DATASET_INFO = {
    "name": "ucsd-book-graph",
    "description": "UCSD Book Graph with 15.7M reviews and 1.5M books",
    "files": ["goodreads_books.json.gz", "goodreads_reviews.json.gz", "goodreads_book_series.json.gz", 
              "goodreads_book_works.json.gz", "goodreads_interactions.json.gz"],
    "min_size_mb": 1000,  # These files are big!
    "download_url": "https://sites.google.com/eng.ucsd.edu/ucsdbookgraph/home"
}

# Base directory for all data
DATA_DIR = Path("data")
# Dataset-specific directory
DATASET_DIR = DATA_DIR / DATASET_INFO["name"]
# Archive directory to store downloaded zip files
ARCHIVE_DIR = DATA_DIR / "archives"


def check_kaggle_credentials():
    """
    Verify that Kaggle API credentials are properly set up.

    Returns:
        bool: True if credentials are present and authentication succeeds,
              otherwise False.
    """
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
    
    if os.name != 'nt':  # On non-Windows, verify file permissions.
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
    """
    Check if the dataset already exists locally.

    Returns:
        bool: True if all expected CSV files exist and have a minimal size,
              False otherwise.
    """
    # Check if dataset-specific directory exists
    if not DATASET_DIR.exists():
        logger.info(f"Dataset directory {DATASET_DIR} does not exist")
        # If dataset-specific directory doesn't exist, but files are in DATA_DIR,
        # we can create the directory and move files there
        if DATA_DIR.exists() and all(DATA_DIR.joinpath(file).exists() for file in DATASET_INFO["files"]):
            logger.info("CSV files found in data directory. Organizing into dataset-specific directory...")
            DATASET_DIR.mkdir(exist_ok=True)
            for file in DATASET_INFO["files"]:
                source = DATA_DIR / file
                target = DATASET_DIR / file
                shutil.move(str(source), str(target))
            logger.info(f"Files organized into {DATASET_DIR}")
            return True
        return False
        
    # Verify each expected file exists and has a minimal size (in bytes).
    for file in DATASET_INFO["files"]:
        file_path = DATASET_DIR / file
        if not file_path.exists() or file_path.stat().st_size < 1000:
            logger.info(f"Required file missing or too small: {file_path}")
            return False
            
    # Sanity check: ensure the total size meets expectations.
    total_size_mb = sum(f.stat().st_size for f in DATASET_DIR.glob("*") if f.is_file()) / (1024 * 1024)
    if total_size_mb < DATASET_INFO["min_size_mb"]:
        logger.warning(f"Dataset files exist but total size ({total_size_mb:.2f} MB) is less than expected ({DATASET_INFO['min_size_mb']} MB)")
        return False
        
    logger.info(f"Dataset exists and appears valid in {DATASET_DIR}")
    return True


def download_dataset():
    """
    Download the dataset from Kaggle.

    Returns:
        bool: True if the download (and extraction) is successful, otherwise False.
    """
    if not check_kaggle_credentials():
        logger.error("Cannot download dataset: Kaggle credentials not found or invalid")
        return False
        
    logger.info(f"Downloading dataset: {DATASET_INFO['description']}...")
    
    # Create necessary directories
    DATA_DIR.mkdir(exist_ok=True)
    DATASET_DIR.mkdir(exist_ok=True)
    ARCHIVE_DIR.mkdir(exist_ok=True)
    
    try:
        # Download to dataset-specific directory
        kaggle.api.dataset_download_files(
            DATASET_INFO["kaggle_dataset"],
            path=str(DATASET_DIR),
            unzip=True
        )
        
        # Try to also save the zip file for future reference
        try:
            dataset_name = DATASET_INFO["kaggle_dataset"].split("/")[-1]
            zip_file = f"{dataset_name}.zip"
            # If the zip file exists in the dataset directory, move it to archives
            if (DATASET_DIR / zip_file).exists():
                shutil.move(
                    str(DATASET_DIR / zip_file),
                    str(ARCHIVE_DIR / f"{dataset_name}_{datetime.now().strftime('%Y%m%d')}.zip")
                )
        except Exception as e:
            logger.warning(f"Could not save the dataset zip file: {e}")
            
        logger.info(f"Successfully downloaded and unzipped dataset to {DATASET_DIR}")
        return True
    except Exception as e:
        logger.error(f"Error downloading dataset: {e}")
        return False

def import_dataset_to_db(db_path="data/analytics.db", ratings_limit=None):
    logger.info("Importing dataset into database...")
    
    # Verify dataset files exist
    if not dataset_exists():
        logger.warning("Dataset files not found or incomplete")
        if not download_dataset():
            logger.error("Failed to download dataset")
            return False
    
    try:
        from app.src.analytics_db import AnalyticsDB
        dataset_name = DATASET_INFO["name"]  # Get the dataset name
        
        # Initialize database connection.
        db = AnalyticsDB(db_path)
        
        # Check if database already has records - useful for incremental imports
        db_stats = db.verify_database_contents()
        
        # Optimize SQLite settings for bulk import.
        db.optimize_for_bulk_import()
        
        try:
            # Import books data - pass the dataset name
            if not import_books(db, dataset_name):
                logger.error("Books import failed")
                return False
                
            # Import ratings data - pass the dataset name
            if not import_ratings(db, ratings_limit, dataset_name):
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
                
            # Register the dataset in the database metadata
            db.register_dataset(dataset_name, source="kaggle")
                
            # Verify database contents.
            final_stats = db.verify_database_contents()

            # Log the changes
            logger.info("\nImport Summary:")
            for table in final_stats:
                before = db_stats.get(table, 0)
                after = final_stats.get(table, 0)
                if isinstance(before, int) and isinstance(after, int):
                    logger.info(f"- {table}: {before} -> {after} records (+{after - before})")
                else:
                    logger.info(f"- {table}: {after} records")
                    
            logger.info("Dataset import completed successfully")
            return True
                
        except Exception as e:
            logger.error(f"Error during dataset import: {e}", exc_info=True)
            return False
        finally:
            # Always restore normal database settings.
            db.restore_normal_settings()
            db.close()
            
    except Exception as e:
        logger.error(f"Error initializing database: {e}", exc_info=True)
        return False


def import_books(db, dataset_source):
    """
    Import books data from the CSV file into the database.

    Args:
        db: Instance of AnalyticsDB.
        dataset_source: Source dataset identifier.

    Returns:
        bool: True if the books import is successful, otherwise False.
    """
    books_file = DATASET_DIR / "books.csv"
    
    if not books_file.exists():
        logger.error(f"Required file not found: {books_file}")
        return False
        
    logger.info(f"Loading books from {books_file}...")
    
    try:
        books_df = pd.read_csv(books_file)
        logger.info(f"Loaded {len(books_df)} books from CSV")
        
        # Log CSV columns for debugging.
        columns = books_df.columns.tolist()
        logger.info(f"Book CSV columns: {columns}")
        
        # Prepare data for batch insertion.
        book_records = []
        author_map = {}      # Maps author names to internal IDs (to be updated later).
        book_author_pairs = []  # List of (external book_id, author_name) pairs.
        
        for _, book in tqdm(books_df.iterrows(), total=len(books_df), desc="Processing books"):
            try:
                # Extract required book data with validation.
                book_id = int(book['id']) if 'id' in book and not pd.isna(book['id']) else None
                title = str(book['title']) if 'title' in book and not pd.isna(book['title']) else "Unknown Title"
                
                # Optional fields.
                language = str(book['language_code']) if 'language_code' in book and not pd.isna(book['language_code']) else None
                avg_rating = float(book['average_rating']) if 'average_rating' in book and not pd.isna(book['average_rating']) else None
                ratings_count = int(book['ratings_count']) if 'ratings_count' in book and not pd.isna(book['ratings_count']) else None
                text_reviews_count = int(book['text_reviews_count']) if 'text_reviews_count' in book and not pd.isna(book['text_reviews_count']) else None
                publisher = str(book['publisher']) if 'publisher' in book and not pd.isna(book['publisher']) else None
                pages = int(book['num_pages']) if 'num_pages' in book and not pd.isna(book['num_pages']) else None
                pub_date = str(book['publication_date']) if 'publication_date' in book and not pd.isna(book['publication_date']) else None
                
                # Append the book record; note external book_id is stored as external_id.
                book_records.append((
                    book_id,  # external_id 
                    title, 
                    None,     # subtitle (placeholder)
                    publisher, 
                    pub_date, 
                    language, 
                    pages, 
                    None,     # description (placeholder)
                    avg_rating, 
                    ratings_count, 
                    text_reviews_count,
                    dataset_source  # Add the dataset source
                ))
                
                # Process authors for this book.
                if 'authors' in book and not pd.isna(book['authors']) and book_id is not None:
                    authors_text = str(book['authors'])
                    author_names = [name.strip() for name in authors_text.split(',') if name.strip()]
                    
                    for author_name in author_names:
                        if author_name not in author_map:
                            author_map[author_name] = None  # Placeholder; actual ID will be fetched after insertion.
                        
                        # Record the relationship using the external book_id.
                        book_author_pairs.append((book_id, author_name))
            
            except Exception as e:
                logger.warning(f"Error processing book {book.get('book_id', 'unknown')}: {e}")
                continue
                
        # Batch insert books.
        logger.info(f"Batch inserting {len(book_records)} books...")
        if not db.batch_insert_books(book_records):
            logger.error("Book insertion failed")
            return False
            
        # Insert authors and update mapping.
        author_records = [(name, None, None, None, None) for name in author_map.keys()]
        logger.info(f"Batch inserting {len(author_records)} authors...")
        if not db.batch_insert_authors(author_records):
            logger.error("Author insertion failed")
            return False
            
        # Retrieve internal author IDs.
        cursor = db.conn.cursor()
        cursor.execute("SELECT id, name FROM author")
        for author_id, name in cursor.fetchall():
            author_map[name] = author_id
            
        # Build a mapping from external book IDs to internal IDs.
        cursor.execute("SELECT id, external_id FROM book WHERE external_id IS NOT NULL")
        book_id_map = {ext_id: int_id for int_id, ext_id in cursor.fetchall()}
        
        # Build book-author relationship records using internal IDs.
        book_author_records = []
        for ext_book_id, author_name in book_author_pairs:
            if ext_book_id in book_id_map and author_name in author_map:
                internal_book_id = book_id_map[ext_book_id]
                author_id = author_map[author_name]
                book_author_records.append((internal_book_id, author_id))
            else:
                logger.debug(f"Skipping relationship: Book ID {ext_book_id} or author {author_name} not found")
                
        # Insert book-author relationships.
        logger.info(f"Batch inserting {len(book_author_records)} book-author relationships...")
        if not db.batch_insert_book_authors(book_author_records):
            logger.error("Book-author relationship insertion failed")
            return False
            
        logger.info("Books import completed successfully")
        return True
            
    except Exception as e:
        logger.error(f"Error during books import: {e}", exc_info=True)
        return False


def import_ratings(db, limit=None, dataset_source=None):
    """
    Import ratings data from CSV file.

    Args:
        db: Instance of AnalyticsDB.
        limit (int, optional): Optional limit on the number of ratings to import.

    Returns:
        bool: True if the ratings import is successful, otherwise False.
    """
    ratings_file = DATASET_DIR / "ratings.csv"
    
    if not ratings_file.exists():
        logger.warning(f"Ratings file not found: {ratings_file}")
        return True  # Non-fatal; continue with other imports.
    
    logger.info(f"Processing ratings from {ratings_file}...")
    
    try:
        # Extract unique user IDs from the ratings file.
        logger.info("Extracting unique user IDs...")
        user_ids = set()
        chunk_size = 100000  # Process in chunks.
        
        total_lines = sum(1 for _ in open(ratings_file))
        logger.info(f"Total ratings in file: {total_lines:,}")
        
        max_chunks = (limit // chunk_size) + 1 if limit else None
        
        chunk_count = 0
        for chunk in pd.read_csv(ratings_file, usecols=['user_id'], chunksize=chunk_size):
            # Convert NumPy types to native Python int.
            for uid in chunk['user_id'].unique():
                user_ids.add(int(uid))
            
            chunk_count += 1
            logger.info(f"Processed chunk {chunk_count} for user IDs, found {len(user_ids)} unique users so far")
            if max_chunks and chunk_count >= max_chunks:
                break
                
        logger.info(f"Found {len(user_ids)} unique users")
        
        # Insert users in batches.
        user_records = [(int(uid), f"user_{uid}", 0, None, None, dataset_source) for uid in user_ids]
        batch_size = 10000
        for i in range(0, len(user_records), batch_size):
            batch = user_records[i:i+batch_size]
            logger.info(f"Inserting user batch {i//batch_size + 1}/{(len(user_records)-1)//batch_size + 1} ({len(batch)} users)...")
            if not db.batch_insert_users(batch):
                logger.error(f"Failed inserting users batch {i//batch_size + 1}")
                return False
                
        # Build mapping of external user IDs to internal IDs.
        cursor = db.conn.cursor()
        cursor.execute("SELECT id, external_id FROM user WHERE external_id IS NOT NULL")
        user_id_map = {int(ext_id): int_id for int_id, ext_id in cursor.fetchall()}
        logger.info(f"Created user ID map with {len(user_id_map)} entries")
        if len(user_id_map) > 0:
            sample_keys = list(user_id_map.keys())[:5]
            logger.info(f"Sample user ID map entries: {[(k, user_id_map[k]) for k in sample_keys]}")
        
        # Build mapping of external book IDs to internal IDs.
        cursor.execute("SELECT id, external_id FROM book WHERE external_id IS NOT NULL")
        book_id_map = {int(ext_id): int_id for int_id, ext_id in cursor.fetchall()}
        logger.info(f"Created book ID map with {len(book_id_map)} entries")
        if len(book_id_map) > 0:
            sample_keys = list(book_id_map.keys())[:5]
            logger.info(f"Sample book ID map entries: {[(k, book_id_map[k]) for k in sample_keys]}")
        
        # Process ratings data in chunks.
        logger.info("Processing ratings data...")
        ratings_processed = 0
        ratings_skipped = 0
        review_date = datetime.now().strftime("%Y-%m-%d")  # Placeholder date.
        
        for chunk_num, chunk in enumerate(pd.read_csv(ratings_file, chunksize=chunk_size)):
            review_records = []
            current_chunk_skipped = 0
            
            for _, rating in chunk.iterrows():
                try:
                    ext_book_id = int(rating.get('book_id'))
                    ext_user_id = int(rating.get('user_id'))
                    rating_val = int(rating.get('rating'))
                    
                    # Debug log for initial records.
                    if ratings_processed < 5:
                        logger.debug(f"Processing rating: book_id={ext_book_id}, user_id={ext_user_id}, rating={rating_val}")
                        logger.debug(f"  Book ID in map: {ext_book_id in book_id_map}")
                        logger.debug(f"  User ID in map: {ext_user_id in user_id_map}")
                    
                    # Skip if mappings are missing.
                    if ext_book_id not in book_id_map or ext_user_id not in user_id_map:
                        current_chunk_skipped += 1
                        if ratings_skipped < 5:
                            if ext_book_id not in book_id_map:
                                logger.debug(f"Skipping rating: book_id {ext_book_id} not found in map")
                            if ext_user_id not in user_id_map:
                                logger.debug(f"Skipping rating: user_id {ext_user_id} not found in map")
                        continue
                        
                    internal_book_id = book_id_map[ext_book_id]
                    internal_user_id = user_id_map[ext_user_id]
                    
                    review_records.append((
                        internal_book_id,  # book_id
                        internal_user_id,  # user_id
                        rating_val,        # rating
                        "",                # Empty review text
                        review_date,       # Placeholder review date
                        0, 0, None,        # Default values for remaining fields
                        dataset_source     # Add dataset source
                    ))
                except Exception as e:
                    logger.debug(f"Error processing rating: {e}")
                    current_chunk_skipped += 1
                    continue
            
            if review_records:
                logger.info(f"Inserting ratings batch {chunk_num+1} with {len(review_records)} ratings...")
                if not db.batch_insert_reviews(review_records):
                    logger.error(f"Failed inserting ratings batch {chunk_num+1}")
                    return False
                
                ratings_processed += len(review_records)
                
            ratings_skipped += current_chunk_skipped
            logger.info(f"Progress: {ratings_processed:,} ratings imported, {ratings_skipped:,} skipped")
            
            if limit and ratings_processed >= limit:
                logger.info(f"Reached import limit of {limit} ratings")
                break
        
        logger.info(f"Successfully imported {ratings_processed:,} ratings")
        logger.info(f"Skipped {ratings_skipped:,} ratings (usually due to ID mapping issues)")
        return True
    except Exception as e:
        logger.error(f"Error during ratings import: {e}", exc_info=True)
        return False


def import_genres_and_tags(db):
    """
    Import genres and book-genre relationships from tags data.

    Args:
        db: Instance of AnalyticsDB.

    Returns:
        bool: True if the genres and tags import is successful, otherwise False.
    """
    tags_file = DATASET_DIR / "tags.csv"
    book_tags_file = DATASET_DIR / "book_tags.csv"
    
    if not tags_file.exists() or not book_tags_file.exists():
        logger.warning(f"Tags files not found: {tags_file} or {book_tags_file}")
        return True  # Non-fatal; continue with other imports.
    
    logger.info(f"Processing genres from {tags_file}...")
    
    try:
        tags_df = pd.read_csv(tags_file)
        logger.info(f"Loaded {len(tags_df)} tags from CSV")
        
        # Log CSV columns.
        columns = tags_df.columns.tolist()
        logger.info(f"Tags CSV columns: {columns}")
        
        genre_records = []
        tag_id_to_genre_id = {}  # Map from external tag IDs to internal genre IDs.
        
        for _, tag in tqdm(tags_df.iterrows(), total=len(tags_df), desc="Processing tags"):
            try:
                tag_id = int(tag['tag_id']) if 'tag_id' in tag and not pd.isna(tag['tag_id']) else None
                tag_name = str(tag['tag_name']) if 'tag_name' in tag and not pd.isna(tag['tag_name']) else None
                
                if tag_id is not None and tag_name:
                    genre_records.append((tag_name,))
            except Exception as e:
                logger.warning(f"Error processing tag {tag.get('tag_id', 'unknown')}: {e}")
                continue
        
        logger.info(f"Inserting {len(genre_records)} genres...")
        db.conn.execute("BEGIN TRANSACTION")
        try:
            sql = "INSERT OR IGNORE INTO genre (name) VALUES (?)"
            db.cursor.executemany(sql, genre_records)
            db.conn.commit()
            logger.info("Successfully inserted genres")
            
            # Retrieve mapping from genre names to internal IDs.
            cursor = db.conn.cursor()
            cursor.execute("SELECT id, name FROM genre")
            for genre_id, name in cursor.fetchall():
                matching_tags = tags_df[tags_df['tag_name'] == name]
                if not matching_tags.empty:
                    tag_id = int(matching_tags.iloc[0]['tag_id'])
                    tag_id_to_genre_id[tag_id] = genre_id
            
            logger.info(f"Created tag-to-genre mapping with {len(tag_id_to_genre_id)} entries")
            
            # Process book-tag relationships.
            logger.info(f"Processing book-genre relationships from {book_tags_file}...")
            book_tags_df = pd.read_csv(book_tags_file)
            logger.info(f"Loaded {len(book_tags_df)} book-tag relationships from CSV")
            
            columns = book_tags_df.columns.tolist()
            logger.info(f"Book-Tags CSV columns: {columns}")
            
            # Build mapping for books.
            cursor.execute("SELECT id, external_id FROM book WHERE external_id IS NOT NULL")
            book_id_map = {int(ext_id): int_id for int_id, ext_id in cursor.fetchall()}
            
            book_genre_records = []
            processed = 0
            skipped = 0
            
            for _, book_tag in tqdm(book_tags_df.iterrows(), total=len(book_tags_df), desc="Processing book-genre relationships"):
                try:
                    goodreads_book_id = int(book_tag['goodreads_book_id']) if 'goodreads_book_id' in book_tag and not pd.isna(book_tag['goodreads_book_id']) else None
                    tag_id = int(book_tag['tag_id']) if 'tag_id' in book_tag and not pd.isna(book_tag['tag_id']) else None
                    
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
            
            if book_genre_records:
                logger.info(f"Inserting {len(book_genre_records)} book-genre relationships...")
                db.conn.execute("BEGIN TRANSACTION")
                try:
                    sql = "INSERT OR IGNORE INTO book_genre (book_id, genre_id) VALUES (?, ?)"
                    db.cursor.executemany(sql, book_genre_records)
                    db.conn.commit()
                    logger.info("Successfully inserted book-genre relationships")
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
    """
    Generate ratings history records based on existing books and ratings.

    Args:
        db: Instance of AnalyticsDB.

    Returns:
        bool: True if history generation is successful, otherwise False.
    """
    logger.info("Generating ratings history records...")
    
    try:
        cursor = db.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM ratings_history")
        if cursor.fetchone()[0] > 0:
            logger.info("Ratings history already exists, skipping generation")
            return True
        
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
        
        # Generate quarterly snapshot dates for the past 3 years.
        today = datetime.now()
        snapshot_dates = []
        for year_offset in range(3):
            for quarter in range(4):
                date = today.replace(
                    year=today.year - year_offset,
                    month=3 * quarter + 1,
                    day=1
                )
                snapshot_dates.append(date.strftime("%Y-%m-%d"))
        snapshot_dates.reverse()  # Process oldest dates first.
        logger.info(f"Generated {len(snapshot_dates)} historical snapshot dates")
        
        history_records = []
        
        for book_id, ext_book_id, final_count, final_rating in tqdm(books, desc="Generating ratings history"):
            # Simulate a growth model for ratings accumulation.
            count_percentages = [
                5 + (95 * ((i+1) / len(snapshot_dates)) * (0.8 + 0.4 * random.random()))
                for i in range(len(snapshot_dates))
            ]
            count_percentages[-1] = 100.0  # Ensure final percentage is 100.
            
            for i, date in enumerate(snapshot_dates):
                count_pct = count_percentages[i]
                cum_count = max(1, int((count_pct / 100.0) * final_count))
                volatility = max(0.05, 0.3 * (1 - (i / len(snapshot_dates))))
                rating = max(1.0, min(5.0, final_rating + (volatility * (random.random() - 0.5))))
                
                history_records.append((
                    book_id,      # Internal book ID.
                    date,         # Snapshot date.
                    cum_count,    # Cumulative ratings count.
                    round(rating, 2)  # Simulated average rating.
                ))
        
        logger.info(f"Inserting {len(history_records)} rating history records...")
        batch_size = 5000
        for i in range(0, len(history_records), batch_size):
            batch = history_records[i:i+batch_size]
            db.conn.execute("BEGIN TRANSACTION")
            try:
                sql = """
                    INSERT INTO ratings_history 
                    (book_id, snapshot_date, cum_ratings_count, avg_rating)
                    VALUES (?, ?, ?, ?)
                """
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
    """
    Verify database contents after import by summarizing record counts.

    Args:
        db_path (str): Path to the SQLite database file.

    Returns:
        dict: A summary dictionary with table names as keys and record counts as values.
    """
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
            
            # Display a sample for non-empty tables.
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


def organize_datasets():
    """
    Organize existing dataset files into a proper structure.
    
    This function is useful for organizing legacy data layouts into the new
    dataset-specific directory structure.
    
    Returns:
        bool: True if organization was successful or wasn't needed, False if errors occurred.
    """
    # Check if we need to organize anything
    if not DATA_DIR.exists():
        logger.info("No data directory exists yet")
        return True
        
    # Check if the existing data files match our expected dataset
    csv_files_in_data = [f for f in DATA_DIR.glob("*.csv") if f.name in DATASET_INFO["files"]]
    
    # If no CSV files in the root data directory, nothing to organize
    if not csv_files_in_data:
        logger.info("No CSV files found in data directory that need organization")
        return True
        
    try:
        # Create dataset directory if it doesn't exist
        DATASET_DIR.mkdir(exist_ok=True)
        
        # Move files to the dataset directory
        for file_path in csv_files_in_data:
            target_path = DATASET_DIR / file_path.name
            if not target_path.exists():
                logger.info(f"Moving {file_path.name} to {DATASET_DIR}")
                shutil.move(str(file_path), str(target_path))
            else:
                logger.info(f"File {target_path} already exists in dataset directory")
                
        logger.info(f"Dataset organization complete: Files moved to {DATASET_DIR}")
        return True
    except Exception as e:
        logger.error(f"Error organizing dataset files: {e}")
        return False
        

def clean_up_duplicate_files():
    """
    Remove duplicate files that might exist in both the data directory and dataset-specific directories.
    
    This is useful after organizing files to clean up redundant copies.
    
    Returns:
        bool: True if cleanup was successful, False if errors occurred.
    """
    try:
        if not DATA_DIR.exists() or not DATASET_DIR.exists():
            return True
            
        # Check for duplicate CSV files
        for file_name in DATASET_INFO["files"]:
            data_dir_file = DATA_DIR / file_name
            dataset_dir_file = DATASET_DIR / file_name
            
            # If file exists in both locations and dataset dir file is valid
            if data_dir_file.exists() and dataset_dir_file.exists():
                if dataset_dir_file.stat().st_size > 1000:  # Basic validation
                    logger.info(f"Removing duplicate file: {data_dir_file}")
                    data_dir_file.unlink()
        
        return True
    except Exception as e:
        logger.error(f"Error cleaning up duplicate files: {e}")
        return False


def main():
    """
    Main entry point when the script is executed directly.
    
    Returns:
        int: Exit code (0 for success, 1 for failure).
    """
    # Check if the dataset is properly organized
    organize_datasets()
    
    # Check if the dataset exists locally
    if dataset_exists():
        logger.info("Dataset already exists locally.")
        clean_up_duplicate_files()
    else:
        # Dataset doesn't exist, attempt to download
        logger.info("Dataset not found locally. Attempting to download...")
        if check_kaggle_credentials():
            if download_dataset():
                logger.info("Dataset downloaded successfully.")
            else:
                logger.error("Failed to download dataset.")
                return 1
        else:
            logger.error("Failed to authenticate with Kaggle API.")
            return 1
    
    # Import data into the database
    logger.info("Importing dataset into the database...")
    if import_dataset_to_db():
        logger.info("Database import completed successfully.")
        return 0
    else:
        logger.error("Database import failed.")
        return 1


if __name__ == "__main__":
    sys.exit(main())

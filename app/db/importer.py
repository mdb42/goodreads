# app/db/importer.py
"""
Handles importing of UCSD Book Graph dataset files into the application's database.
"""

import logging
import json
import gzip
from pathlib import Path
from typing import Dict, List, Optional, Callable, Iterator

from app.db.downloader import DATASET_INFO, FileDownloader  # or adjust import path as needed

logger = logging.getLogger(__name__)


class DatasetImporter:
    """
    Responsible for reading the local dataset files and importing them into the database.
    """

    def __init__(self, config, db):
        """
        Args:
            config: Application configuration dictionary
            db:     Database connection or wrapper
        """
        self.config = config
        self.db = db
        # If you want to re-use the same checks from the downloader
        self.downloader = FileDownloader(config)

    def read_json_chunks(self, file_path: Path, chunk_size: int = 1000) -> Iterator[List[Dict]]:
        """
        Read a gzipped JSON-lines file in chunks.
        """
        try:
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                chunk = []
                for line in f:
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        record = json.loads(line)
                        chunk.append(record)
                        if len(chunk) >= chunk_size:
                            yield chunk
                            chunk = []
                    except json.JSONDecodeError as e:
                        logger.warning(f"Skipping invalid JSON line: {e}")
                        continue

                # Last partial chunk
                if chunk:
                    yield chunk

        except Exception as e:
            logger.error(f"Error reading {file_path}: {e}")
            raise

    def import_books(self, progress_callback: Optional[Callable] = None) -> bool:
        """
        Import books from the dataset file into the database.
        """
        books_file = Path(self.config["data"]["books"])
        if not books_file.exists():
            logger.error(f"Books file not found: {books_file}")
            return False

        logger.info(f"Importing books from {books_file}...")

        try:
            chunk_size = 1000
            total_books = 0
            author_set = set()
            book_author_pairs = []

            # Add collection for genres/shelves
            shelf_counts = {}  # shelf_name -> total occurrences
            book_shelf_pairs = []  # (book_id, shelf_name, count)

            self.db.optimize_for_bulk_import()

            total_records_estimate = books_file.stat().st_size // 2000  # Rough estimate

            for chunk_num, books_chunk in enumerate(self.read_json_chunks(books_file, chunk_size)):
                book_records = []
                for book in books_chunk:
                    try:
                        book_id = book.get('book_id')
                        title = book.get('title', 'Unknown Title')
                        isbn = book.get('isbn')
                        isbn13 = book.get('isbn13')
                        language = book.get('language_code')
                        avg_rating = book.get('average_rating')
                        ratings_count = book.get('ratings_count')
                        text_reviews_count = book.get('text_reviews_count')
                        publication_date = book.get('publication_date')
                        publisher = book.get('publisher')
                        pages = book.get('num_pages')
                        description = book.get('description')

                        # Authors
                        authors = book.get('authors', [])
                        if isinstance(authors, list):
                            for author in authors:
                                author_id = author.get('author_id')
                                role = author.get('role', '')
                                if author_id:
                                    # Use author_id as the primary identifier
                                    # We'll use a placeholder name based on the ID until we can fetch real names
                                    name = f"Author_{author_id}"  # Placeholder name
                                    author_set.add((author_id, name, role, None))
                                    book_author_pairs.append((book_id, author_id))

                        # Create book record
                        book_records.append((
                            book_id, title, description, isbn, isbn13,
                            publisher, publication_date, language, pages,
                            avg_rating, ratings_count, text_reviews_count
                        ))

                        # Extract popular shelves
                        popular_shelves = book.get('popular_shelves', [])
                        if isinstance(popular_shelves, list):
                            for shelf in popular_shelves:
                                if isinstance(shelf, dict):
                                    shelf_name = shelf.get('name')
                                    count = int(shelf.get('count', 0))
                                    
                                    if shelf_name and count > 0:
                                        # Update global shelf count
                                        shelf_counts[shelf_name] = shelf_counts.get(shelf_name, 0) + count
                                        
                                        # Store relationship with confidence score
                                        book_shelf_pairs.append((book_id, shelf_name, count))

                    except Exception as e:
                        logger.warning(f"Error processing book {book.get('book_id', 'unknown')}: {e}")
                        continue

                if book_records:
                    logger.info(f"Inserting batch {chunk_num+1} with {len(book_records)} books...")
                    if not self.db.batch_insert_books(book_records):
                        logger.error(f"Failed to insert batch {chunk_num+1}")
                        return False

                    total_books += len(book_records)

                # Update progress
                if progress_callback and total_records_estimate > 0:
                    progress_percent = min(95, int((total_books / total_records_estimate) * 100))
                    progress_callback(progress_percent)

                if (chunk_num + 1) % 10 == 0:
                    logger.info(f"Progress: {total_books} books processed so far")

            # Now insert authors
            logger.info(f"Inserting {len(author_set)} unique authors...")
            author_records = list(author_set)
            for i in range(0, len(author_records), 5000):
                batch = author_records[i:i+5000]
                if not self.db.batch_insert_authors(batch):
                    logger.error(f"Failed to insert author batch {i//5000 + 1}")
                    return False

            # Create book-author relationships
            self._create_book_author_relationships(book_author_pairs)

            self.db.restore_normal_settings()
            logger.info(f"Successfully imported {total_books} books")

            logger.info(f"Processing {len(shelf_counts)} unique shelves/genres...")
            self._import_genres(shelf_counts)

            # Create book-genre relationships
            self._create_book_genre_relationships(book_shelf_pairs)

            # Final progress
            if progress_callback:
                progress_callback(100)

            return True

        except Exception as e:
            logger.error(f"Error during book import: {e}", exc_info=True)
            self.db.restore_normal_settings()
            return False

    def _import_genres(self, shelf_counts):
        """Import all shelves as potential genres."""
        logger.info(f"Importing {len(shelf_counts)} unique shelves as genres...")
        
        # Sort shelves by popularity for better analysis later
        sorted_shelves = sorted(shelf_counts.items(), key=lambda x: x[1], reverse=True)
        
        # Log top 50 most common shelves
        logger.info("Top 50 most common shelves:")
        for shelf, count in sorted_shelves[:50]:
            logger.info(f"  {shelf}: {count}")
        
        # Insert all shelves in batches
        batch_size = 5000
        genre_records = [(name, f"User shelf: {name}", None, count) for name, count in sorted_shelves]
        
        for i in range(0, len(genre_records), batch_size):
            batch = genre_records[i:i+batch_size]
            try:
                self.db.executemany(
                    """INSERT OR IGNORE INTO genre 
                    (name, description, parent_id, usage_count)
                    VALUES (?, ?, ?, ?)""",
                    batch
                )
                self.db.commit()
                logger.info(f"Inserted genre batch {i//batch_size + 1}/{(len(genre_records)-1)//batch_size + 1}")
            except Exception as e:
                logger.error(f"Failed to insert genre batch: {e}")
                self.db.rollback()

    def _create_book_genre_relationships(self, book_shelf_pairs):
        """Create relationships between books and genres with confidence scores."""
        logger.info(f"Creating {len(book_shelf_pairs)} book-genre relationships...")
        
        # Build ID mappings
        book_id_map = {}
        cursor = self.db.execute("SELECT id, book_id FROM book")
        for row in cursor.fetchall():
            book_id_map[row[1]] = row[0]
        
        genre_id_map = {}
        cursor = self.db.execute("SELECT id, name FROM genre")
        for row in cursor.fetchall():
            genre_id_map[row[1]] = row[0]
        
        # Create relationship records
        relationship_records = []
        for book_id, shelf_name, count in book_shelf_pairs:
            if book_id in book_id_map and shelf_name in genre_id_map:
                internal_book_id = book_id_map[book_id]
                genre_id = genre_id_map[shelf_name]
                # Use count as confidence score
                relationship_records.append((internal_book_id, genre_id, count))
        
        # Insert relationships in batches
        batch_size = 10000
        total_inserted = 0
        
        for i in range(0, len(relationship_records), batch_size):
            batch = relationship_records[i:i+batch_size]
            try:
                self.db.executemany(
                    "INSERT OR IGNORE INTO book_genre (book_id, genre_id, confidence_score) VALUES (?, ?, ?)",
                    batch
                )
                self.db.commit()
                total_inserted += len(batch)
                logger.info(
                    f"Inserted book-genre relationship batch "
                    f"{i//batch_size + 1}/{(len(relationship_records)-1)//batch_size + 1} "
                    f"({total_inserted}/{len(relationship_records)})"
                )
            except Exception as e:
                logger.error(f"Failed to insert book-genre relationship batch: {e}")
                self.db.rollback()

    def _create_book_author_relationships(self, book_author_pairs):
        """Create relationships between books and authors."""
        logger.info(f"Creating {len(book_author_pairs)} book-author relationships...")

        # Build ID mappings
        book_id_map = {}
        cursor = self.db.execute("SELECT id, book_id FROM book")
        for row in cursor.fetchall():
            book_id_map[row[1]] = row[0]

        author_id_map = {}
        cursor = self.db.execute("SELECT id, author_id FROM author")
        for row in cursor.fetchall():
            author_id_map[row[1]] = row[0]

        # Create relationship records
        relationship_records = []
        for ext_book_id, author_id in book_author_pairs:
            if ext_book_id in book_id_map and author_id in author_id_map:
                relationship_records.append((book_id_map[ext_book_id], author_id_map[author_id]))

        # Insert relationships in batches
        batch_size = 10000
        total_inserted = 0
        
        for i in range(0, len(relationship_records), batch_size):
            batch = relationship_records[i:i+batch_size]
            try:
                self.db.executemany(
                    "INSERT OR IGNORE INTO book_authors (book_id, author_id) VALUES (?, ?)",
                    batch
                )
                self.db.commit()
                total_inserted += len(batch)
                logger.info(
                    f"Inserted book-author relationship batch "
                    f"{i//batch_size + 1}/{(len(relationship_records)-1)//batch_size + 1} "
                    f"({total_inserted}/{len(relationship_records)})"
                )
            except Exception as e:
                logger.error(f"Failed to insert book-author relationship batch: {e}")
                self.db.rollback()

    def import_reviews(self, limit=None, progress_callback=None):
        """Import reviews from the dataset into the database."""
        reviews_file = Path(self.config["data"]["reviews"])
        if not reviews_file.exists():
            logger.error(f"Reviews file not found: {reviews_file}")
            return False

        logger.info(f"Importing reviews from {reviews_file}...")
        try:
            chunk_size = 10000
            logger.info("First pass: collecting unique user IDs...")
            
            # Ensure limit is a number or None, not a function
            review_limit = limit
            if callable(review_limit):
                review_limit = None
                
            self._import_users_from_reviews(reviews_file, chunk_size, review_limit, progress_callback)

            logger.info("Second pass: importing reviews...")
            self._import_review_records(reviews_file, chunk_size, review_limit, progress_callback)
            return True
        except Exception as e:
            logger.error(f"Error during review import: {e}", exc_info=True)
            return False

    def _import_users_from_reviews(self, reviews_file, chunk_size, limit=None, progress_callback=None):
        user_ids = set()
        reviews_processed = 0

        for chunk_num, reviews_chunk in enumerate(self.read_json_chunks(reviews_file, chunk_size)):
            for review in reviews_chunk:
                user_id = review.get('user_id')
                if user_id:
                    user_ids.add(user_id)

            reviews_processed += len(reviews_chunk)
            logger.info(f"Processed {reviews_processed} reviews, found {len(user_ids)} unique users")

            # Update progress (first 30%)
            if progress_callback:
                progress_callback(min(30, int((reviews_processed / (limit or 1e9)) * 30)))

            if limit and reviews_processed >= limit:
                break

        logger.info(f"Found {len(user_ids)} unique users")
        logger.info("Inserting users...")

        user_records = [(uid, f"user_{uid}", 0, None, None) for uid in user_ids]
        batch_size = 10000
        total_batches = (len(user_records) - 1) // batch_size + 1

        for i in range(0, len(user_records), batch_size):
            batch = user_records[i:i+batch_size]
            batch_num = i // batch_size + 1
            logger.info(f"Inserting user batch {batch_num}/{total_batches}...")

            try:
                self.db.executemany(
                    "INSERT OR IGNORE INTO user (user_id, username, review_count, rating_avg, rating_stddev) "
                    "VALUES (?, ?, ?, ?, ?)",
                    batch
                )
                self.db.commit()

                # Update progress (30% -> 50%)
                if progress_callback:
                    progress_percent = 30 + min(20, int((batch_num / total_batches) * 20))
                    progress_callback(progress_percent)
            except Exception as e:
                logger.error(f"Failed to insert user batch {batch_num}: {e}")
                self.db.rollback()

    def _import_review_records(self, reviews_file, chunk_size, limit=None, progress_callback=None):
        logger.info("Building ID mappings...")
        book_id_map = {}
        cursor = self.db.execute("SELECT id, book_id FROM book")
        for row in cursor.fetchall():
            book_id_map[row[1]] = row[0]

        user_id_map = {}
        cursor = self.db.execute("SELECT id, user_id FROM user")
        for row in cursor.fetchall():
            user_id_map[row[1]] = row[0]

        reviews_processed = 0
        reviews_skipped = 0

        for chunk_num, reviews_chunk in enumerate(self.read_json_chunks(reviews_file, chunk_size)):
            review_records = []

            for review in reviews_chunk:
                try:
                    user_id = review.get('user_id')
                    book_id = review.get('book_id')
                    rating = review.get('rating')
                    if not user_id or not book_id or not rating:
                        reviews_skipped += 1
                        continue

                    if user_id not in user_id_map or book_id not in book_id_map:
                        reviews_skipped += 1
                        continue

                    internal_user_id = user_id_map[user_id]
                    internal_book_id = book_id_map[book_id]
                    review_id = review.get('review_id')

                    review_sentences_list = review.get('review_sentences', [])
                    # Combine the second element (the sentence string) from each sublist
                    review_text = " ".join([sentence_data[1] for sentence_data in review_sentences_list if isinstance(sentence_data, list) and len(sentence_data) > 1])

                    date_added = review.get('date_added') # Note: The example JSON uses 'timestamp', you might need review.get('timestamp')

                    # Check if date_added is None or needs conversion if using timestamp
                    if not date_added:
                        date_added = review.get('timestamp') # Fallback to timestamp if date_added missing

                    # Simplified spoiler flag processing based on example record
                    spoiler_flag = 1 if review.get('has_spoiler', False) else 0

                    # Sentiment processing remains the same
                    has_sentiment = 0
                    sentiment_score = None
                    sentiment_magnitude = None

                    # Append the record - ensure tuple order matches INSERT statement below
                    review_records.append((
                        review_id,
                        internal_book_id,
                        internal_user_id,
                        rating,
                        review_text, # Uses the newly constructed review_text
                        date_added,
                        spoiler_flag,
                        has_sentiment,
                        sentiment_score,
                        sentiment_magnitude,
                        0  # helpful_votes - Assuming 0 as it's not in example JSON
                    ))
                except Exception as e:
                    logger.debug(f"Error processing review: {e}")
                    reviews_skipped += 1
                    continue

            if review_records:
                logger.info(f"Inserting reviews batch {chunk_num+1} with {len(review_records)} reviews...")
                try:
                    self.db.executemany(
                        """INSERT OR IGNORE INTO review
                            (review_id, book_id, user_id, rating, review_text, date_added,
                            is_spoiler, has_sentiment, sentiment_score, sentiment_magnitude, helpful_votes)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        review_records
                    )
                    self.db.commit()
                    reviews_processed += len(review_records)

                    # Update progress (50% -> 100%)
                    if progress_callback and limit:
                        progress_percent = 50 + min(50, int((reviews_processed / limit) * 50))
                        progress_callback(progress_percent)
                except Exception as e:
                    logger.error(f"Failed to insert reviews batch {chunk_num+1}: {e}")
                    self.db.rollback()

            logger.info(f"Progress: {reviews_processed} reviews imported, {reviews_skipped} skipped")

            if limit and reviews_processed >= limit:
                logger.info(f"Reached import limit of {limit} reviews")
                break

        logger.info(f"Successfully imported {reviews_processed} reviews")
        if progress_callback:
            progress_callback(100)

    def import_all(self, limit=None, progress_callback=None) -> bool:
        """
        Import all dataset components into the database.
        """
        # If you want to verify existence again before import:
        file_status = self.downloader.check_files_exist()
        if not all(file_status.values()):
            missing = [k for k, v in file_status.items() if not v]
            logger.error(f"Cannot import: missing files: {', '.join(missing)}")
            return False

        # Books
        logger.info("Importing books...")
        if not self.import_books(progress_callback=lambda pct: progress_callback("books", pct) if progress_callback else None):
            logger.error("Book import failed")
            return False

        # Reviews
        logger.info("Importing reviews...")
        if not self.import_reviews(limit, progress_callback=lambda pct: progress_callback("reviews", pct) if progress_callback else None):
            logger.error("Review import failed")
            return False

        # TODO: Import interactions if needed
        # self.import_interactions(...)

        logger.info("Dataset import completed successfully")
        return True

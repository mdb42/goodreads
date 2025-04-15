# app/core/analytics.py
from enum import Enum, auto
from pathlib import Path
import logging
import math
import json
import gzip
from collections import Counter, defaultdict
from PyQt6.QtCore import pyqtSignal

from app.gui.main_window import MainWindow
from app.db.database import Database

class AppState(Enum):
    """Application state enumeration for managing transitions."""
    INITIALIZING = auto()
    SETUP_NEEDED = auto()   # Database setup required
    DOWNLOADING = auto()    # Downloading dataset files
    IMPORTING = auto()      # Importing data into database
    PROCESSING = auto()     # Processing data/analytics
    READY = auto()          # Ready for analysis
    CLOSING = auto()        # Application shutting down

class AnalyticsEngine:
    """
    Core analytics engine for the Goodreads Analytics Tool.
    
    This engine handles database connectivity, data processing, and analytics
    operations, serving as the bridge between the UI and the underlying data.
    """
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.state = AppState.INITIALIZING
        self.state_changed = pyqtSignal(AppState)
        self.app_window = None
        self.platform_stats = None

        # Initialize database
        self._initialize_database()
        
        # Calculate platform statistics on startup
        self._calculate_platform_statistics()
        
        # Show appropriate window based on application state
        self._initialize_ui()

    def _initialize_database(self):
        """Initialize database connection and check schema status."""
        try:
            db_path = Path(self.config["data"]["database"])
            
            # Create the database directory if it doesn't exist
            db_path.parent.mkdir(exist_ok=True, parents=True)
            
            # Initialize database with the config
            self.db = Database(self.config)
            
            # Check if database needs setup
            if not db_path.exists() or not self.db.is_initialized():
                self.logger.info("Database setup required")
                self.state = AppState.SETUP_NEEDED
            else:
                self.logger.info("Database is ready")
                self.state = AppState.READY
                
        except Exception as e:
            self.logger.error(f"Database initialization error: {e}")
            self.state = AppState.SETUP_NEEDED

    def _calculate_platform_statistics(self):
        """Calculate and store key platform statistics during initialization."""
        if self.state != AppState.READY:
            return
            
        try:
            # Get basic database statistics
            db_stats = self.db.get_database_stats()
            
            # Rating distribution
            rating_dist = self._get_rating_distribution()
            
            # User review count distribution
            review_count_dist = self._get_review_count_distribution()
            
            # Rating average distribution
            avg_rating_dist = self._get_avg_rating_distribution()
            
            # Put it all together
            self.platform_stats = {
                "database_stats": db_stats,
                "rating_distribution": rating_dist,
                "review_count_distribution": review_count_dist,
                "avg_rating_distribution": avg_rating_dist
            }
            
            # Log a summary
            self.logger.info(f"Platform statistics calculated:")
            self.logger.info(f"Total books: {db_stats.get('books', 0):,}")
            self.logger.info(f"Total reviews: {db_stats.get('reviews', 0):,}")
            self.logger.info(f"Total users: {db_stats.get('users', 0):,}")
            
            # Specific details about negative reviewers
            self._analyze_negative_reviewers()
            
        except Exception as e:
            self.logger.error(f"Error calculating platform statistics: {e}")
            self.platform_stats = {}

    def _get_rating_distribution(self):
        """Get the distribution of individual ratings."""
        try:
            query = """
            SELECT rating, COUNT(*) as count
            FROM review
            GROUP BY rating
            ORDER BY rating
            """
            cursor = self.db.execute(query)
            distribution = {row[0]: row[1] for row in cursor.fetchall()}
            return distribution
        except Exception as e:
            self.logger.error(f"Error getting rating distribution: {e}")
            return {}

    def _get_review_count_distribution(self):
        """Get the distribution of review counts per user."""
        try:
            query = """
            SELECT 
                CASE
                    WHEN review_count BETWEEN 1 AND 5 THEN '1-5'
                    WHEN review_count BETWEEN 6 AND 10 THEN '6-10'
                    WHEN review_count BETWEEN 11 AND 20 THEN '11-20'
                    WHEN review_count BETWEEN 21 AND 50 THEN '21-50'
                    WHEN review_count BETWEEN 51 AND 100 THEN '51-100'
                    ELSE '100+'
                END as review_range,
                COUNT(*) as user_count
            FROM (
                SELECT u.id, COUNT(r.id) as review_count
                FROM user u
                JOIN review r ON u.id = r.user_id
                GROUP BY u.id
            ) as user_stats
            GROUP BY review_range
            ORDER BY MIN(review_count)
            """
            cursor = self.db.execute(query)
            distribution = {row[0]: row[1] for row in cursor.fetchall()}
            return distribution
        except Exception as e:
            self.logger.error(f"Error getting review count distribution: {e}")
            return {}

    def _get_avg_rating_distribution(self):
        """Get the distribution of average ratings per user."""
        try:
            query = """
            SELECT 
                CAST(ROUND(avg_rating * 2) / 2 AS TEXT) as rating_bin,
                COUNT(*) as user_count
            FROM (
                SELECT u.id, AVG(r.rating) as avg_rating, COUNT(r.id) as review_count
                FROM user u
                JOIN review r ON u.id = r.user_id
                GROUP BY u.id
                HAVING review_count >= 5
            ) as user_stats
            GROUP BY rating_bin
            ORDER BY rating_bin
            """
            cursor = self.db.execute(query)
            distribution = {row[0]: row[1] for row in cursor.fetchall()}
            return distribution
        except Exception as e:
            self.logger.error(f"Error getting average rating distribution: {e}")
            return {}

    def _analyze_negative_reviewers(self):
        """Analyze users with consistently negative reviews."""
        try:
            # Count users with different thresholds
            thresholds = [
                (1.5, 5), (1.5, 10), (1.5, 20),
                (2.0, 5), (2.0, 10), (2.0, 20),
                (2.5, 5), (2.5, 10), (2.5, 20)
            ]
            
            results = {}
            for rating, min_reviews in thresholds:
                query = """
                SELECT COUNT(*) as count
                FROM (
                    SELECT u.id
                    FROM user u
                    JOIN review r ON u.id = r.user_id
                    GROUP BY u.id
                    HAVING AVG(r.rating) <= ? AND COUNT(r.id) >= ?
                )
                """
                cursor = self.db.execute(query, (rating, min_reviews))
                count = cursor.fetchone()[0]
                results[f"below_{rating}_min_{min_reviews}"] = count
            
            # Get the most negative reviewer
            query = """
            SELECT 
                u.user_id, 
                COUNT(r.id) as review_count, 
                AVG(r.rating) as avg_rating,
                MIN(r.rating) as min_rating,
                MAX(r.rating) as max_rating
            FROM user u
            JOIN review r ON u.id = r.user_id
            GROUP BY u.id
            HAVING COUNT(r.id) >= 10
            ORDER BY avg_rating ASC
            LIMIT 1
            """
            cursor = self.db.execute(query)
            most_negative = cursor.fetchone()
            
            if most_negative:
                self.logger.info(f"Most negative reviewer: {most_negative[0]}")
                self.logger.info(f"  Review count: {most_negative[1]}")
                self.logger.info(f"  Average rating: {most_negative[2]:.2f}")
                self.logger.info(f"  Rating range: {most_negative[3]}-{most_negative[4]}")
                
                # Add to platform stats
                if self.platform_stats:
                    self.platform_stats["negative_reviewer_counts"] = results
                    self.platform_stats["most_negative_reviewer"] = {
                        "user_id": most_negative[0],
                        "review_count": most_negative[1],
                        "avg_rating": most_negative[2],
                        "min_rating": most_negative[3],
                        "max_rating": most_negative[4]
                    }
            
        except Exception as e:
            self.logger.error(f"Error analyzing negative reviewers: {e}")

    def _initialize_ui(self):
        """Initialize the appropriate UI based on application state."""
        try:
            # Create main window passing the engine reference so it can access the database
            self.app_window = MainWindow(self)
            
            # If setup is needed, show setup interface
            if self.state == AppState.SETUP_NEEDED:
                self.logger.info("Showing setup interface")
                self.app_window.show_setup_interface()
            else:
                self.logger.info("Showing main interface")
                self.app_window.show_main_interface()
                
            # Show the window
            self.app_window.show()
            
        except Exception as e:
            self.logger.error(f"UI initialization error: {e}")
            raise

    def close(self):
        """Initiate application shutdown procedures."""
        self.logger.info("Initiating application shutdown...")
        self.state = AppState.CLOSING
        self._cleanup()
    
    def _cleanup(self):
        """Perform final cleanup operations."""
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

    def update_state(self, new_state, status_message=None):
        """Update application state and log the change."""
        self.state = new_state
        self.logger.info(f"Application state changed to: {new_state.name}")
        
        if status_message and hasattr(self, 'app_window') and self.app_window:
            if hasattr(self.app_window, 'status_bar'):
                self.app_window.status_bar.showMessage(status_message)
                
    def initialize_review_index(self):
        """Initialize the review index for sentiment analysis and similar review finding."""
        from app.core.review_indexer import ReviewIndex
        
        # Get paths to stopwords and special chars files from config
        stopwords_file = self.config.get("data", {}).get("stopwords_file")
        special_chars_file = self.config.get("data", {}).get("special_chars_file")
        
        self.review_index = ReviewIndex(self.db, stopwords_file, special_chars_file)
        self.logger.info("Review index initialized")
        
    def find_negative_reviewers_sql(self, min_reviews=20, max_avg_rating=2.0, keywords=None, genre=None):
        """
        Find persistently negative reviewers using direct SQL queries.
        
        Args:
            min_reviews (int): Minimum number of reviews required
            max_avg_rating (float): Maximum average rating to qualify as negative
            keywords (list): Optional keywords to search for in review text
            genre (str): Optional genre to filter by
            
        Returns:
            list: List of user data dictionaries
        """
        try:
            # Log parameters
            self.logger.info(f"Searching for negative reviewers with params: " +
                            f"min_reviews={min_reviews}, max_avg_rating={max_avg_rating}")
            
            # Build the base query
            query = """
            SELECT 
                u.id, 
                u.user_id, 
                COUNT(r.id) as review_count, 
                AVG(r.rating) as avg_rating
            FROM user u
            JOIN review r ON u.id = r.user_id
            """
            
            # Add genre filtering if specified
            if genre:
                query += """
                JOIN book b ON r.book_id = b.id
                JOIN book_genre bg ON b.id = bg.book_id
                JOIN genre g ON bg.genre_id = g.id
                WHERE g.name = ?
                """
                params = [genre]
            else:
                params = []
            
            # Add keyword filtering if specified
            if keywords and len(keywords) > 0:
                if genre:
                    query += " AND ("
                else:
                    query += " WHERE ("
                    
                keyword_clauses = []
                for keyword in keywords:
                    keyword_clauses.append("r.review_text LIKE ?")
                    params.append(f"%{keyword}%")
                
                query += " OR ".join(keyword_clauses) + ")"
            
            # Complete the query with grouping and filtering
            query += """
            GROUP BY u.id, u.user_id
            HAVING COUNT(r.id) >= ? AND AVG(r.rating) <= ?
            ORDER BY avg_rating ASC, review_count DESC
            LIMIT 50
            """
            
            # Add the HAVING clause parameters
            params.extend([min_reviews, max_avg_rating])
            
            # Log query details
            self.logger.info(f"Executing query: {query}")
            self.logger.info(f"With parameters: {params}")
            
            # Execute the query
            cursor = self.db.execute(query, params)
            results = cursor.fetchall()
            
            # Log the count
            self.logger.info(f"Query returned {len(results)} results")
            
            # Process the results
            negative_reviewers = []
            for row in results:
                user_details = {
                    'id': row[0],
                    'user_id': row[1],
                    'review_count': row[2],
                    'avg_rating': row[3],
                }
                
                # Add additional calculated fields
                user_details['rating_stddev'] = self._calculate_rating_stddev(row[0])
                user_details['top_genre'] = self._get_top_genre_for_user(row[0])
                
                negative_reviewers.append(user_details)
                
            return negative_reviewers
                
        except Exception as e:
            self.logger.error(f"Error finding negative reviewers: {e}", exc_info=True)
            return []
            
    def _calculate_rating_stddev(self, user_id):
        """Calculate standard deviation of ratings for a user."""
        try:
            query = "SELECT rating FROM review WHERE user_id = ?"
            cursor = self.db.execute(query, (user_id,))
            ratings = [row[0] for row in cursor.fetchall()]
            
            if not ratings:
                return 0.0
                
            mean = sum(ratings) / len(ratings)
            squared_diffs = [(r - mean) ** 2 for r in ratings]
            variance = sum(squared_diffs) / len(ratings)
            return (variance ** 0.5)
        except Exception as e:
            self.logger.error(f"Error calculating rating stddev: {e}")
            return 0.0
            
    def _get_top_genre_for_user(self, user_id):
        """Get the most frequently reviewed genre for a user."""
        try:
            query = """
                SELECT g.name, COUNT(*) as genre_count
                FROM review r
                JOIN book b ON r.book_id = b.id
                JOIN book_genre bg ON b.id = bg.book_id
                JOIN genre g ON bg.genre_id = g.id
                WHERE r.user_id = ?
                GROUP BY g.name
                ORDER BY genre_count DESC
                LIMIT 1
            """
            cursor = self.db.execute(query, (user_id,))
            result = cursor.fetchone()
            return result[0] if result else "Unknown"
        except Exception as e:
            self.logger.error(f"Error getting top genre: {e}")
            return "Unknown"
            
    def get_reviewer_details(self, user_id):
        """
        Get detailed information about a specific reviewer.
        
        Args:
            user_id (str): User ID to get details for
            
        Returns:
            dict: Dictionary of user details and reviews
        """
        try:
            # Get basic user stats
            query = """
                SELECT 
                    u.id, u.user_id, COUNT(r.id) as review_count, 
                    AVG(r.rating) as avg_rating
                FROM user u
                JOIN review r ON u.id = r.user_id
                WHERE u.user_id = ?
                GROUP BY u.id, u.user_id
            """
            cursor = self.db.execute(query, (user_id,))
            user_row = cursor.fetchone()
            
            if not user_row:
                return {'error': 'User not found'}
                
            user_details = {
                'id': user_row[0],
                'user_id': user_row[1],
                'review_count': user_row[2],
                'avg_rating': user_row[3],
                'rating_stddev': self._calculate_rating_stddev(user_row[0]),
                'top_genre': self._get_top_genre_for_user(user_row[0])
            }
            
            # Get expanded user statistics
            stats_query = """
                SELECT 
                    COUNT(CASE WHEN rating = 1 THEN 1 END) as rating_1_count,
                    COUNT(CASE WHEN rating = 2 THEN 1 END) as rating_2_count,
                    COUNT(CASE WHEN rating = 3 THEN 1 END) as rating_3_count,
                    COUNT(CASE WHEN rating = 4 THEN 1 END) as rating_4_count,
                    COUNT(CASE WHEN rating = 5 THEN 1 END) as rating_5_count,
                    MIN(date_added) as first_review_date,
                    MAX(date_added) as last_review_date,
                    AVG(LENGTH(review_text)) as avg_review_length
                FROM review
                WHERE user_id = ?
            """
            cursor = self.db.execute(stats_query, (user_row[0],))
            stats_row = cursor.fetchone()
            
            if stats_row:
                user_details.update({
                    'rating_1_count': stats_row[0],
                    'rating_2_count': stats_row[1],
                    'rating_3_count': stats_row[2],
                    'rating_4_count': stats_row[3],
                    'rating_5_count': stats_row[4],
                    'first_review_date': stats_row[5],
                    'last_review_date': stats_row[6],
                    'avg_review_length': stats_row[7]
                })
            
            # Get reviews with a higher limit (up to 60)
            query = """
                SELECT 
                    r.id, r.rating, r.review_text, r.date_added,
                    b.title as book_title, b.book_id as book_id
                FROM review r
                JOIN book b ON r.book_id = b.id
                WHERE r.user_id = ?
                ORDER BY r.date_added DESC
                LIMIT 60
            """
            cursor = self.db.execute(query, (user_row[0],))
            recent_reviews = []
            for row in cursor.fetchall():
                recent_reviews.append({
                    'id': row[0],
                    'rating': row[1],
                    'review_text': row[2] or "[No text]",
                    'date_added': row[3],
                    'book_title': row[4],
                    'book_id': row[5]
                })
                
            user_details['reviews'] = recent_reviews
            
            return user_details
            
        except Exception as e:
            self.logger.error(f"Error getting reviewer details: {e}")
            return {'error': str(e)}
    
    def get_platform_stats(self):
        """Get the platform statistics summary."""
        if not self.platform_stats:
            self._calculate_platform_statistics()
        return self.platform_stats

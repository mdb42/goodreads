# app/gui/search_widget.py
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, 
    QLineEdit, QSpinBox, QDoubleSpinBox, QComboBox,
    QPushButton, QGroupBox, QFormLayout, QTextEdit,
    QTableWidget, QTableWidgetItem, QHeaderView
)
from PyQt6.QtCore import Qt, pyqtSignal

class ParametricSearchWidget(QWidget):
    """Widget for parametric searching of the Goodreads dataset."""
    
    search_requested = pyqtSignal(dict)  # Signal emitted when search is performed
    
    def __init__(self, analytics_engine, parent=None):
        super().__init__(parent)
        self.analytics = analytics_engine
        self.setup_ui()
        
    def setup_ui(self):
        main_layout = QVBoxLayout(self)
        
        # --- Create status area first so it's available for logging ---
        self.status_text = QTextEdit()
        self.status_text.setReadOnly(True)
        self.status_text.setMaximumHeight(100)
        
        # --- Search Parameters Section ---
        params_group = QGroupBox("Search Parameters")
        params_layout = QFormLayout(params_group)
        
        # User parameters
        self.min_reviews = QSpinBox()
        self.min_reviews.setRange(1, 1000)
        self.min_reviews.setValue(20)
        self.min_reviews.setToolTip("Minimum number of reviews for a user to be included")
        params_layout.addRow("Minimum Reviews:", self.min_reviews)
        
        self.max_avg_rating = QDoubleSpinBox()
        self.max_avg_rating.setRange(1.0, 5.0)
        self.max_avg_rating.setSingleStep(0.1)
        self.max_avg_rating.setValue(2.0)
        self.max_avg_rating.setToolTip("Maximum average rating to qualify as 'negative'")
        params_layout.addRow("Max Average Rating:", self.max_avg_rating)
        
        # Review content parameters
        self.keyword_filter = QLineEdit()
        self.keyword_filter.setPlaceholderText("Optional: Filter by keywords (comma-separated)")
        params_layout.addRow("Keywords:", self.keyword_filter)
        
        self.genre_filter = QComboBox()
        self.genre_filter.addItem("Any Genre", None)
        params_layout.addRow("Genre:", self.genre_filter)
        
        # --- Action Buttons ---
        button_layout = QHBoxLayout()
        
        self.search_btn = QPushButton("Find Negative Reviewers")
        self.search_btn.clicked.connect(self.perform_search)
        button_layout.addWidget(self.search_btn)
        
        self.build_index_btn = QPushButton("Build Review Index")
        self.build_index_btn.clicked.connect(self.build_index)
        button_layout.addWidget(self.build_index_btn)
        
        # Add all sections to main layout
        main_layout.addWidget(params_group)
        main_layout.addLayout(button_layout)
        main_layout.addWidget(QLabel("Status:"))
        main_layout.addWidget(self.status_text)
        
        # Populate genres last, after status_text is created
        self.populate_genres()
        
    def populate_genres(self):
        """Populate the genre filter dropdown with genres from the database."""
        try:
            # Get top 20 genres by usage count
            query = """
                SELECT name, usage_count 
                FROM genre 
                ORDER BY usage_count DESC 
                LIMIT 20
            """
            cursor = self.analytics.db.execute(query)
            results = cursor.fetchall()
            
            # Add genres to the combobox
            for genre, count in results:
                self.genre_filter.addItem(f"{genre} ({count})", genre)
                
            self.log_status(f"Loaded {len(results)} genres for filtering")
        except Exception as e:
            self.log_status(f"Error loading genres: {e}")
    
    def perform_search(self):
        """Execute the search with current parameters."""
        try:
            # Collect search parameters
            params = {
                'min_reviews': self.min_reviews.value(),
                'max_avg_rating': self.max_avg_rating.value(),
                'keywords': [k.strip() for k in self.keyword_filter.text().split(',') if k.strip()],
                'genre': self.genre_filter.currentData()
            }
            
            self.log_status(f"Searching for negative reviewers with parameters: {params}")
            
            # Emit signal with search parameters
            self.search_requested.emit(params)
            
        except Exception as e:
            self.log_status(f"Error performing search: {e}")
    
    def build_index(self):
        """Build the review index for more advanced analysis."""
        try:
            if not hasattr(self.analytics, 'review_index'):
                self.log_status("Initializing review index...")
                self.analytics.initialize_review_index()
            
            # Limit to 10,000 reviews for the demonstration
            self.log_status("Building review index (this may take a while)...")
            stats = self.analytics.build_review_index(limit=10000)
            
            self.log_status(f"Review index built with {stats['document_count']} documents.")
            self.log_status(f"Vocabulary size: {stats['vocabulary_size']} terms")
            self.log_status(f"Unique users: {stats['unique_users']}")
            
        except Exception as e:
            self.log_status(f"Error building index: {e}")
    
    def log_status(self, message):
        """Add a message to the status area."""
        self.status_text.append(message)
        # Scroll to the bottom
        scrollbar = self.status_text.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

    
# app/gui/results_widget.py
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, 
    QTableWidget, QTableWidgetItem, QHeaderView,
    QPushButton, QTextEdit, QSplitter, QMainWindow
)
from PyQt6.QtCore import Qt, pyqtSlot, pyqtSignal

class ResultsWidget(QWidget):
    """Widget to display search results and analysis findings."""

    back_to_search_requested = pyqtSignal()
    
    def __init__(self, analytics_engine, parent=None):
        super().__init__(parent)
        self.analytics = analytics_engine
        self.setup_ui()
        
    def setup_ui(self):
        main_layout = QVBoxLayout(self)
        
        # Create a splitter to divide the results area
        self.splitter = QSplitter(Qt.Orientation.Vertical)
        
        # --- Results Table ---
        self.table_widget = QTableWidget()
        self.table_widget.setColumnCount(5)
        self.table_widget.setHorizontalHeaderLabels([
            "User ID", "Review Count", "Avg Rating", "Rating StdDev", "Top Genre"
        ])
        self.table_widget.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.table_widget.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table_widget.clicked.connect(self.show_user_details)
        
        # --- Detail View ---
        self.detail_widget = QWidget()
        detail_layout = QVBoxLayout(self.detail_widget)
        
        self.detail_label = QLabel("Select a reviewer to see details")
        detail_layout.addWidget(self.detail_label)
        
        self.review_text = QTextEdit()
        self.review_text.setReadOnly(True)
        detail_layout.addWidget(self.review_text)
        
        # --- Back Button ---
        back_layout = QHBoxLayout()
        self.back_btn = QPushButton("Back to Search")
        self.back_btn.clicked.connect(self.go_back_to_search)
        back_layout.addWidget(self.back_btn)
        back_layout.addStretch()

        # Add widgets to splitter
        self.splitter.addWidget(self.table_widget)
        self.splitter.addWidget(self.detail_widget)
        self.splitter.setSizes([200, 300])  # Set initial sizes
        
        main_layout.addLayout(back_layout)
        main_layout.addWidget(self.splitter)
    
    def go_back_to_search(self):
        """Return to the search screen."""
        self.back_to_search_requested.emit()
        
    @pyqtSlot(dict)
    def handle_search_results(self, results):
        """Handle search results from the analytics engine."""
        self.table_widget.setRowCount(0)  # Clear existing rows
        
        if not results:
            self.review_text.setText("No results found matching the criteria.")
            return
            
        # Populate the table with results
        self.table_widget.setRowCount(len(results))
        
        for row, user_data in enumerate(results):
            self.table_widget.setItem(row, 0, QTableWidgetItem(str(user_data['user_id'])))
            self.table_widget.setItem(row, 1, QTableWidgetItem(str(user_data['review_count'])))
            self.table_widget.setItem(row, 2, QTableWidgetItem(f"{user_data['avg_rating']:.2f}"))
            self.table_widget.setItem(row, 3, QTableWidgetItem(f"{user_data['rating_stddev']:.2f}"))
            
            # Try to get top genre if available
            top_genre = user_data.get('top_genre', "Unknown")
            self.table_widget.setItem(row, 4, QTableWidgetItem(top_genre))
            
        self.review_text.setText(f"Found {len(results)} negative reviewers matching criteria.")
        
    def show_user_details(self):
        """Show details for the selected reviewer."""
        selected_rows = self.table_widget.selectedItems()
        if not selected_rows:
            return
            
        # Get the user ID from the first column
        row = selected_rows[0].row()
        user_id = self.table_widget.item(row, 0).text()
        
        try:
            # Get user details from analytics engine
            user_details = self.analytics.get_reviewer_details(user_id)
            
            if 'error' in user_details:
                self.review_text.setText(f"Error: {user_details['error']}")
                return
                
            # Display user details
            details_text = f"User ID: {user_id}\n\n"
            details_text += f"Average Rating: {user_details['avg_rating']:.2f} (StdDev: {user_details['rating_stddev']:.2f})\n"
            details_text += f"Reviews: {user_details['review_count']}\n"
            
            # Add rating distribution
            details_text += f"\nRating Distribution:\n"
            details_text += f"★☆☆☆☆: {user_details.get('rating_1_count', 0)} reviews\n"
            details_text += f"★★☆☆☆: {user_details.get('rating_2_count', 0)} reviews\n"
            details_text += f"★★★☆☆: {user_details.get('rating_3_count', 0)} reviews\n"
            details_text += f"★★★★☆: {user_details.get('rating_4_count', 0)} reviews\n"
            details_text += f"★★★★★: {user_details.get('rating_5_count', 0)} reviews\n"
            
            details_text += f"\nTop Genre: {user_details['top_genre']}\n"
            details_text += f"Average Review Length: {int(user_details.get('avg_review_length', 0))} characters\n"
            details_text += f"Review Period: {user_details.get('first_review_date', 'Unknown')} - {user_details.get('last_review_date', 'Unknown')}\n"
            
            details_text += "\nRecent Reviews:\n"
            for i, review in enumerate(user_details.get('reviews', [])):
                stars = "★" * review['rating'] + "☆" * (5 - review['rating'])
                details_text += f"\n--- {i+1}. {review['book_title']} ({stars}) ---\n"
                
                # Format date if available
                date_str = f" on {review['date_added']}" if review['date_added'] else ""
                details_text += f"Rated {review['rating']}/5{date_str}\n"
                
                # Include full review text
                review_text = review['review_text']
                if review_text and review_text != "[No text]":
                    details_text += f"{review_text}\n"
                else:
                    details_text += "[No review text]\n"
                    
            self.review_text.setText(details_text)
            
        except Exception as e:
            self.review_text.setText(f"Error fetching user details: {e}")
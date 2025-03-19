# app/gui/home_widget.py
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton, 
    QLabel, QFrame, QSizePolicy, QSpacerItem
)
from PyQt6.QtCore import Qt, QTimer, QPropertyAnimation, QEasingCurve, QSize
from PyQt6.QtGui import QFont, QColor
import qtawesome as qta

class HomeWidget(QWidget):
    """
    Main home screen for the application.
    
    Features:
    - Quick access buttons for key functions
    - Database statistics summary
    - Recent activity panel
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self.parent = parent
        self.setup_ui()
        self.animations = []
        
        # Simple animation timer for any ongoing animations
        self.animation_timer = QTimer()
        self.animation_timer.timeout.connect(self.update_animations)
        self.last_frame_time = 0
        self.delta_time = 0
        
    def setup_ui(self):
        main_layout = QVBoxLayout(self)
        
        # Welcome header
        header = QLabel("Welcome to Goodreads Analytics")
        header.setAlignment(Qt.AlignmentFlag.AlignCenter)
        header.setFont(QFont("Arial", 24, QFont.Weight.Bold))
        main_layout.addWidget(header)
        
        # Quick action buttons section
        action_frame = QFrame()
        action_frame.setFrameShape(QFrame.Shape.StyledPanel)
        action_frame.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Maximum)
        
        action_layout = QHBoxLayout(action_frame)
        
        # Search books button
        search_btn = self.create_action_button(
            "Search Books", 
            "fa5s.search", 
            "Search the book database"
        )
        
        # User analysis button
        users_btn = self.create_action_button(
            "User Analysis", 
            "fa5s.users", 
            "Analyze reader behaviors"
        )
        
        # Genre analysis button
        genres_btn = self.create_action_button(
            "Genres", 
            "fa5s.tags", 
            "Explore book genres"
        )
        
        # Add buttons to layout
        action_layout.addWidget(search_btn)
        action_layout.addWidget(users_btn)
        action_layout.addWidget(genres_btn)
        
        main_layout.addWidget(action_frame)
        
        # Stats summary panel
        stats_frame = QFrame()
        stats_frame.setFrameShape(QFrame.Shape.StyledPanel)
        stats_layout = QVBoxLayout(stats_frame)
        
        stats_title = QLabel("Database Statistics")
        stats_title.setFont(QFont("Arial", 14, QFont.Weight.Bold))
        stats_layout.addWidget(stats_title)
        
        # Placeholder for actual stats that will come from the database
        stats_content = QLabel("Loading database statistics...")
        stats_layout.addWidget(stats_content)
        self.stats_content = stats_content  # Save reference for updating later
        
        main_layout.addWidget(stats_frame)
        
        # Add spacer to push everything up
        main_layout.addItem(QSpacerItem(20, 40, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Expanding))
    
    def create_action_button(self, text, icon_name, tooltip=""):
        """Create a styled action button with icon and text"""
        button = QPushButton()
        button.setToolTip(tooltip)
        button.setMinimumHeight(120)
        
        layout = QVBoxLayout(button)
        layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
        
        icon = qta.icon(icon_name, color="white" if self.is_dark_mode() else "black")
        icon_label = QLabel()
        icon_label.setPixmap(icon.pixmap(64, 64))
        icon_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        
        text_label = QLabel(text)
        text_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        text_label.setFont(QFont("Arial", 12))
        
        layout.addWidget(icon_label)
        layout.addWidget(text_label)
        
        return button
    
    def is_dark_mode(self):
        """Check if dark mode is enabled"""
        if self.parent and hasattr(self.parent, 'dark_mode'):
            return self.parent.dark_mode
        return False
    
    def update_stats(self, stats_data):
        """Update the stats display with actual database statistics"""
        if not stats_data:
            self.stats_content.setText("No statistics available")
            return
            
        stats_text = f"""
        <b>Books:</b> {stats_data.get('books', 0):,}<br>
        <b>Authors:</b> {stats_data.get('authors', 0):,}<br>
        <b>Reviews:</b> {stats_data.get('reviews', 0):,}<br>
        <b>Users:</b> {stats_data.get('users', 0):,}<br>
        <b>Average Rating:</b> {stats_data.get('avg_rating', 0):.2f}/5.0
        """
        self.stats_content.setText(stats_text)
    
    # Animation handling methods
    def start_animation(self, animation):
        """Add and start a new animation"""
        self.animations.append(animation)
        animation.start()
        
        if not self.animation_timer.isActive():
            self.last_frame_time = 0
            self.animation_timer.start(16)  # ~60 FPS
    
    def update_animations(self):
        """Update ongoing animations"""
        current_time = QTimer.currentTime().msecsSinceStartOfDay()
        
        if self.last_frame_time > 0:
            self.delta_time = (current_time - self.last_frame_time) / 1000.0
        
        self.last_frame_time = current_time
        
        # Remove completed animations
        self.animations = [anim for anim in self.animations if not anim.state() == QPropertyAnimation.State.Stopped]
        
        # Stop timer if no animations are running
        if not self.animations:
            self.animation_timer.stop()

    def update_icon_colors(self, is_dark_mode):
        """
        Update the icon colors in the UI based on the current theme.

        Args:
            is_dark_mode (bool): True if dark mode is active, otherwise False.
        """
        icon_color = "white" if is_dark_mode else "black"
        
        # Update icons
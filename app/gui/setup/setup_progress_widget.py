from PyQt6.QtWidgets import QWidget, QVBoxLayout, QLabel, QProgressBar, QStackedWidget
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QFont

class SetupProgressWidget(QWidget):
    """
    Displays either instructional text or progress information
    using a stacked widget to maintain consistent layout.
    """
    # States
    IDLE = 0
    ACTIVE = 1
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setup_ui()
        
    def setup_ui(self):
        layout = QVBoxLayout(self)
        
        # Create a stacked widget to hold our two states
        self.stacked_widget = QStackedWidget()
        
        # Page 1: Idle state with instructions
        self.idle_widget = QWidget()
        idle_layout = QVBoxLayout(self.idle_widget)
        
        self.instruction_label = QLabel("Click 'Download Missing Files' to begin setup")
        self.instruction_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.instruction_label.setFont(QFont("Arial", 10))
        
        idle_layout.addWidget(self.instruction_label)
        idle_layout.setContentsMargins(20, 20, 20, 20)  # Add padding
        
        # Page 2: Active operation with progress
        self.active_widget = QWidget()
        active_layout = QVBoxLayout(self.active_widget)
        
        self.operation_label = QLabel("Operation in progress...")
        self.operation_label.setAlignment(Qt.AlignmentFlag.AlignLeft)
        
        self.operation_progress = QProgressBar()
        self.operation_progress.setRange(0, 100)
        self.operation_progress.setValue(0)
        
        active_layout.addWidget(self.operation_label)
        active_layout.addWidget(self.operation_progress)
        
        # Add both pages to the stacked widget
        self.stacked_widget.addWidget(self.idle_widget)
        self.stacked_widget.addWidget(self.active_widget)
        
        # Start with the idle page
        self.stacked_widget.setCurrentIndex(self.IDLE)
        
        # Add stacked widget to main layout
        layout.addWidget(self.stacked_widget)
        
        # Set a consistent height
        self.setMinimumHeight(100)
        
    def set_instruction(self, message: str):
        """Update the instruction message in idle state."""
        self.instruction_label.setText(message)
        self.stacked_widget.setCurrentIndex(self.IDLE)
    
    def set_operation(self, message: str):
        """Set the current operation message and reset its progress."""
        self.operation_label.setText(message)
        self.operation_progress.setValue(0)
        self.stacked_widget.setCurrentIndex(self.ACTIVE)
    
    def set_operation_progress(self, value: int):
        """Update the current operation progress."""
        self.operation_progress.setValue(value)
        # Ensure we're showing the active state
        if self.stacked_widget.currentIndex() != self.ACTIVE:
            self.stacked_widget.setCurrentIndex(self.ACTIVE)
        
    def complete_operation(self):
        """Return to idle state when operation completes."""
        self.operation_progress.setValue(100)
        # Could add a small delay before switching back
        self.stacked_widget.setCurrentIndex(self.IDLE)

    def update_instruction_based_on_status(self, files_status, db_initialized):
        """Update instruction text based on current setup status."""
        if not all(files_status.values()):
            missing = [k for k, v in files_status.items() if not v]
            self.set_instruction(f"Missing files: {', '.join(missing)}. Click 'Download Missing Files'.")
        elif not db_initialized:
            self.set_instruction("Files ready. Click 'Initialize Database' to continue.")
        else:
            self.set_instruction("Setup complete! Click 'Begin Analysis' to start.")
# src/utils/logger.py
"""
CSC790 Information Retrieval - Final Project
Goodreads Sentiment Analysis and Information Retrieval System

Module: logger.py

This module provides logging functionality for the system with color-coded
console output and file-based logging. It supports both terminal display
with visual formatting and persistent log files for debugging.

Authors:
    Matthew D. Branson (branson773@live.missouristate.edu)
    James R. Brown (brown926@live.missouristate.edu)

Missouri State University
Department of Computer Science
May 1, 2025
"""

import logging
import os
from datetime import datetime

class ColorFormatter(logging.Formatter):
    """
    Custom log formatter that adds color coding to console log messages.
    
    This formatter applies ANSI color codes to log messages based on their
    severity level, making it easier to visually distinguish between
    different types of log messages in the terminal.
    
    Attributes:
        COLORS (dict): Mapping of log levels to ANSI color codes
        RESET (str): ANSI code to reset text color
    """
    COLORS = {
        'DEBUG': '\033[94m',    # Blue
        'INFO': '\033[92m',     # Green
        'WARNING': '\033[93m',  # Yellow
        'ERROR': '\033[91m',    # Red
        'CRITICAL': '\033[95m', # Magenta
    }
    RESET = '\033[0m'

    def format(self, record):
        """
        Format the log record with appropriate color coding.
        
        Args:
            record: Log record to format
            
        Returns:
            str: Color-coded formatted log message
        """
        color = self.COLORS.get(record.levelname, self.RESET)
        message = super().format(record)
        return f"{color}{message}{self.RESET}"

def setup_logger(log_dir="logs"):
    """
    Set up and configure the logging system.
    
    This function:
    1. Creates a logs directory if it doesn't exist
    2. Sets up a root logger with both console and file handlers
    3. Configures color-coded console output
    4. Creates a timestamped log file for persistent logging
    
    Args:
        log_dir (str): Directory to store log files (default: "logs")
        
    Returns:
        logging.Logger: Configured logger instance
    """
    # Create logs directory if it doesn't exist
    os.makedirs(log_dir, exist_ok=True)
    
    # Create timestamped log filename
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    log_path = os.path.join(log_dir, f"run_{timestamp}.log")

    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # Define log format
    formatter = logging.Formatter(
        "[%(levelname)s] [%(asctime)s] [%(module)s:%(funcName)s] %(message)s",
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Add color-coded console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(ColorFormatter(
        "[%(levelname)s] [%(asctime)s] [%(module)s:%(funcName)s] %(message)s",
        datefmt='%Y-%m-%d %H:%M:%S'
    ))
    logger.addHandler(console_handler)

    # Add file handler for persistent logging
    file_handler = logging.FileHandler(log_path, mode='w', encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger

def get_logger(name=None):
    """
    Get the existing logger by name, or the root logger if None.
    
    This function provides a convenient way to access the logger
    from different modules without reconfiguring it.
    
    Args:
        name (str, optional): Logger name, typically the module name
        
    Returns:
        logging.Logger: Logger instance
    """
    return logging.getLogger(name)

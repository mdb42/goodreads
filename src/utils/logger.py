# src/logger.py

import logging
import os
from datetime import datetime

class ColorFormatter(logging.Formatter):
    COLORS = {
        'DEBUG': '\033[94m',
        'INFO': '\033[92m',
        'WARNING': '\033[93m',
        'ERROR': '\033[91m',
        'CRITICAL': '\033[95m',
    }
    RESET = '\033[0m'

    def format(self, record):
        color = self.COLORS.get(record.levelname, self.RESET)
        message = super().format(record)
        return f"{color}{message}{self.RESET}"

def setup_logger(log_dir="logs"):
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    log_path = os.path.join(log_dir, f"run_{timestamp}.log")

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        "[%(levelname)s] [%(asctime)s] [%(module)s:%(funcName)s] %(message)s",
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(ColorFormatter(
        "[%(levelname)s] [%(asctime)s] [%(module)s:%(funcName)s] %(message)s",
        datefmt='%Y-%m-%d %H:%M:%S'
    ))
    logger.addHandler(console_handler)

    file_handler = logging.FileHandler(log_path, mode='w', encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger

def get_logger(name=None):
    """Get the existing logger by name, or the root logger if None."""
    return logging.getLogger(name)

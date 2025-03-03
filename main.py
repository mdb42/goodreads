#!/usr/bin/env python3
"""
Main application entry point for the Goodreads Analytics Tool.

This module initializes the application, handles command-line arguments,
and manages the startup of different components based on user options.
"""

import sys
import json
import logging
import argparse
import importlib.resources as resources
from pathlib import Path
from PyQt6.QtWidgets import QApplication
from PyQt6.QtGui import QGuiApplication
from app.src.analytics import AnalyticsEngine
import signal
from app.src.dataset_loader import download_dataset, import_dataset_to_db, verify_database

def load_config():
    """Load application configuration from config.json or create from default."""
    config_path = Path("config.json")    
    if config_path.exists():
        with open(config_path, 'r') as f:
            return json.load(f)
    else:
        # If no config exists, create one from the default template
        default_config_str = resources.read_text("resources", "default_config.json")
        default_config = json.loads(default_config_str)
        with open(config_path, 'w') as f:
            json.dump(default_config, indent=4, fp=f)
                
        return default_config

def setup_logging(config):
    """Configure application-wide logging based on config settings."""
    log_path = Path(config["logging"]["file_path"])
    log_path.parent.mkdir(exist_ok=True)
    
    logging.basicConfig(
        level=getattr(logging, config["logging"]["level"]),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler(sys.stdout)
        ]
    )

def setup_signal_handling(app, analytics, logger):
    """Configure graceful shutdown on SIGINT and SIGTERM."""
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
        analytics.close()
        app.quit()    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

def setup_exception_hook(logger):
    """Configure global exception handling to ensure all unhandled exceptions are logged."""
    def exception_hook(exctype, value, traceback):
        logger.critical("Uncaught exception:", exc_info=(exctype, value, traceback))
        sys.__excepthook__(exctype, value, traceback)  # Call the default handler
    sys.excepthook = exception_hook

def parse_arguments():
    """Parse command-line arguments for the application."""
    parser = argparse.ArgumentParser(
        description="Goodreads Data Analysis Tool",
        epilog="Example: python main.py --rebuild-db"
    )
    
    # Command line options
    parser.add_argument(
        "--rebuild-db", 
        action="store_true",
        help="Rebuild the database with Goodreads dataset"
    )
    
    parser.add_argument(
        "--force-download", 
        action="store_true",
        help="Force download even if dataset exists locally"
    )
    
    parser.add_argument(
        "--limit-ratings", 
        type=int,
        help="Limit the number of ratings to import (useful for testing)"
    )
    
    parser.add_argument(
        "--verify-db",
        action="store_true",
        help="Verify database contents"
    )
    
    parser.add_argument(
        "--db-path",
        default="data/analytics.db",
        help="Path to the database file (default: data/analytics.db)"
    )
    
    return parser.parse_args()

def rebuild_db(args):
    """Handle the rebuild-db command."""
    # Quick setup for logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    logger.info("Rebuilding database with Goodreads dataset")
    
    # Force download if requested
    if args.force_download or not download_dataset():
        if not download_dataset():
            logger.error("Failed to download dataset")
            return 1
    
    # Import dataset to database
    if import_dataset_to_db(args.db_path, args.limit_ratings):
        logger.info("Database rebuild completed successfully")
        return 0
    else:
        logger.error("Database rebuild failed")
        return 1

def verify_db(args):
    """Handle the verify-db command."""
    # Quick setup for logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    logger.info(f"Verifying database at: {args.db_path}")
    
    # Verify database contents
    results = verify_database(args.db_path)
    
    # Print summary
    logger.info("\nDatabase Summary:")
    for table, count in results.items():
        logger.info(f"- {table}: {count} records")
    
    return 0

def main():
    """Main application entry point."""
    args = parse_arguments()
    
    if args.rebuild_db:
        return rebuild_db(args)
    elif args.verify_db:
        return verify_db(args)
    
    # Normal application startup (GUI mode)
    config = load_config()
    setup_logging(config)
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Starting Application in GUI mode...")
        
        # Configure High DPI settings
        if config["display"]["enable_high_dpi"]:
            QApplication.setHighDpiScaleFactorRoundingPolicy(
                QGuiApplication.highDpiScaleFactorRoundingPolicy()
            )
        
        app = QApplication(sys.argv)
        app.setApplicationName(config["application"]["name"])
        app.setApplicationVersion(config["application"]["version"])
        
        # Initialize AnalyticsEngine instance
        analytics = AnalyticsEngine(config)
        setup_signal_handling(app, analytics, logger)
        setup_exception_hook(logger)
        
        return app.exec()
        
    except Exception as e:
        logger.error(f"Application failed to start: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())
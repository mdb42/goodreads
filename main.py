#!/usr/bin/env python3
"""
Main application entry point for the Goodreads Analytics Tool.

This module initializes the application, handles command-line arguments,
and manages startup of various components (GUI, database rebuild, etc.)
based on user-provided options.
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
    """
    Load application configuration from 'config.json', or create it from a default template.

    Returns:
        dict: Configuration dictionary loaded from file or generated from default.
    """
    config_path = Path("config.json")
    try:
        if config_path.exists():
            with open(config_path, 'r') as f:
                return json.load(f)
        else:
            # Use default config from package resources and create a new config file.
            default_config_str = resources.read_text("resources", "default_config.json")
            default_config = json.loads(default_config_str)
            with open(config_path, 'w') as f:
                json.dump(default_config, indent=4, fp=f)
            return default_config
    except (IOError, json.JSONDecodeError) as e:
        print(f"Error loading configuration: {e}")
        print("Using minimal default configuration")
        return {
            "application": {"name": "Goodreads Analytics", "version": "0.1.0"},
            "logging": {"level": "INFO", "file_path": "logs/app.log"},
            "display": {"enable_high_dpi": True},
            "data": {"database": {"path": "data/analytics.db"}}
        }

def setup_logging(config):
    """
    Configure application-wide logging based on settings in the configuration.

    Args:
        config (dict): Application configuration containing logging settings.
    """
    log_path = Path(config["logging"]["file_path"])
    
    try:
        log_path.parent.mkdir(exist_ok=True)
        
        logging.basicConfig(
            level=getattr(logging, config["logging"]["level"], logging.INFO),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_path),
                logging.StreamHandler(sys.stdout)
            ]
        )
    except (IOError, PermissionError) as e:
        # Fall back to console-only logging if file logging fails
        print(f"Warning: Could not set up file logging: {e}")
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler(sys.stdout)]
        )

def setup_signal_handling(app, analytics, logger):
    """
    Configure graceful shutdown on SIGINT and SIGTERM signals.

    Args:
        app (QApplication): The Qt application instance.
        analytics (AnalyticsEngine): Instance of the analytics engine.
        logger (logging.Logger): Logger to record shutdown events.
    """
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
        analytics.close()
        app.quit()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

def setup_exception_hook(logger):
    """
    Configure a global exception hook to log all uncaught exceptions.

    Args:
        logger (logging.Logger): Logger to record exception details.
    """
    def exception_hook(exctype, value, traceback):
        logger.critical("Uncaught exception:", exc_info=(exctype, value, traceback))
        sys.__excepthook__(exctype, value, traceback)  # Invoke default handler.
    
    sys.excepthook = exception_hook

def parse_arguments():
    """
    Parse command-line arguments for the application.

    Returns:
        argparse.Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description="Goodreads Data Analysis Tool",
        epilog="Example: python main.py --rebuild-db"
    )
    
    # Mutually exclusive group for database commands to avoid conflicting options.
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--rebuild-db", 
        action="store_true",
        help="Rebuild the database schema (doesn't load data)"
    )
    group.add_argument(
        "--verify-db",
        action="store_true",
        help="Verify the database contents"
    )
    group.add_argument(
        "--load-data",
        action="store_true",
        help="Load dataset into the database (doesn't rebuild schema)"
    )
    
    parser.add_argument(
        "--force-download", 
        action="store_true",
        help="Force dataset download even if it exists locally"
    )
    
    parser.add_argument(
        "--limit-ratings", 
        type=int,
        help="Limit the number of ratings to import (useful for testing)"
    )
    
    parser.add_argument(
        "--db-path",
        default="data/analytics.db",
        help="Path to the database file (default: data/analytics.db)"
    )
    
    return parser.parse_args()

def rebuild_db(args):
    """
    Handle the 'rebuild-db' command to download the dataset and import it into the database.

    Args:
        args (argparse.Namespace): Command-line arguments with options.

    Returns:
        int: 0 on success, 1 on failure.
    """
    # Temporary logging setup for the rebuild command.
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    logger.info("Rebuilding database with Goodreads dataset")
    
    # Fix: Avoid duplicate calls by storing the result of download_dataset()
    dataset_downloaded = download_dataset()
    if args.force_download and not dataset_downloaded:
        dataset_downloaded = download_dataset()
        
    if not dataset_downloaded:
        logger.error("Failed to download dataset")
        return 1
    
    # Import dataset to the database.
    if import_dataset_to_db(args.db_path, args.limit_ratings):
        logger.info("Database rebuild completed successfully")
        return 0
    else:
        logger.error("Database rebuild failed")
        return 1

def verify_db(args):
    """
    Handle the 'verify-db' command to verify the database contents.

    Args:
        args (argparse.Namespace): Command-line arguments with options.

    Returns:
        int: Always returns 0.
    """
    # Temporary logging setup for the verification command.
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    logger.info(f"Verifying database at: {args.db_path}")
    
    # Verify the database contents.
    results = verify_database(args.db_path)
    
    # Print summary of the database.
    logger.info("\nDatabase Summary:")
    for table, count in results.items():
        logger.info(f"- {table}: {count} records")
    
    return 0

def load_data(args):
    """
    Handle the 'load-data' command to import data into the database.

    Args:
        args (argparse.Namespace): Command-line arguments with options.

    Returns:
        int: 0 on success, 1 on failure.
    """
    # Temporary logging setup
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    logger.info("Loading data into database")
    
    # Force download if requested
    if args.force_download:
        from app.src.dataset_loader import download_dataset
        if not download_dataset():
            logger.error("Failed to download dataset")
            return 1
    
    # Import dataset to the database
    from app.src.dataset_loader import import_dataset_to_db
    if import_dataset_to_db(args.db_path, args.limit_ratings):
        logger.info("Data import completed successfully")
        return 0
    else:
        logger.error("Data import failed")
        return 1

def main():
    """
    Main function to start the application.

    Handles command-line arguments to decide whether to rebuild or verify the database,
    or to launch the GUI for analytics. Sets up configuration, logging, signal handling,
    and a global exception hook.

    Returns:
        int: Exit code of the application.
    """
    args = parse_arguments()
    
    # Process command-line commands for database operations.
    if args.rebuild_db:
        from app.src.analytics_db import AnalyticsDB
        # Just rebuild the database schema without loading data
        db = AnalyticsDB(args.db_path)
        db.close()
        return 0
    elif args.verify_db:
        return verify_db(args)
    elif args.load_data:
        # Only import data without rebuilding the schema
        return load_data(args)
    
    # Normal application startup in GUI mode.
    config = load_config()
    setup_logging(config)
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Starting Application in GUI mode...")
        
        # Configure High DPI settings if enabled in the configuration.
        if config["display"].get("enable_high_dpi", False):
            QApplication.setHighDpiScaleFactorRoundingPolicy(
                QGuiApplication.highDpiScaleFactorRoundingPolicy()
            )
        
        app = QApplication(sys.argv)
        app.setApplicationName(config["application"].get("name", "Goodreads Analytics Tool"))
        app.setApplicationVersion(config["application"].get("version", "0.0.1"))
        
        # Initialize the analytics engine with the loaded configuration.
        analytics = AnalyticsEngine(config)
        
        # Setup graceful shutdown signal handling.
        setup_signal_handling(app, analytics, logger)
        
        # Setup global exception hook to log any unhandled exceptions.
        setup_exception_hook(logger)
        
        # Start the Qt event loop.
        return app.exec()
        
    except Exception as e:
        logger.error(f"Application failed to start: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())
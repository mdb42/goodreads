# src/utils/bootstrap.py
"""
CSC790 Information Retrieval - Final Project
Goodreads Sentiment Analysis and Information Retrieval System

Module: bootstrap.py

This module provides bootstrap functionality for the Goodreads IR system,
handling configuration management, directory initialization, and dataset 
verification. It ensures all required resources are available before the
system starts.

Authors:
    Matthew D. Branson (branson773@live.missouristate.edu)
    James R. Brown (brown926@live.missouristate.edu)

Missouri State University
Department of Computer Science
May 1, 2025
"""

import os
import json
import urllib.request
from pathlib import Path
from typing import Dict, Optional, Any

# Define a type alias for the logger to avoid circular imports
# This allows us to type-hint without importing the actual Logger class
LoggerType = Any

# Default configuration with dataset URLs, paths, and system settings
DEFAULT_CONFIG = {
  "available_datasets": {
    "goodreads_120k": {
      "zip_url": "https://www.dropbox.com/scl/fi/uaxtsmcafw7bfy3y4bajv/goodreads_120k.zip?rlkey=ic8raj4d5lgsatxxf30yffoi1&st=8d0aivye&dl=1",
      "metadata_url": "https://www.dropbox.com/scl/fi/looxnctya8lrqqsfx75vl/metadata_120k.csv?rlkey=hg4p8bljw98m3uxh56dko09t4&st=cglufpb7&dl=1",
      "index_url": "https://www.dropbox.com/scl/fi/9wbg10bcssu8j4aknzcer/goodreads_120k.pkl?rlkey=k0tobojq0jgramk3u8db4x387&st=kppc7wpt&dl=1",
      "local_zip": "datasets/goodreads_120k.zip",
      "local_metadata": "datasets/metadata_120k.csv",
      "local_index": "indexes/goodreads_120k.pkl",
      "models_dir": "models/goodreads_120k",
      "clusters_dir": "clusters/goodreads_120k"
    },
    "goodreads_full": {
      "zip_url": "https://www.dropbox.com/scl/fi/8hammbdkx9prxqkr5b6vp/goodreads_full.zip?rlkey=ccdet50xaxyo4g5t3vs72bcep&st=oial1zbt&dl=1",
      "metadata_url": "https://www.dropbox.com/scl/fi/mk5zlj4gd1b3c9a7qzokg/metadata_full.csv?rlkey=pi44ppfbudfacyb29v6utfpac&st=f16ca3j9&dl=1",
      "index_url": "https://www.dropbox.com/scl/fi/q5e0j26kcon58y94xf805/goodreads_full.pkl?rlkey=qy7za8mdbb0d10tg4wxps0uc5&st=q0nfzxgd&dl=1",
      "local_zip": "datasets/goodreads_full.zip",
      "local_metadata": "datasets/metadata_full.csv",
      "local_index": "indexes/goodreads_full.pkl",
      "models_dir": "models/goodreads_full",
      "clusters_dir": "clusters/goodreads_full"
    }
  },
  "selected_dataset": "goodreads_120k",
  "use_existing_index": True,
  "show_index_stats": False,
  "phases": {
    "search": {
      "enabled": True,
      "interactive": True,
      "top_k": 10
    },
    "classify": {
      "enabled": True,
      "use_existing_model": True,
      "interactive": False,
      "k_folds": 5
    },
    "cluster": {
      "enabled": True,
      "recluster": True,
      "num_clusters": 5,
      "visualize": True
    },
    "crossdomain": {
      "enabled": False
    }
  }
}


def load_config(config_file: str = 'config.json', logger: Optional[LoggerType] = None) -> Dict:
    """
    Load configuration from JSON file or create default if not found.
    
    This function attempts to load the configuration from the specified file.
    If the file doesn't exist or cannot be parsed, it creates a default
    configuration file.
    
    Args:
        config_file: Path to the configuration file
        logger: Logger instance for status messages
        
    Returns:
        Dict containing merged configuration (default + loaded values)
    """
    if logger is None:
        from src.utils.logger import get_logger
        logger = get_logger(name="bootstrap")

    if not os.path.exists(config_file):
        logger.warning(f"[!] Config file not found: {config_file}. Creating default...")
        return _create_default_config(config_file, logger)

    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        logger.info(f"[+] Loaded configuration from {config_file}")
        return {**DEFAULT_CONFIG, **config}  # Merge with defaults, prioritizing loaded values
    except Exception as e:
        logger.error(f"[X] Failed to load config: {e}")
        return _create_default_config(config_file, logger)


def save_config(config: Dict, config_file: str = 'config.json', logger: Optional[LoggerType] = None) -> None:
    """
    Save configuration dictionary to JSON file.
    
    Args:
        config: Configuration dictionary to save
        config_file: Path to save the configuration file
        logger: Logger instance for status messages
    """
    if logger is None:
        from src.utils.logger import get_logger
        logger = get_logger(name="bootstrap")

    try:
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2)
        logger.info(f"[+] Configuration saved to {config_file}")
    except Exception as e:
        logger.error(f"[X] Error saving config: {e}")


def _create_default_config(config_file: str = 'config.json', logger: Optional[LoggerType] = None) -> Dict:
    """
    Create and save default configuration file.
    
    Args:
        config_file: Path to save the default configuration
        logger: Logger instance for status messages
        
    Returns:
        Dict containing default configuration
    """
    if logger is None:
        from src.utils.logger import get_logger
        logger = get_logger(name="bootstrap")

    try:
        save_config(DEFAULT_CONFIG, config_file, logger=logger)
        logger.info(f"[+] Default configuration created at {config_file}")
    except Exception as e:
        logger.error(f"[X] Could not create default config: {e}")
    return DEFAULT_CONFIG


def check_multiprocessing(logger: Optional[LoggerType] = None) -> bool:
    """
    Check if multiprocessing is supported on the current system.
    
    Attempts to create a multiprocessing pool to verify functionality.
    
    Args:
        logger: Logger instance for status messages
        
    Returns:
        Boolean indicating whether multiprocessing is supported
    """
    if logger is None:
        from src.utils.logger import get_logger
        logger = get_logger(name="bootstrap")

    try:
        import multiprocessing
        with multiprocessing.Pool(1) as _:
            pass
        logger.info("[+] Multiprocessing supported.")
        return True
    except (ImportError, OSError, ValueError):
        logger.warning("[!] Multiprocessing not available.")
        return False


def ensure_directories_exist(logger: Optional[LoggerType] = None) -> None:
    """
    Create all required directories for the system if they don't exist.
    
    Creates directories for datasets, indexes, models, logs, and outputs.
    
    Args:
        logger: Logger instance for status messages
    """
    if logger is None:
        from src.utils.logger import get_logger
        logger = get_logger(name="bootstrap")

    for folder in ["datasets", "indexes", "models", "clusters", "logs", "outputs"]:
        Path(folder).mkdir(parents=True, exist_ok=True)
    logger.info("[+] Ensured all required directories exist.")


def verify_local_file(path: str, description: str = "File", logger: Optional[LoggerType] = None) -> None:
    """
    Verify that a required file exists.
    
    Args:
        path: Path to the file to verify
        description: Description of the file for logging
        logger: Logger instance for status messages
        
    Raises:
        FileNotFoundError: If the file doesn't exist
    """
    if logger is None:
        from src.utils.logger import get_logger
        logger = get_logger(name="bootstrap")

    if not os.path.exists(path):
        logger.error(f"[X] {description} missing: {path}")
        raise FileNotFoundError(f"{description} missing: {path}")
    else:
        logger.info(f"[+] {description} found: {path}")


def download_file(url: str, destination: str, logger: Optional[LoggerType] = None) -> None:
    """
    Download a file from a URL to a local destination with progress tracking.
    
    Args:
        url: Source URL to download from
        destination: Local path to save the file
        logger: Logger instance for status messages
        
    Raises:
        Exception: If download fails
    """
    if logger is None:
        from src.utils.logger import get_logger
        logger = get_logger(name="bootstrap")

    logger.info(f"[+] Starting download: {url}")
    try:
        with urllib.request.urlopen(url) as response:
            total_size_header = response.getheader('Content-Length')
            total_size = int(total_size_header.strip()) if total_size_header else None

            chunk_size = 8192
            bytes_downloaded = 0
            with open(destination, 'wb') as f:
                while chunk := response.read(chunk_size):
                    f.write(chunk)
                    bytes_downloaded += len(chunk)
                    if total_size:
                        progress = (bytes_downloaded / total_size) * 100
                        print(f"\rProgress: {progress:.2f}%", end='')
            print()  # Print newline after progress bar
            logger.info(f"[+] Download completed: {destination}")
    except Exception as e:
        logger.error(f"[X] Download failed: {e}")
        raise


def get_selected_dataset_info(config: Dict, logger: Optional[LoggerType] = None) -> Dict:
    """
    Get information about the currently selected dataset.
    
    Args:
        config: Configuration dictionary
        logger: Logger instance for status messages
        
    Returns:
        Dict containing dataset information
        
    Raises:
        ValueError: If the selected dataset is not defined in configuration
    """
    if logger is None:
        from src.utils.logger import get_logger
        logger = get_logger(name="bootstrap")

    dataset_name = config.get("selected_dataset")
    datasets = config.get("available_datasets", {})
    if dataset_name not in datasets:
        logger.error(f"[X] Dataset '{dataset_name}' not defined in configuration.")
        raise ValueError(f"Dataset '{dataset_name}' not defined in config.")
    logger.info(f"[+] Selected dataset: {dataset_name}")
    return datasets[dataset_name]


def verify_dataset_and_index(config: Dict, logger: Optional[LoggerType] = None) -> None:
    """
    Verify that all necessary dataset files exist, downloading if missing.
    
    Checks and downloads dataset zip, index file, and metadata file if needed.
    
    Args:
        config: Configuration dictionary
        logger: Logger instance for status messages
    """
    if logger is None:
        from src.utils.logger import get_logger
        logger = get_logger(name="bootstrap")

    dataset_info = get_selected_dataset_info(config, logger=logger)

    zip_path = dataset_info["local_zip"]
    index_path = dataset_info["local_index"]
    metadata_path = dataset_info["local_metadata"]

    if not os.path.exists(zip_path):
        logger.warning("[!] Dataset zip missing. Downloading...")
        download_file(dataset_info["zip_url"], zip_path, logger=logger)

    if not os.path.exists(index_path):
        logger.warning("[!] Index file missing. Downloading...")
        download_file(dataset_info["index_url"], index_path, logger=logger)

    if not os.path.exists(metadata_path):
        logger.warning("[!] Metadata file missing. Downloading...")
        download_file(dataset_info["metadata_url"], metadata_path, logger=logger)

    logger.info("[+] All necessary dataset and index files verified.")


def download_if_missing(local_path: str, url: str, logger: Optional[LoggerType] = None) -> None:
    """
    Download a file if it doesn't exist locally.
    
    Args:
        local_path: Local path to check and save to
        url: URL to download from if file is missing
        logger: Logger instance for status messages
    """
    if logger is None:
        from src.utils.logger import get_logger
        logger = get_logger(name="bootstrap")

    if not os.path.exists(local_path):
        logger.info(f"[+] File missing. Initiating download: {local_path}")
        download_file(url, local_path, logger=logger)
    else:
        logger.info(f"[+] File already present: {local_path}")
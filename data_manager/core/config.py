# app/core/config.py
import json
from pathlib import Path
import logging

# Default configuration
DEFAULT_CONFIG = {
    "application": {
        "name": "Goodreads Dataset Manager", 
        "version": "0.1.0"
    },
    "logging": {
        "level": "INFO", 
        "file_path": "logs/app.log"
    },
    "display": {
        "enable_high_dpi": True, 
        "theme": "dark"
    },
    "data": {
        "database": "data/goodreads.db",
        "books": "data/goodreads_books.json.gz",
        "interactions": "data/goodreads_interactions.json.gz",
        "reviews": "data/goodreads_reviews_spoiler.json.gz"
    }        
}

def load_config(config_path="data_manager/config.json"):
    """
    Load application configuration from a JSON file.
    If file doesn't exist, create it with default configuration.
    
    Args:
        config_path (str): Path to the configuration file
        
    Returns:
        dict: Configuration dictionary
    """
    path = Path(config_path)
    logger = logging.getLogger(__name__)
    
    try:
        if path.exists():
            with open(path, 'r') as f:
                config = json.load(f)
                logger.info(f"Configuration loaded from {path}")
                return config
        else:
            logger.info(f"Configuration file {path} not found, creating with defaults")
            config = DEFAULT_CONFIG.copy()
            
            # Ensure the parent directory exists
            path.parent.mkdir(parents=True, exist_ok=True)
            
            # Save the default configuration
            with open(path, 'w') as f:
                json.dump(config, f, indent=4)
                
            logger.info(f"Default configuration saved to {path}")
            return config
    except Exception as e:
        logger.error(f"Error handling configuration: {e}")
        return DEFAULT_CONFIG.copy()

def save_config(config, config_path="config.json"):
    """
    Save configuration to a JSON file.
    
    Args:
        config (dict): Configuration dictionary
        config_path (str): Path to save the configuration file
        
    Returns:
        bool: True if successful, False otherwise
    """
    path = Path(config_path)
    logger = logging.getLogger(__name__)
    
    try:
        # Create parent directories if they don't exist
        path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(path, 'w') as f:
            json.dump(config, f, indent=4)
            
        logger.info(f"Configuration saved to {path}")
        return True
    except Exception as e:
        logger.error(f"Error saving configuration: {e}")
        return False

def get_default_config():
    """
    Get a copy of the default configuration.
    
    Returns:
        dict: Default configuration dictionary
    """
    return DEFAULT_CONFIG.copy()

def update_config_value(config, key_path, value):
    """
    Update a value in the configuration using a dot-separated path.
    
    Args:
        config (dict): Configuration dictionary
        key_path (str): Dot-separated path to the config value (e.g. "display.theme")
        value: New value to set
        
    Returns:
        dict: Updated configuration dictionary
    """
    keys = key_path.split('.')
    current = config
    
    # Navigate to the nested dictionary
    for key in keys[:-1]:
        if key not in current:
            current[key] = {}
        current = current[key]
    
    # Set the value
    current[keys[-1]] = value
    return config
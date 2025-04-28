# src/utils/bootstrap.py

import os
import json
import urllib.request
from pathlib import Path
from typing import Dict

DEFAULT_CONFIG = {
    "available_datasets": {
        "goodreads_120k": {
            "zip_url": "https://www.dropbox.com/scl/fi/uaxtsmcafw7bfy3y4bajv/goodreads_120k.zip?rlkey=ic8raj4d5lgsatxxf30yffoi1&st=nvxtcwfe&dl=0",
            "metadata_url": "https://www.dropbox.com/scl/fi/looxnctya8lrqqsfx75vl/metadata_120k.csv?rlkey=hg4p8bljw98m3uxh56dko09t4&st=373y6pg4&dl=0",
            "index_url": "https://www.dropbox.com/scl/fi/owsw1yih8hqppk5vghb5z/goodreads_120k.pkl?rlkey=ufob3zojiisv7f2nbrqyn1h5g&st=7o36n73b&dl=1",
            "local_zip": "datasets/goodreads_120k.zip",
            "local_index": "indexes/goodreads_120k.pkl",
            "local_metadata": "datasets/metadata_120k.csv"
        },
        "goodreads_full": {
            "zip_url": "https://www.dropbox.com/scl/fi/8hammbdkx9prxqkr5b6vp/goodreads_full.zip?rlkey=ccdet50xaxyo4g5t3vs72bcep&st=iufh565s&dl=0",
            "metadata_url": "https://www.dropbox.com/scl/fi/mk5zlj4gd1b3c9a7qzokg/metadata_full.csv?rlkey=pi44ppfbudfacyb29v6utfpac&st=szb6tzmz&dl=0",
            "index_url": "https://www.dropbox.com/scl/fi/54gm1zvsjukwn0p4mn8md/goodreads_full.pkl?rlkey=42x84rpgsto2sinauxmez8sws&st=qzdq7lpo&dl=1",
            "local_zip": "datasets/goodreads_full.zip",
            "local_index": "indexes/goodreads_full.pkl",
            "local_metadata": "datasets/metadata_full.csv"
        }
    },
    "selected_dataset": "goodreads_120k",
    "task": "both",
    "use_existing_index": True,
    "output_dir": "output"
}



def load_config(config_file='config.json', logger=None) -> Dict:
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
        return {**DEFAULT_CONFIG, **config}
    except Exception as e:
        logger.error(f"[X] Failed to load config: {e}")
        return _create_default_config(config_file, logger)

def save_config(config: Dict, config_file='config.json', logger=None):
    if logger is None:
        from src.utils.logger import get_logger
        logger = get_logger(name="bootstrap")

    try:
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2)
        logger.info(f"[+] Configuration saved to {config_file}")
    except Exception as e:
        logger.error(f"[X] Error saving config: {e}")

def _create_default_config(config_file='config.json', logger=None) -> Dict:
    if logger is None:
        from src.utils.logger import get_logger
        logger = get_logger(name="bootstrap")

    try:
        save_config(DEFAULT_CONFIG, config_file, logger=logger)
        logger.info(f"[+] Default configuration created at {config_file}")
    except Exception as e:
        logger.error(f"[X] Could not create default config: {e}")
    return DEFAULT_CONFIG

def check_multiprocessing(logger=None) -> bool:
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

def ensure_directories_exist(logger=None):
    if logger is None:
        from src.utils.logger import get_logger
        logger = get_logger(name="bootstrap")

    for folder in ["datasets", "indexes", "models", "logs", "outputs"]:
        Path(folder).mkdir(parents=True, exist_ok=True)
    logger.info("[+] Ensured all required directories exist.")

def verify_local_file(path, description="File", logger=None):
    if logger is None:
        from src.utils.logger import get_logger
        logger = get_logger(name="bootstrap")

    if not os.path.exists(path):
        logger.error(f"[X] {description} missing: {path}")
        raise FileNotFoundError(f"{description} missing: {path}")
    else:
        logger.info(f"[+] {description} found: {path}")

def download_file(url, destination, logger=None):
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
            print()
            logger.info(f"[+] Download completed: {destination}")
    except Exception as e:
        logger.error(f"[X] Download failed: {e}")
        raise

def get_selected_dataset_info(config, logger=None):
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

def verify_dataset_and_index(config, logger=None):
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

def download_if_missing(local_path, url, logger=None):
    if logger is None:
        from src.utils.logger import get_logger
        logger = get_logger(name="bootstrap")

    if not os.path.exists(local_path):
        logger.info(f"[+] File missing. Initiating download: {local_path}")
        download_file(url, local_path, logger=logger)
    else:
        logger.info(f"[+] File already present: {local_path}")

#!/usr/bin/env python3
"""
Goodreads Dataset Loader

This script checks for the existence of the Goodreads dataset in a data folder
and attempts to download it if missing. It supports the Goodbooks-10k dataset,
which is one of the most commonly used Goodreads datasets.

Prerequisites:
- Python 3.6+
- Required packages: pandas, requests, tqdm, kaggle

Usage:
1. Make sure you have a Kaggle account and API credentials set up:
   - Go to https://www.kaggle.com/
   - Create an account if you don't have one
   - Go to your account settings > API > Create New API Token
   - This will download a kaggle.json file
   - Place this file in ~/.kaggle/ (create the directory if it doesn't exist)
   - Run: chmod 600 ~/.kaggle/kaggle.json (on Linux/Mac to set permissions)

2. Run this script: python goodreads_loader.py

The script will create a 'data' directory in the current working directory
and download the Goodreads dataset there if it doesn't exist.
"""

import os
import sys
import json
import shutil
import zipfile
import pandas as pd
from pathlib import Path

try:
    import kaggle
    from tqdm import tqdm
except ImportError:
    print("Installing required packages...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "kaggle", "tqdm"])
    import kaggle
    from tqdm import tqdm

# Define dataset-specific constants
DATASETS = {
    "goodbooks-10k": {
        "kaggle_dataset": "zygmunt/goodbooks-10k",
        "description": "10,000 books with 6 million ratings",
        "files": ["books.csv", "ratings.csv", "book_tags.csv", "tags.csv", "to_read.csv"],
        "min_size_mb": 100  # Minimum expected size in MB
    },
    "goodreads-books": {
        "kaggle_dataset": "jealousleopard/goodreadsbooks",
        "description": "50,000+ books with metadata",
        "files": ["books.csv"],
        "min_size_mb": 20
    },
    "goodreads-reviews": {
        "kaggle_dataset": "kevinmsammut/goodreads-user-reviews-120k",
        "description": "120,000 user reviews",
        "files": ["user_reviews.csv"],
        "min_size_mb": 30
    }
}

DEFAULT_DATASET = "goodbooks-10k"
DATA_DIR = Path("data")


def check_kaggle_credentials():
    """Verify that Kaggle credentials are properly set up."""
    # Check for kaggle.json file
    kaggle_dir = Path.home() / ".kaggle"
    kaggle_json = kaggle_dir / "kaggle.json"
    
    if not kaggle_json.exists():
        print("Kaggle API credentials not found.")
        print("Please set up your Kaggle credentials:")
        print("1. Go to https://www.kaggle.com/ and log in")
        print("2. Go to Account > API > Create New API Token")
        print("3. Move the downloaded kaggle.json to ~/.kaggle/")
        print("4. On Linux/Mac, run: chmod 600 ~/.kaggle/kaggle.json")
        return False
    
    # Verify the permissions (on Unix systems)
    if os.name != 'nt':  # Skip this check on Windows
        permissions = oct(kaggle_json.stat().st_mode)[-3:]
        if permissions != '600':
            print(f"Warning: Your kaggle.json file has permissions {permissions}.")
            print("For security, it's recommended to run: chmod 600 ~/.kaggle/kaggle.json")
    
    # Try to list datasets to verify credentials work
    try:
        # Just testing API connectivity
        kaggle.api.authenticate()
        return True
    except Exception as e:
        print(f"Error authenticating with Kaggle API: {e}")
        return False


def dataset_exists(dataset_name):
    """Check if the dataset already exists locally."""
    dataset_info = DATASETS[dataset_name]
    
    # Check if data directory exists
    if not DATA_DIR.exists():
        return False
    
    # Check if expected files exist and have minimum size
    for file in dataset_info["files"]:
        file_path = DATA_DIR / file
        if not file_path.exists():
            return False
        
        # Check if the file is not empty (sometimes downloads can fail)
        if file_path.stat().st_size < 1000:  # Check if at least 1KB
            return False
    
    # Check if the total size of the files is reasonable
    total_size_mb = sum(file.stat().st_size for file in DATA_DIR.glob("*") if file.is_file()) / (1024 * 1024)
    if total_size_mb < dataset_info["min_size_mb"]:
        print(f"Dataset files exist but total size ({total_size_mb:.2f} MB) is less than expected ({dataset_info['min_size_mb']} MB)")
        return False
    
    return True


def download_dataset(dataset_name):
    """Download the specified dataset from Kaggle."""
    dataset_info = DATASETS[dataset_name]
    
    print(f"Downloading {dataset_name} dataset: {dataset_info['description']}...")
    
    # Create data directory if it doesn't exist
    DATA_DIR.mkdir(exist_ok=True)
    
    try:
        # Download the dataset
        kaggle.api.dataset_download_files(
            dataset_info["kaggle_dataset"], 
            path=DATA_DIR, 
            unzip=True
        )
        print(f"Successfully downloaded {dataset_name} dataset to {DATA_DIR}")
        return True
    except Exception as e:
        print(f"Error downloading dataset: {e}")
        return False


def preview_dataset(dataset_name):
    """Preview the dataset to confirm it loaded correctly."""
    dataset_info = DATASETS[dataset_name]
    
    print(f"\nPreviewing {dataset_name} dataset:")
    for file in dataset_info["files"]:
        file_path = DATA_DIR / file
        try:
            df = pd.read_csv(file_path)
            print(f"\n{file}:")
            print(f"- Shape: {df.shape[0]} rows, {df.shape[1]} columns")
            print("- Columns:", ", ".join(df.columns.tolist()))
            print("- Sample data:")
            print(df.head(3))
        except Exception as e:
            print(f"Error previewing {file}: {e}")


def main():
    """Main function to download and validate the Goodreads dataset."""
    # Default dataset to download
    dataset_name = DEFAULT_DATASET
    
    # Allow command-line selection of dataset
    if len(sys.argv) > 1 and sys.argv[1] in DATASETS:
        dataset_name = sys.argv[1]
    
    print(f"Goodreads Dataset Loader - Target: {dataset_name}")
    
    # Check if the dataset already exists
    if dataset_exists(dataset_name):
        print(f"{dataset_name} dataset already exists in {DATA_DIR}")
    else:
        # Verify Kaggle credentials before attempting download
        if check_kaggle_credentials():
            success = download_dataset(dataset_name)
            if not success:
                print("Download failed. Please check your internet connection and Kaggle API credentials.")
                return False
        else:
            print("Kaggle credentials check failed. Cannot download dataset.")
            return False
    
    # Preview the dataset
    preview_dataset(dataset_name)
    
    print("\nDataset is ready for use!")
    return True


if __name__ == "__main__":
    main()
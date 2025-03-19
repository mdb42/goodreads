# app/db/downloader.py
"""
Handles downloading and basic file validation for the UCSD Book Graph dataset.
"""

import logging
import shutil
import urllib.request
import hashlib
from pathlib import Path
from typing import Dict, Optional, Callable

logger = logging.getLogger(__name__)

# New KB sizes -> approximate MB conversion:
#   books:        2,029,883 KB  -> ~1,983 MB
#   reviews:        605,933 KB  -> ~592 MB
#   interactions: 11,191,253 KB -> ~10,928 MB (≈ 10.68 GB)

# Dataset metadata
DATASET_INFO = {
    "name": "ucsd-book-graph",
    "description": "UCSD Book Graph with 15.7M reviews and 1.5M books",
    "files": {
        "books": {
            "filename": "goodreads_books.json.gz",
            "url": "https://datarepo.eng.ucsd.edu/mcauley_group/gdrive/goodreads/goodreads_books.json.gz",
            "size_mb": 1983,  # ~1.94 GB
            "md5": "75f2f23cedf111b926910614506a58b6"
        },
        "reviews": {
            "filename": "goodreads_reviews_spoiler.json.gz",
            "url": "https://datarepo.eng.ucsd.edu/mcauley_group/gdrive/goodreads/goodreads_reviews_spoiler.json.gz",
            "size_mb": 592,   # ~592 MB
            "md5": "b7dafdf4ad25a9eb797f4f39608e5a0e"
        },
        "interactions": {
            "filename": "goodreads_interactions.json.gz",
            "url": "https://datarepo.eng.ucsd.edu/mcauley_group/gdrive/goodreads/goodreads_interactions_dedup.json.gz",
            "size_mb": 10928, # ~10.68 GB
            "md5": "1cd3716e4088ffa9b785f603b398c843"
        }
    }
}


class DownloadProgressTracker:
    """Track download progress and provide callbacks for UI updates."""

    def __init__(self, total_size: int, progress_callback: Optional[Callable] = None):
        self.total_size = total_size
        self.downloaded = 0
        self.callback = progress_callback
        self.last_percent = 0

    def update(self, chunk_size: int):
        """Update progress with newly downloaded `chunk_size` bytes."""
        self.downloaded += chunk_size
        if self.total_size > 0:
            percent = int((self.downloaded / self.total_size) * 100)
            if percent > self.last_percent:
                self.last_percent = percent
                if self.callback:
                    self.callback(percent, self.downloaded, self.total_size)


class FileDownloader:
    """
    Handles dataset file operations including downloading, validation, and existence checks.
    """

    def __init__(self, config):
        self.config = config

    def verify_file_integrity(self, file_path: Path, expected_md5: str) -> bool:
        """
        Verify a file's integrity using MD5 hash.
        """
        if not file_path.exists():
            return False
        try:
            logger.info(f"Verifying integrity of {file_path.name}...")
            md5_hash = hashlib.md5()
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    md5_hash.update(chunk)
            file_md5 = md5_hash.hexdigest()
            if file_md5 != expected_md5:
                logger.warning(f"MD5 mismatch for {file_path.name}: Expected {expected_md5}, got {file_md5}")
                return False
            logger.info(f"File integrity verified: {file_path.name}")
            return True
        except Exception as e:
            logger.error(f"Error verifying file integrity: {e}")
            return False

    def check_files_exist(self) -> Dict[str, bool]:
        """
        Check if all required dataset files exist and have valid sizes.
        """
        results = {}
        for file_key, file_info in DATASET_INFO["files"].items():
            file_path = Path(self.config["data"][file_key])
            if file_path.exists():
                size_mb = file_path.stat().st_size / (1024 * 1024)
                # Allow about 10% smaller than the “expected” size
                min_size = file_info["size_mb"] * 0.9
                if size_mb < min_size:
                    logger.warning(f"File {file_path.name} is too small: {size_mb:.1f}MB < {min_size:.1f}MB")
                    results[file_key] = False
                else:
                    logger.info(f"File {file_path.name} exists and size looks good ({size_mb:.1f}MB)")
                    results[file_key] = True
            else:
                logger.info(f"File {file_path.name} not found")
                results[file_key] = False
        return results

    def download_file(self, file_key: str, progress_callback: Optional[Callable] = None) -> bool:
        """
        Download a specific dataset file with progress tracking.
        """
        file_info = DATASET_INFO["files"][file_key]
        destination = Path(self.config["data"][file_key])
        url = file_info["url"]

        logger.info(f"Downloading {file_key} dataset from {url}")
        destination.parent.mkdir(parents=True, exist_ok=True)

        temp_file = destination.with_suffix('.download')

        try:
            opener = urllib.request.build_opener()
            opener.addheaders = [('User-Agent', 'Mozilla/5.0')]
            urllib.request.install_opener(opener)

            response = urllib.request.urlopen(url)
            total_size = int(response.info().get('Content-Length', -1))

            progress = DownloadProgressTracker(total_size, progress_callback)

            with open(temp_file, 'wb') as f:
                while True:
                    chunk = response.read(8192)
                    if not chunk:
                        break
                    f.write(chunk)
                    progress.update(len(chunk))

            shutil.move(str(temp_file), str(destination))
            logger.info(f"Successfully downloaded {file_key} dataset")
            return True
        except Exception as e:
            logger.error(f"Error downloading {file_key} dataset: {e}")
            if temp_file.exists():
                temp_file.unlink()
            return False

    def download_all_missing(self, progress_callback: Optional[Callable] = None) -> bool:
        """
        Download all missing dataset files.
        """
        existing_files = self.check_files_exist()
        missing_files = [k for k, exists in existing_files.items() if not exists]

        if not missing_files:
            logger.info("All dataset files already exist")
            return True

        logger.info(f"Will download {len(missing_files)} missing files: {', '.join(missing_files)}")

        overall_success = True
        for i, file_key in enumerate(missing_files):
            file_progress_callback = None
            if progress_callback:
                file_progress_callback = lambda percent, bytes_dl, total: progress_callback(
                    file_key, percent, bytes_dl, total, i, len(missing_files)
                )

            success = self.download_file(file_key, file_progress_callback)
            if not success:
                overall_success = False

        return overall_success

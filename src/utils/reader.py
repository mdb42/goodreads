# src/utils/reader.py
"""
CSC790 Information Retrieval - Final Project
Goodreads Sentiment Analysis and Information Retrieval System

Module: reader.py

This module provides functionality for reading documents directly from compressed
zip archives, enabling efficient processing of large document collections without
extraction to disk.

Authors:
    Matthew D. Branson (branson773@live.missouristate.edu)
    James R. Brown (brown926@live.missouristate.edu)

Missouri State University
Department of Computer Science
May 1, 2025
"""

import zipfile
from pathlib import Path
from typing import List

class ZipCorpusReader:
    """
    Utility class for reading document collections directly from zip archives.
    
    This class provides methods to list and read documents from a zip archive
    without extracting them to disk, enabling efficient processing of large
    document collections while minimizing filesystem overhead.
    
    Attributes:
        zip_path (Path): Path to the zip archive containing documents
    """
    
    def __init__(self, zip_path):
        """
        Initialize the zip corpus reader.
        
        Args:
            zip_path (str or Path): Path to the zip archive containing documents
            
        Raises:
            FileNotFoundError: If the zip file doesn't exist
        """
        self.zip_path = Path(zip_path)
        if not self.zip_path.exists():
            raise FileNotFoundError(f"Zip file not found: {zip_path}")

    def list_documents(self) -> List[str]:
        """
        List all text documents in the zip archive.
        
        Returns:
            List[str]: Names of all .txt files in the archive
        """
        with zipfile.ZipFile(self.zip_path, 'r') as zf:
            return [name for name in zf.namelist() if name.endswith('.txt')]

    @staticmethod
    def read_document(zf: zipfile.ZipFile, doc_name: str) -> str:
        """
        Read the content of a document from an open zip file.
        
        Args:
            zf (zipfile.ZipFile): Open zip file object
            doc_name (str): Name of the document in the zip archive
            
        Returns:
            str: Content of the document, decoded as UTF-8 with error handling
            
        Note:
            This method is static to allow it to be used efficiently in parallel
            processing contexts where multiple workers share access to the same
            zip file.
        """
        with zf.open(doc_name) as file:
            content = file.read()
            return content.decode('utf-8', errors='ignore')
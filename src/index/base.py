# src/index/base.py
"""
Base Document Index Interface
Author: Matthew Branson
Date: March 14, 2025

This module defines the abstract base class for all document index implementations.
It establishes the common interface that concrete index implementations must provide.
"""
from abc import ABC, abstractmethod
from typing import List, Tuple, Dict, Any, Optional


class BaseIndex(ABC):
    """
    Abstract base class for document index implementations.
    
    This class defines the common interface for all index implementations.
    It provides the foundation for document processing, term frequency tracking,
    and index persistence.
    
    Attributes:
        documents_dir (str): Directory containing documents to index
        stopwords_file (str): File containing stopwords to remove
        special_chars_file (str): File containing special characters to remove
        profiler (Profiler): Performance monitoring utility for timing operations
        
    Note:
        While this is defined as an abstract base class to allow for different
        implementation approaches, the current system primarily uses StandardIndex
        with ParallelIndex extending it for improved performance on larger collections.
    """
    def __init__(self, documents_dir=None, stopwords_file=None, special_chars_file=None, profiler=None):
        """
        Initialize the base document index.
        
        Args:
            documents_dir (str, optional): Directory containing documents to index
            stopwords_file (str, optional): File containing stopwords to remove
            special_chars_file (str, optional): File containing special characters to remove
            profiler (Profiler, optional): Performance profiler for timing operations
        """
        self.documents_dir = documents_dir
        self.stopwords_file = stopwords_file
        self.special_chars_file = special_chars_file
        self.profiler = profiler
    
    @property
    @abstractmethod
    def doc_count(self) -> int:
        """
        Get the number of documents in the index.
        
        Returns:
            int: Number of documents in the index
        """
        pass
    
    @property
    @abstractmethod
    def vocab_size(self) -> int:
        """
        Get the size of the vocabulary (number of unique terms).
        
        Returns:
            int: Number of unique terms in the index
        """
        pass
    
    @abstractmethod
    def build_index(self):
        """
        Build the index by processing all documents in the documents directory.
        """
        pass
    
    @abstractmethod
    def add_document(self, term_freqs: dict, filename: str = None) -> int:
        """
        Add a single document to the index.
        
        Args:
            term_freqs (dict): Dictionary mapping terms to their frequencies
            filename (str, optional): Name of the document file
            
        Returns:
            int: ID assigned to the document
        """
        pass
    
    @abstractmethod
    def _preprocess_text(self, text: str) -> List[str]:
        """
        Preprocess and tokenize document text.
        
        Args:
            text (str): Raw document text
            
        Returns:
            List[str]: List of preprocessed tokens
        """
        pass
    
    @abstractmethod
    def _process_single_file(self, filepath: str) -> Optional[Tuple[str, Dict[str, int]]]:
        """
        Process a single document file.
        
        Args:
            filepath (str): Path to the document file
            
        Returns:
            Optional[Tuple[str, Dict[str, int]]]: Tuple containing the filename
                                                and term frequency dictionary,
                                                or None if processing failed
        """
        pass
    
    @abstractmethod
    def get_term_freq(self, term: str, doc_id: int) -> int:
        """
        Get the frequency of a term in a specific document.
        
        Args:
            term (str): The term to look up
            doc_id (int): The document ID
            
        Returns:
            int: Frequency of the term in the document (0 if not found)
        """
        pass
    
    @abstractmethod
    def get_doc_freq(self, term: str) -> int:
        """
        Get the document frequency of a term (number of documents containing the term).
        
        Args:
            term (str): The term to look up
            
        Returns:
            int: Number of documents containing the term
        """
        pass
    
    @abstractmethod
    def get_most_frequent_terms(self, n: int = 10) -> List[Tuple[str, int]]:
        """
        Get the n most frequent terms across all documents.
        
        Args:
            n (int): Number of terms to return
            
        Returns:
            List[Tuple[str, int]]: List of (term, frequency) tuples,
                                  sorted by frequency in descending order
        """
        pass
    
    @abstractmethod
    def save(self, filepath: str) -> None:
        """
        Save the index to a file.
        
        Args:
            filepath (str): Path where the index should be saved
        """
        pass
    
    @classmethod
    @abstractmethod
    def load(cls, filepath: str):
        """
        Load an index from a file.
        
        Args:
            filepath (str): Path to the saved index file
            
        Returns:
            BaseIndex: The loaded index instance
        """
        pass
    
    @abstractmethod
    def get_memory_usage(self) -> Dict[str, int]:
        """
        Get memory usage statistics for the index.
        
        Returns:
            Dict[str, int]: Dictionary mapping component names to memory usage in bytes
        """
        pass
    
    @abstractmethod
    def export_json(self, filepath: str = None) -> Optional[str]:
        """
        Export the index to a JSON format.
        
        Args:
            filepath (str, optional): Path where the JSON should be saved
                                     If None, the JSON is returned as a string
            
        Returns:
            Optional[str]: JSON string representation if no filepath is provided
        """
        pass
    
    @abstractmethod
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive statistics about the index.
        
        Returns:
            Dict[str, Any]: Dictionary containing various statistics about the index,
                           such as document count, vocabulary size, average document length, etc.
        """
        pass
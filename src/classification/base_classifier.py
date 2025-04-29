#!/usr/bin/env python3
"""
CSC790 Information Retrieval - Final Project
Goodreads Sentiment Analysis and Information Retrieval System

This module defines the abstract base class for sentiment classification.
It establishes the common interface that concrete classifier implementations must provide.

Authors:
    Matthew D. Branson (branson773@live.missouristate.edu)
    James R. Brown (brown926@live.missouristate.edu)

Missouri State University
Department of Computer Science
May 1, 2025
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional

class BaseClassifier(ABC):
    """
    Abstract base class for sentiment classifier implementations.
    
    This class defines the common interface for all classifier implementations.
    It provides the foundation for training, prediction, and evaluation.
    
    Attributes:
        zip_path: Path to ZIP archive containing review documents
        metadata_path: Path to CSV file with review ratings
        models_dir: Directory to save/load models
        logger: Logger for recording progress and errors
        profiler: Optional performance profiler
    """
    
    def __init__(self, zip_path: str, metadata_path: str, models_dir: str, 
                 logger=None, profiler=None):
        """
        Initialize the base classifier with all necessary resources.
        
        Args:
            zip_path: Path to ZIP archive containing review documents
            metadata_path: Path to CSV file with review ratings
            models_dir: Directory to save/load models
            logger: Logger for recording progress and errors
            profiler: Optional performance profiler
        """
        self.zip_path = zip_path
        self.metadata_path = metadata_path
        self.models_dir = models_dir
        self.logger = logger
        self.profiler = profiler
    
    @abstractmethod
    def train(self, k_folds: int = 5, **kwargs) -> Dict[str, float]:
        """
        Train the classifier with k-fold cross-validation.
        
        Args:
            k_folds: Number of folds for cross-validation
            **kwargs: Additional training parameters
                
        Returns:
            Dict[str, float]: Training metrics
        """
        pass
    
    @abstractmethod
    def save(self) -> bool:
        """
        Save the trained classifier models to the models directory.
        
        Returns:
            bool: Whether the save was successful
        """
        pass
    
    @classmethod
    @abstractmethod
    def load(cls, zip_path: str, metadata_path: str, models_dir: str, 
             logger=None, profiler=None) -> 'BaseClassifier':
        """
        Load trained classifier models from a directory.
        
        Args:
            zip_path: Path to ZIP archive containing review documents
            metadata_path: Path to CSV file with review ratings
            models_dir: Directory containing saved model files
            logger: Logger for recording progress and errors
            profiler: Optional performance profiler
                
        Returns:
            BaseClassifier: Loaded classifier instance
        """
        pass
    
    @abstractmethod
    def evaluate(self) -> Dict[str, float]:
        """
        Evaluate the classifier on test data.
        
        Returns:
            Dict[str, float]: Evaluation metrics
        """
        pass
    
    @abstractmethod
    def predict(self, text: str) -> int:
        """
        Predict rating for a single review text.
        
        Args:
            text: Review text to classify
                
        Returns:
            int: Predicted rating (1-5)
        """
        pass
    
    @abstractmethod
    def predict_batch(self, texts: List[str]) -> List[int]:
        """
        Predict ratings for multiple review texts.
        
        Args:
            texts: List of review texts to classify
                
        Returns:
            List[int]: Predicted ratings (1-5)
        """
        pass
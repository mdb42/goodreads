# src/clustering/base_clusterer.py
"""
CSC790 Information Retrieval - Final Project
Goodreads Sentiment Analysis and Information Retrieval System

This module defines the abstract base class for user clustering.
It establishes the common interface that concrete clustering implementations must provide.

Authors:
    Matthew D. Branson (branson773@live.missouristate.edu)
    James R. Brown (brown926@live.missouristate.edu)

Missouri State University
Department of Computer Science
May 1, 2025
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional

class BaseClusterer(ABC):

    def __init__(self, index, metadata_path: str, output_dir: str, 
                 logger=None, profiler=None):
        self.index = index
        self.metadata_path = metadata_path
        self.output_dir = output_dir
        self.logger = logger
        self.profiler = profiler
    
    @abstractmethod
    def extract_user_features(self) -> Dict[str, Dict[str, Any]]:
        pass
    
    @abstractmethod
    def cluster(self, k: int = 5, **kwargs) -> Dict[int, List[str]]:
        pass
    
    @abstractmethod
    def save(self) -> bool:
        pass
    
    @classmethod
    @abstractmethod
    def load(cls, index, metadata_path: str, output_dir: str, 
             logger=None, profiler=None) -> 'BaseClusterer':
        pass
    
    @abstractmethod
    def analyze_clusters(self) -> Dict[int, Dict[str, Any]]:
        pass
    
    @abstractmethod
    def visualize(self, filepath: str = None) -> Optional[str]:
        pass
    
    @abstractmethod
    def get_cluster_for_user(self, user_id: str) -> int:
        pass
    
    @abstractmethod
    def get_cluster_description(self, cluster_id: int) -> str:
        pass
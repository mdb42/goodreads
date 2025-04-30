# src/clustering/kmeans.py
"""
CSC790 Information Retrieval - Final Project
Goodreads Sentiment Analysis and Information Retrieval System

This module implements K-means clustering for grouping users based on
their review patterns and behaviors. It identifies reviewer archetypes
by analyzing similarities in rating patterns, vocabulary usage, and 
review frequencies.

Authors:
    Matthew D. Branson (branson773@live.missouristate.edu)
    James R. Brown (brown926@live.missouristate.edu)

Missouri State University
Department of Computer Science
May 1, 2025
"""

import os
import json
import pickle
import random
import numpy as np
import pandas as pd
from collections import defaultdict, Counter
from typing import Dict, List, Any, Optional
import matplotlib.pyplot as plt
from src.clustering.base_clusterer import BaseClusterer

class KMeansClusterer(BaseClusterer):
    """
    K-means clustering implementation for grouping similar users.
    
    This class uses K-means clustering to identify groups of users with
    similar review behaviors and preferences. It implements the BaseClusterer
    interface, providing methods for extracting user features, clustering,
    and analyzing the resulting clusters.
    
    Attributes:
        index: Document index containing review information
        metadata_path: Path to CSV file with review metadata
        output_dir: Directory to save clustering results
        logger: Logger for status messages
        profiler: Optional performance profiler
        k: Number of clusters to create
        max_iter: Maximum iterations for K-means algorithm
        tol: Convergence tolerance
        user_features: Extracted feature vectors for each user
        centroids: Cluster centers
        cluster_assignments: Mapping of users to clusters
        metadata: Loaded metadata from CSV
    """
    
    def __init__(self, index, metadata_path: str, output_dir: str, 
                 logger=None, profiler=None, k: int = 5, 
                 max_iter: int = 100, tol: float = 1e-4):
        """
        Initialize the KMeans clusterer.
        
        Args:
            index: Document index containing review information
            metadata_path: Path to CSV file with review metadata
            output_dir: Directory to save clustering results
            logger: Logger for status messages
            profiler: Optional performance profiler
            k: Number of clusters to create
            max_iter: Maximum iterations for K-means algorithm
            tol: Convergence tolerance
        """
        super().__init__(index, metadata_path, output_dir, logger, profiler)
        self.k = k
        self.max_iter = max_iter
        self.tol = tol
        self.user_features = {}
        self.centroids = {}
        self.cluster_assignments = {}
        self.metadata = None
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Load metadata
        try:
            self.metadata = pd.read_csv(metadata_path)
            if self.logger:
                self.logger.info(f"[+] Loaded metadata with {len(self.metadata):,} rows")
        except Exception as e:
            if self.logger:
                self.logger.error(f"[!] Failed to load metadata: {e}")
    
    def extract_user_features(self) -> Dict[str, Dict[str, Any]]:
        """
        Extract feature vectors for each user from their reviews.

        Returns:
            Dict[str, Dict[str, float]]: Mapping of user_ids to feature vectors
        """
        if self.logger:
            self.logger.info("[+] Extracting user features...")

        if self.metadata is None or self.metadata.empty:
            if self.logger:
                self.logger.error("[!] Metadata not loaded.")
            return {}

        # Group reviews by user
        grouped = self.metadata.groupby("user_id")

        user_features = {}

        for user_id, group in grouped:
            ratings = group["rating"].values
            lengths = group["text_length"].values

            # Simple numeric feature vector per user
            features = {
                "num_reviews": len(ratings),
                "avg_rating": float(np.mean(ratings)),
                "rating_variance": float(np.var(ratings)),
                "avg_length": float(np.mean(lengths)),
            }
            user_features[str(user_id)] = features

        self.user_features = user_features

        if self.logger:
            self.logger.info(f"[+] Extracted features for {len(self.user_features)} users")

        return self.user_features

    def cluster(self, k: int = None, **kwargs) -> Dict[int, List[str]]:
        """
        Cluster users based on their feature vectors using K-means.
        
        Args:
            k: Number of clusters (overrides the value set in constructor)
            
        Returns:
            Dict[int, List[str]]: Mapping of cluster_ids to lists of user_ids
        """
        if k is not None:
            self.k = k

        if self.logger:
            self.logger.info(f"[+] Clustering users into {self.k} groups...")

        if not self.user_features:
            self.extract_user_features()

        if not self.user_features:
            if self.logger:
                self.logger.error("[!] No user features available for clustering")
            return {}

        user_ids = list(self.user_features.keys())
        vectors = [self.user_features[uid] for uid in user_ids]

        # Convert vectors to NumPy array
        feature_keys = sorted(vectors[0].keys())
        X = np.array([[vec[key] for key in feature_keys] for vec in vectors])

        # Initialize centroids randomly
        initial_indices = np.random.choice(len(X), self.k, replace=False)
        centroids = X[initial_indices]

        for i in range(self.max_iter):
            # Assign each point to the nearest centroid
            distances = np.linalg.norm(X[:, np.newaxis] - centroids, axis=2)
            assignments = np.argmin(distances, axis=1)

            # Recompute centroids
            new_centroids = np.array([
                X[assignments == j].mean(axis=0) if np.any(assignments == j) else centroids[j]
                for j in range(self.k)
            ])

            if np.allclose(centroids, new_centroids, atol=self.tol):
                break

            centroids = new_centroids

        # Final assignments
        self.cluster_assignments = {
            user_id: int(assignments[i]) for i, user_id in enumerate(user_ids)
        }

        # Save centroids
        self.centroids = {
            i: {key: float(val) for key, val in zip(feature_keys, centroids[i])}
            for i in range(self.k)
        }

        # Group users by cluster
        cluster_members = defaultdict(list)
        for user_id, cluster_id in self.cluster_assignments.items():
            cluster_members[cluster_id].append(user_id)

        if self.logger:
            self.logger.info(f"[+] Clustering complete. Found {len(cluster_members)} clusters")

        return dict(cluster_members)

    def save(self) -> bool:
        """
        Save the clustering results to the output directory.
        
        This saves:
        1. User features
        2. Centroids
        3. Cluster assignments
        
        Returns:
            bool: Whether the save was successful
        """
        if self.logger:
            self.logger.info(f"[+] Saving clustering results to {self.output_dir}")
            
        try:
            # Save user features
            features_path = os.path.join(self.output_dir, "user_features.pkl")
            with open(features_path, 'wb') as f:
                pickle.dump(self.user_features, f)
                
            # Save centroids
            centroids_path = os.path.join(self.output_dir, "centroids.pkl")
            with open(centroids_path, 'wb') as f:
                pickle.dump(self.centroids, f)
                
            # Save cluster assignments
            assignments_path = os.path.join(self.output_dir, "cluster_assignments.json")
            with open(assignments_path, 'w') as f:
                # Convert keys to strings for JSON serialization
                json.dump({str(k): v for k, v in self.cluster_assignments.items()}, f)
                
            # Save cluster analysis
            analysis_path = os.path.join(self.output_dir, "cluster_analysis.json")
            analysis = self.analyze_clusters()
            with open(analysis_path, 'w') as f:
                # Convert numeric keys to strings for JSON
                serializable_analysis = {str(k): v for k, v in analysis.items()}
                json.dump(serializable_analysis, f, indent=2)
                
            # Save configuration
            config_path = os.path.join(self.output_dir, "config.json")
            config = {
                "k": self.k,
                "max_iter": self.max_iter,
                "tol": self.tol,
                "metadata_path": self.metadata_path
            }
            with open(config_path, 'w') as f:
                json.dump(config, f, indent=2)
                
            if self.logger:
                self.logger.info(f"[+] Clustering results saved successfully")
                
            return True
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"[!] Failed to save clustering results: {e}")
            return False
    
    @classmethod
    def load(cls, index, metadata_path: str, output_dir: str, 
             logger=None, profiler=None) -> 'KMeansClusterer':
        """
        Load existing clustering results.
        
        Args:
            index: Document index containing review information
            metadata_path: Path to CSV file with review metadata
            output_dir: Directory containing saved clustering results
            logger: Logger for status messages
            profiler: Optional performance profiler
            
        Returns:
            KMeansClusterer: Loaded clusterer instance
        """
        if logger:
            logger.info(f"[+] Loading clustering results from {output_dir}")
            
        try:
            # Load configuration
            config_path = os.path.join(output_dir, "config.json")
            with open(config_path, 'r') as f:
                config = json.load(f)
                
            # Create instance with loaded configuration
            instance = cls(
                index=index,
                metadata_path=metadata_path,
                output_dir=output_dir,
                logger=logger,
                profiler=profiler,
                k=config.get("k", 5),
                max_iter=config.get("max_iter", 100),
                tol=config.get("tol", 1e-4)
            )
            
            # Load user features
            features_path = os.path.join(output_dir, "user_features.pkl")
            with open(features_path, 'rb') as f:
                instance.user_features = pickle.load(f)
                
            # Load centroids
            centroids_path = os.path.join(output_dir, "centroids.pkl")
            with open(centroids_path, 'rb') as f:
                instance.centroids = pickle.load(f)
                
            # Load cluster assignments
            assignments_path = os.path.join(output_dir, "cluster_assignments.json")
            with open(assignments_path, 'r') as f:
                # Convert string keys back to integers
                string_assignments = json.load(f)
                instance.cluster_assignments = {int(k): v for k, v in string_assignments.items()}
                
            if logger:
                logger.info(f"[+] Loaded clustering results for {len(instance.user_features)} users")
                
            return instance
            
        except Exception as e:
            if logger:
                logger.error(f"[!] Failed to load clustering results: {e}")
                
            # Return a new instance if loading fails
            return cls(index, metadata_path, output_dir, logger, profiler)
    
    def analyze_clusters(self) -> Dict[int, Dict[str, Any]]:
        """
        Generate statistics and insights about each cluster.

        Returns:
            Dict[int, Dict[str, Any]]: Statistics for each cluster
        """
        if self.logger:
            self.logger.info("[+] Analyzing clusters...")

        if not self.cluster_assignments:
            if self.logger:
                self.logger.warning("[!] No clusters to analyze. Run cluster() first.")
            return {}

        analysis = {}

        # Group users by cluster
        cluster_members = defaultdict(list)
        for user_id, cluster_id in self.cluster_assignments.items():
            cluster_members[cluster_id].append(user_id)

        # Analyze each cluster
        for cluster_id, members in cluster_members.items():
            analysis[cluster_id] = {
                "size": len(members),
                "members": members,
                "centroid": self.centroids.get(cluster_id, {}),
                "description": self.get_cluster_description(cluster_id),
            }

        if self.logger:
            self.logger.info(f"[+] Analysis complete for {len(analysis)} clusters")

        return analysis

    def visualize(self, filepath: str = None) -> Optional[str]:
        """
        Generate a 2D PCA scatter plot of the user clusters.

        Args:
            filepath: Optional path to save the visualization

        Returns:
            Optional[str]: Path to saved visualization if specified
        """
        if self.logger:
            self.logger.info("[+] Generating cluster visualizations...")

        if not self.cluster_assignments:
            if self.logger:
                self.logger.warning("[!] No clusters to visualize. Run cluster() first.")
            return None

        import matplotlib.pyplot as plt
        from sklearn.decomposition import PCA

        user_ids = list(self.user_features.keys())
        feature_keys = sorted(next(iter(self.user_features.values())).keys())
        X = np.array([[self.user_features[uid].get(k, 0.0) for k in feature_keys] for uid in user_ids])
        labels = np.array([self.cluster_assignments[uid] for uid in user_ids])

        # Reduce dimensions with PCA
        pca = PCA(n_components=2)
        X_reduced = pca.fit_transform(X)

        # Plot
        plt.figure(figsize=(10, 7))
        for cluster_id in np.unique(labels):
            idxs = labels == cluster_id
            plt.scatter(X_reduced[idxs, 0], X_reduced[idxs, 1], label=f"Cluster {cluster_id}", s=8, alpha=0.7)

        plt.title("User Clusters (PCA Visualization)")
        plt.xlabel("PC1")
        plt.ylabel("PC2")
        plt.legend()
        plt.tight_layout()

        if filepath:
            plt.savefig(filepath)
            if self.logger:
                self.logger.info(f"[+] Visualization saved to {filepath}")
            return filepath
        else:
            plt.show()
            return None

    def get_cluster_for_user(self, user_id: str) -> int:
        """
        Get the cluster assignment for a specific user.
        
        Args:
            user_id: User identifier
            
        Returns:
            int: Cluster ID for the user, or -1 if not found
        """
        return self.cluster_assignments.get(user_id, -1)
    
    def get_cluster_description(self, cluster_id: int) -> str:
        """
        Generate a descriptive label for the cluster based on centroid values.

        Args:
            cluster_id: ID of the cluster

        Returns:
            str: Natural-language summary of the cluster's reviewer type
        """
        if cluster_id not in self.centroids:
            return "Unknown cluster"

        centroid = self.centroids[cluster_id]
        mean_rating = centroid.get("mean_rating", 0)
        var_rating = centroid.get("var_rating", 0)
        mean_length = centroid.get("mean_review_length", 0)

        description = []

        # Rating-based
        if mean_rating < 2:
            description.append("critical reviewers")
        elif mean_rating < 3:
            description.append("mixed or skeptical reviewers")
        elif mean_rating < 4:
            description.append("moderately positive reviewers")
        else:
            description.append("enthusiastic fans")

        # Variance-based
        if var_rating > 1.5:
            description.append("with inconsistent ratings")
        elif var_rating < 0.5:
            description.append("with consistent ratings")

        # Review length
        if mean_length > 500:
            description.append("who write long reviews")
        elif mean_length < 100:
            description.append("who write short reviews")

        return ", ".join(description).capitalize()
    
    def _euclidean_distance(self, v1: Dict[str, float], v2: Dict[str, float]) -> float:
        """
        Calculate Euclidean distance between two feature vectors.
        
        Args:
            v1: First feature vector
            v2: Second feature vector
            
        Returns:
            float: Euclidean distance
        """
        return np.sqrt(sum((v1.get(dim, 0) - v2.get(dim, 0))**2 for dim in set(v1) | set(v2)))
    
    def _calculate_centroid(self, vectors: List[Dict[str, float]]) -> Dict[str, float]:
        """
        Calculate centroid (mean vector) of a set of vectors.
        
        Args:
            vectors: List of feature vectors
            
        Returns:
            Dict[str, float]: Centroid vector
        """
        dimensions = set()
        for v in vectors:
            dimensions.update(v.keys())
            
        centroid = {}
        for dim in dimensions:
            centroid[dim] = sum(v.get(dim, 0) for v in vectors) / len(vectors)
            
        return centroid
    
    def _assignments_converged(self, prev: Dict[str, int], current: Dict[str, int]) -> bool:
        """
        Check if cluster assignments have converged.
        
        Args:
            prev: Previous cluster assignments
            current: Current cluster assignments
            
        Returns:
            bool: Whether assignments have converged
        """
        if not prev:
            return False
            
        differences = sum(1 for user_id in current if user_id in prev and current[user_id] != prev[user_id])
        return differences / len(current) < self.tol
    
    def write_summary_markdown(self, filepath: str):
        """
        Write a Markdown summary of each cluster's characteristics.

        Args:
            filepath: Path to save the summary Markdown file
        """
        if not self.cluster_assignments or not self.centroids:
            if self.logger:
                self.logger.warning("[!] No clustering data available for summary.")
            return

        # Group members by cluster
        cluster_members = defaultdict(list)
        for user_id, cluster_id in self.cluster_assignments.items():
            cluster_members[cluster_id].append(user_id)

        # Begin writing Markdown
        lines = [
            "# Cluster Summary",
            "",
            f"**Total Users Clustered:** {len(self.cluster_assignments)}",
            f"**Total Clusters:** {len(self.centroids)}",
            ""
        ]

        for cluster_id in sorted(self.centroids.keys()):
            centroid = self.centroids[cluster_id]
            description = self.get_cluster_description(cluster_id)
            size = len(cluster_members[cluster_id])

            lines.extend([
                f"## Cluster {cluster_id}",
                f"**Size:** {size} users",
                f"**Description:** {description}",
                "",
                "**Centroid Statistics:**",
                "",
                "```",
                json.dumps(centroid, indent=2),
                "```",
                ""
            ])

        try:
            with open(filepath, "w", encoding="utf-8") as f:
                f.write("\n".join(lines))
            if self.logger:
                self.logger.info(f"[+] Cluster summary written to {filepath}")
        except Exception as e:
            if self.logger:
                self.logger.error(f"[!] Failed to write cluster summary: {e}")

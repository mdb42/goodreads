# src/clustering/kmeans.py
import numpy as np
from collections import defaultdict
import random
from typing import List, Dict, Tuple

class KMeans:
    # TODO: Implement this for real. This is just a rough sketch.
    
    def __init__(self, k=5, max_iter=100, tol=1e-4):
        """
        Initialize K-means clustering.
        
        Args:
            k: Number of clusters
            max_iter: Maximum iterations
            tol: Convergence tolerance
        """
        self.k = k
        self.max_iter = max_iter
        self.tol = tol
        self.centroids = None
        self.cluster_assignments = None
        
    def fit(self, user_vectors):
        """
        Cluster the user vectors.
        
        Args:
            user_vectors: Dict mapping user_ids to feature vectors
            
        Returns:
            Self for method chaining
        """
        # Initialize centroids (randomly select k users)
        user_ids = list(user_vectors.keys())
        centroid_users = random.sample(user_ids, self.k)
        self.centroids = {i: user_vectors[user_id].copy() for i, user_id in enumerate(centroid_users)}
        
        # Initialize cluster assignments
        self.cluster_assignments = {}
        prev_assignments = {}
        
        # Iterative refinement
        for iteration in range(self.max_iter):
            # Assign users to nearest centroid
            for user_id, vector in user_vectors.items():
                distances = {
                    cluster_id: self._euclidean_distance(vector, centroid) 
                    for cluster_id, centroid in self.centroids.items()
                }
                self.cluster_assignments[user_id] = min(distances, key=distances.get)
            
            # Check for convergence
            if self._assignments_converged(prev_assignments, self.cluster_assignments):
                break
                
            prev_assignments = self.cluster_assignments.copy()
            
            # Update centroids
            cluster_members = defaultdict(list)
            for user_id, cluster_id in self.cluster_assignments.items():
                cluster_members[cluster_id].append(user_vectors[user_id])
                
            for cluster_id, members in cluster_members.items():
                if members:
                    # Calculate mean vector for cluster
                    self.centroids[cluster_id] = self._calculate_centroid(members)
        
        return self
        
    def _euclidean_distance(self, v1, v2):
        """Calculate Euclidean distance between vectors."""
        return np.sqrt(sum((v1.get(dim, 0) - v2.get(dim, 0))**2 for dim in set(v1) | set(v2)))
        
    def _calculate_centroid(self, vectors):
        """Calculate centroid (mean vector) of a set of vectors."""
        dimensions = set()
        for v in vectors:
            dimensions.update(v.keys())
            
        centroid = {}
        for dim in dimensions:
            centroid[dim] = sum(v.get(dim, 0) for v in vectors) / len(vectors)
            
        return centroid
        
    def _assignments_converged(self, prev, current):
        """Check if cluster assignments have converged."""
        if not prev:
            return False
            
        differences = sum(1 for user_id in current if user_id in prev and current[user_id] != prev[user_id])
        return differences / len(current) < self.tol
    
    def predict(self, vector):
        """Assign a new vector to the nearest cluster."""
        if not self.centroids:
            raise ValueError("Model must be fitted before prediction")
            
        distances = {
            cluster_id: self._euclidean_distance(vector, centroid) 
            for cluster_id, centroid in self.centroids.items()
        }
        return min(distances, key=distances.get)
        
    def get_cluster_stats(self, user_vectors, user_metadata=None):
        """
        Get statistics about each cluster.
        
        Args:
            user_vectors: Dict mapping user_ids to feature vectors
            user_metadata: Optional dict with additional user information
            
        Returns:
            Dict with cluster statistics
        """
        stats = {}
        
        # Group users by cluster
        clusters = defaultdict(list)
        for user_id, cluster_id in self.cluster_assignments.items():
            clusters[cluster_id].append(user_id)
            
        for cluster_id, user_ids in clusters.items():
            cluster_data = {
                "size": len(user_ids),
                "centroid": self.centroids[cluster_id],
                "users": user_ids
            }
            
            # Calculate additional stats if metadata available
            if user_metadata:
                # TODO: Calulate average ratings, review counts, etc. based on user_metadata
                pass
                
            stats[cluster_id] = cluster_data
            
        return stats
#!/usr/bin/env python3
"""
CSC790 Information Retrieval - Final Project
Goodreads Sentiment Analysis and Information Retrieval System

This module provides a stub implementation of the BaseClassifier that can be
extended with actual classification logic. It outlines the expected methods
and parameters for sentiment classification.

Authors:
    Matthew D. Branson (branson773@live.missouristate.edu)
    James R. Brown (brown926@live.missouristate.edu)

Missouri State University
Department of Computer Science
May 1, 2025

REPORT EXCERPT: 
    To construct the sentiment analyzer, we will be using a Naive Bayes classifier to make
    decisions on the sentiment "rating" of a review. This classifier will be fed information from
    a feature extraction pipeline using tf-idf over tokenized and stemmed reviews. This feature
    extraction method helps determine how "important" a word is to any given review. From these,
    we are looking to predict a star rating on a discrete 1-5 scale, matching Goodreads' review
    system.
"""

import os
import pickle
from typing import Dict, List, Any, Optional
from src.classification import BaseClassifier
from sklearn.model_selection import KFold
from src.classification.multinomial_nb import MultinomialNB

class Classifier(BaseClassifier):
    """
    Stub implementation of the sentiment classifier interface.
    
    This class provides the framework for classifying review sentiment
    into star ratings (1-5). It implements the BaseClassifier interface
    with placeholder methods that should be extended with actual logic.
    
    Attributes:
        zip_path: Path to ZIP archive containing review documents
        metadata_path: Path to CSV file with review ratings
        models_dir: Directory to save/load models
        models: List of trained models (one per fold)
        features: Selected features for classification
        logger: Logger for status messages
        profiler: Optional performance profiler
    """
    
    def __init__(self, zip_path: str, metadata_path: str, models_dir: str, 
                 logger=None, profiler=None):
        """
        Initialize the classifier with all necessary resources.
        
        Args:
            zip_path: Path to ZIP archive containing review documents
            metadata_path: Path to CSV file with review ratings
            models_dir: Directory to save/load models
            logger: Logger for status messages
            profiler: Optional performance profiler
        """
        super().__init__(zip_path, metadata_path, models_dir, logger, profiler)
        self.logger = logger
        self.zip_path = zip_path
        self.metadata_path = metadata_path
        self.models_dir = models_dir
        self.profiler = profiler
        self.models = []
        self.features = None
        
        # Create models directory if it doesn't exist
        os.makedirs(models_dir, exist_ok=True)
        
        if self.logger:
            self.logger.info("[+] Classifier initialized")
            self.logger.info(f"[+] Reviews source: {zip_path}")
            self.logger.info(f"[+] Metadata source: {metadata_path}")
            self.logger.info(f"[+] Models directory: {models_dir}")
    
    def train(self, k_folds: int = 5, **kwargs) -> Dict[str, float]:
        """
        Train the classifier with k-fold cross-validation.
        
        This method should:
        1. Load reviews and ratings
        2. Split the data into k folds
        3. Train a model for each fold (using k-1 folds for training)
        4. Evaluate each model on its test fold
        5. Aggregate performance metrics
        
        Args:
            k_folds: Number of folds for cross-validation
            **kwargs: Additional training parameters (feature selection, etc.)
                
        Returns:
            Dict[str, float]: Training metrics
        """
        if self.logger:
            self.logger.info(f"[+] Training classifier with {k_folds}-fold cross-validation")
        
        # TODO: Implement k-fold cross-validation and model training
        # 1. Load data from zip_path and metadata_path
        # 2. Split data into k folds
        # 3. For each fold:
        #    - Train model on k-1 folds
        #    - Test on remaining fold
        #    - Store model and metrics
        # 4. Calculate average metrics across folds

        # === 1. Load metadata + build index ===
        doc_labels = self._load_labels()
        index = self._build_index(doc_labels)

        # === 2. Feature selection (optional) ===
        feature_selector = kwargs.get("feature_selector")
        k_features = kwargs.get("k_features", 1000)

        if feature_selector:
            self.features = feature_selector(index, doc_labels, k=k_features)
            self.logger.info(f"[+] Selected {len(self.features)} features using {feature_selector.__name__}")
        else:
            self.features = set(index.term_doc_freqs.keys())
            self.logger.info(f"[+] Using all {len(self.features)} terms")

        # === 3. K-Fold CV ===
        doc_ids = list(doc_labels.keys())
        kf = KFold(n_splits=k_folds, shuffle=True, random_state=42)
        metrics = []

        classifier_type = kwargs.get("classifier_type", "naive_bayes")
        model_params = kwargs.get("model_params", {})

        for fold_idx, (train_idx, test_idx) in enumerate(kf.split(doc_ids)):
            self.logger.info(f"[+] Fold {fold_idx + 1}/{k_folds}")

            train_ids = [doc_ids[i] for i in train_idx]
            test_ids = [doc_ids[i] for i in test_idx]

            train_labels = {doc_id: doc_labels[doc_id] for doc_id in train_ids}
            test_labels = {doc_id: doc_labels[doc_id] for doc_id in test_ids}

            # === 4. Model selection ===
            if classifier_type == "naive_bayes":
                model = MultinomialNB(**model_params)
            else:
                raise ValueError(f"Unsupported classifier type: {classifier_type}")

            model.fit(index, train_labels)
            result = model.evaluate(index, test_labels)

            self.models.append(model)
            metrics.append(result)

        # === 5. Aggregate results ===
        avg_metrics = {
            "accuracy": sum(m.get("accuracy", 0) for m in metrics) / k_folds
        }

        self.logger.info(f"[+] Training complete. Avg accuracy: {avg_metrics['accuracy']:.4f}")
        return avg_metrics
        # Placeholder metrics
        # return {
        #     "accuracy": 0.0,
        #     "precision": 0.0,
        #     "recall": 0.0,
        #     "f1": 0.0
        # }
    
    def save(self) -> bool:
        """
        Save the trained classifier models to the models directory.
        
        Returns:
            bool: Whether the save was successful
        """
        if self.logger:
            self.logger.info(f"[+] Saving models to {self.models_dir}")
        
        try:
            # Save each model separately
            for i, model in enumerate(self.models):
                model_path = os.path.join(self.models_dir, f"model_{i}.pkl")
                
                # TODO: Implement model saving
                # with open(model_path, 'wb') as f:
                #     pickle.dump(model, f)
            
            # Save features
            features_path = os.path.join(self.models_dir, "features.pkl")
            # TODO: Implement feature saving
            # with open(features_path, 'wb') as f:
            #     pickle.dump(self.features, f)
            
            if self.logger:
                self.logger.info(f"[+] Models saved successfully")
            return True
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"[!] Failed to save models: {e}")
            return False
    
    @classmethod
    def load(cls, zip_path: str, metadata_path: str, models_dir: str, 
             logger=None, profiler=None) -> 'Classifier':
        """
        Load trained classifier models from a directory.
        
        Args:
            zip_path: Path to ZIP archive containing review documents
            metadata_path: Path to CSV file with review ratings
            models_dir: Directory containing saved model files
            logger: Logger for status messages
            profiler: Optional performance profiler
                
        Returns:
            Classifier: Loaded classifier instance
        """
        instance = cls(zip_path, metadata_path, models_dir, logger, profiler)
        
        if logger:
            logger.info(f"[+] Loading models from {models_dir}")
        
        try:
            # Load features
            features_path = os.path.join(models_dir, "features.pkl")
            # TODO: Implement feature loading
            # with open(features_path, 'rb') as f:
            #     instance.features = pickle.load(f)
            
            # Load models
            model_files = [f for f in os.listdir(models_dir) if f.startswith("model_") and f.endswith('.pkl')]
            
            for model_file in sorted(model_files):
                model_path = os.path.join(models_dir, model_file)
                # TODO: Implement model loading
                # with open(model_path, 'rb') as f:
                #     model = pickle.load(f)
                #     instance.models.append(model)
            
            if logger:
                logger.info(f"[+] Loaded {len(instance.models)} models successfully")
            
            return instance
            
        except Exception as e:
            if logger:
                logger.error(f"[!] Failed to load models: {e}")
            return instance
    
    def evaluate(self) -> Dict[str, float]:
        """
        Evaluate the classifier on test data.
        
        This method can be used to evaluate the classifier on a separate
        test set after training. For cross-validation results, see the
        metrics returned by the train method.
        
        Returns:
            Dict[str, float]: Evaluation metrics
        """
        if self.logger:
            self.logger.info(f"[+] Evaluating classifier")
        
        if not self.models:
            if self.logger:
                self.logger.error("[!] No trained models available")
            return {
                "accuracy": 0.0,
                "precision": 0.0,
                "recall": 0.0,
                "f1": 0.0
            }
        
        # TODO: Implement evaluation
        # 1. Load test data
        # 2. Preprocess and extract features
        # 3. Get predictions from all models
        # 4. Calculate metrics
        
        # Placeholder metrics
        return {
            "accuracy": 0.0,
            "precision": 0.0,
            "recall": 0.0,
            "f1": 0.0
        }
    
    def predict(self, text: str) -> int:
        """
        Predict rating for a single review text.
        
        Args:
            text: Review text to classify
                
        Returns:
            int: Predicted rating (1-5)
        """
        if not self.models:
            if self.logger:
                self.logger.error("[!] No trained models available")
            return 3  # Default to middle rating
        
        # TODO: Implement prediction
        # 1. Preprocess text
        # 2. Extract features
        # 3. Get predictions from all models
        # 4. Combine predictions (e.g., voting, averaging)
        
        return 3  # Placeholder (default to middle rating)
    
    def predict_batch(self, texts: List[str]) -> List[int]:
        """
        Predict ratings for multiple review texts.
        
        Args:
            texts: List of review texts to classify

        Returns:
            List[int]: Predicted ratings (1-5)
        """
        return [self.predict(text) for text in texts]

    def _build_index(self):
        from src.index.parallel_zip_index import ParallelZipIndex

        if self.logger:
            self.logger.info("[+] Building or loading index")

        index = ParallelZipIndex(self.zip_path, logger=self.logger)
        index.build_index()

        return index

    def _load_labels(self, index):
        import csv

        if self.logger:
            self.logger.info("[+] Loading document labels using index filenames")

        labels = {}
        filename_to_id = {filename: i for i, filename in enumerate(index.filenames)}

        with open(self.metadata_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                filename = row.get("filename") or row.get("file")  # adjust as needed
                rating = row.get("rating")
                if filename and rating:
                    try:
                        doc_id = filename_to_id[filename]
                        labels[doc_id] = int(rating)
                    except KeyError:
                        continue  # metadata file name not found in ZIP

        if self.logger:
            self.logger.info(f"[+] Loaded {len(labels)} document labels")

        return labels

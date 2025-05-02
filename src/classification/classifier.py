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
import csv
import pickle
from typing import Dict, List
from src.classification import BaseClassifier
from src.classification.multinomial_nb import MultinomialNB
from collections import defaultdict

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
        
        # Create a model directory if it doesn't exist
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

        # Load data
        index = self._build_index()
        doc_labels = self._load_labels(index)

        # Optional feature selection
        feature_selector = kwargs.get("feature_selector")
        k_features = kwargs.get("k_features", 1000)

        if feature_selector:
            self.features = feature_selector(index, doc_labels, k=k_features)
            self.logger.info(f"[+] Selected {len(self.features)} features using {feature_selector.__name__}")
        else:
            self.features = set(index.term_doc_freqs.keys())
            self.logger.info(f"[+] Using all {len(self.features)} terms")

        # Apply feature filtering
        if self.features:
            for doc_id in index.doc_term_freqs:
                index.doc_term_freqs[doc_id] = {
                    t: f for t, f in index.doc_term_freqs[doc_id].items() if t in self.features
                }

        # Train model
        model_params = kwargs.get("model_params", {})
        model = MultinomialNB(**model_params)
        model.fit(index, doc_labels)
        self.models = [model]  # single model

        # Evaluate on full training set
        preds = model.predict(index.doc_term_freqs)

        y_true = [doc_labels[doc_id] for doc_id in preds]
        y_pred = [preds[doc_id] for doc_id in preds]

        # Accuracy
        correct = sum(yt == yp for yt, yp in zip(y_true, y_pred))
        acc = correct / len(y_true) if y_true else 0.0

        # Precision, Recall, F1 (macro-averaged)
        tp = defaultdict(int)
        fp = defaultdict(int)
        fn = defaultdict(int)
        classes = set(y_true + y_pred)

        for yt, yp in zip(y_true, y_pred):
            if yt == yp:
                tp[yt] += 1
            else:
                fp[yp] += 1
                fn[yt] += 1

        precision_sum = 0
        recall_sum = 0
        f1_sum = 0
        for c in classes:
            prec = tp[c] / (tp[c] + fp[c]) if (tp[c] + fp[c]) > 0 else 0
            rec = tp[c] / (tp[c] + fn[c]) if (tp[c] + fn[c]) > 0 else 0
            f1 = 2 * prec * rec / (prec + rec) if (prec + rec) > 0 else 0

            precision_sum += prec
            recall_sum += rec
            f1_sum += f1

        num_classes = len(classes)
        precision = precision_sum / num_classes
        recall = recall_sum / num_classes
        f1 = f1_sum / num_classes

        return {
            "accuracy": acc,
            "precision": precision,
            "recall": recall,
            "f1": f1
        }
    
    def save(self) -> bool:
        """
        Save the trained classifier models to the models directory.
        
        Returns:
            bool: Whether the save was successful
        """
        if self.logger:
            self.logger.info(f"[+] Saving models to {self.models_dir}")

        try:
            if not self.models:
                raise ValueError("No model to save.")

            model_path = os.path.join(self.models_dir, "model.pkl")
            with open(model_path, 'wb') as f:
                pickle.dump(self.models[0], f)

            features_path = os.path.join(self.models_dir, "features.pkl")
            with open(features_path, 'wb') as f:
                pickle.dump(self.features, f)

            if self.logger:
                self.logger.info(f"[+] Model and features saved successfully")
            return True

        except Exception as e:
            if self.logger:
                self.logger.error(f"[!] Failed to save model: {e}")
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
            with open(features_path, 'rb') as f:
                instance.features = pickle.load(f)
            
            # Load models
            model_files = [f for f in os.listdir(models_dir) if f.startswith("model") and f.endswith('.pkl')]
            
            for model_file in sorted(model_files):
                model_path = os.path.join(models_dir, model_file)
                with open(model_path, 'rb') as f:
                    model = pickle.load(f)
                    instance.models.append(model)
            
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
            self.logger.error("[!] No trained model available for evaluation")
            return {"accuracy": 0.0, "precision": 0.0, "recall": 0.0, "f1": 0.0}

        model = self.models[0]  # single trained model

        # Rebuild index and reload labels
        index = self._build_index()
        doc_labels = self._load_labels(index)

        # Apply feature filtering (if features were selected)
        if self.features:
            for doc_id in index.doc_term_freqs:
                index.doc_term_freqs[doc_id] = {
                    t: f for t, f in index.doc_term_freqs[doc_id].items() if t in self.features
                }

        # Use model to predict on all documents
        predictions = model.predict(index.doc_term_freqs)

        # Extract true and predicted labels
        y_true = [doc_labels[doc_id] for doc_id in predictions]
        y_pred = [predictions[doc_id] for doc_id in predictions]

        # Compute metrics
        ## Accuracy
        correct = sum(yt == yp for yt, yp in zip(y_true, y_pred))
        acc = correct / len(y_true) if y_true else 0.0

        ## Precision, Recall, F1
        tp = defaultdict(int)
        fp = defaultdict(int)
        fn = defaultdict(int)
        classes = set(y_true + y_pred)

        for yt, yp in zip(y_true, y_pred):
            if yt == yp:
                tp[yt] += 1
            else:
                fp[yp] += 1
                fn[yt] += 1

        precision_sum = 0
        recall_sum = 0
        f1_sum = 0
        for c in classes:
            prec = tp[c] / (tp[c] + fp[c]) if (tp[c] + fp[c]) > 0 else 0
            rec = tp[c] / (tp[c] + fn[c]) if (tp[c] + fn[c]) > 0 else 0
            f1 = 2 * prec * rec / (prec + rec) if (prec + rec) > 0 else 0

            precision_sum += prec
            recall_sum += rec
            f1_sum += f1

        num_classes = len(classes)
        precision = precision_sum / num_classes
        recall = recall_sum / num_classes
        f1 = f1_sum / num_classes

        return {
            "accuracy": acc,
            "precision": precision,
            "recall": recall,
            "f1": f1
        }

    def predict(self, text: str) -> int:
        """
        Predict a rating for a single preprocessed review string.

        Args:
            text: A preprocessed string of terms (space-separated)

        Returns:
            int: Predicted rating (1–5)
        """
        if not self.models:
            self.logger.error("[!] No trained model available for prediction")
            return 3  # neutral fallback

        model = self.models[0]
        term_freqs = self._count_terms(text)

        doc_id = 0  # dummy key
        preds = model.predict({doc_id: term_freqs})

        return preds[doc_id]

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
        """
        Helper method to build a document index from the ZIP archive, using the ParallelZipIndex method.

        Returns:
            index: Document index
        """
        from src.index.parallel_zip_index import ParallelZipIndex

        if self.logger:
            self.logger.info("[+] Building or loading index")

        index = ParallelZipIndex(self.zip_path, logger=self.logger)
        index.build_index()

        return index

    def _load_labels(self, index):
        """
        Loads in review ratings for each document in the index from the metadata CSV, associates each docID with its
        rating, and returns a dictionary mapping docIDs to ratings.

        Args:
            index: Document index

        Returns:
            labels: Dictionary mapping docIDs to ratings
        """
        if self.logger:
            self.logger.info("[+] Loading document labels using index.filenames")
        labels = {}
        review_id_to_rating = {}

        # Load CSV into a map from review_id → rating
        with open(self.metadata_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                rid = row.get("review_id")
                rating = row.get("rating")
                if rid is not None and rating is not None:
                    review_id_to_rating[rid] = int(rating)

        # Walk index filenames and resolve labels
        for doc_id, fname in index.filenames.items():
            if not isinstance(fname, str):
                raise TypeError(f"Expected filename as string, got {type(fname)}: {fname}")
            review_id = os.path.splitext(fname)[0]

            # removes ".txt"
            if review_id in review_id_to_rating:
                labels[doc_id] = review_id_to_rating[review_id]

        self.logger.info(f"[+] Loaded {len(labels)} document labels from metadata")
        return labels

    def _count_terms(self, text: str) -> Dict[str, int]:
        """
        Convert a preprocessed string into a term frequency dictionary.

        Args:
            text: A pre-tokenized, stemmed string (e.g., "book amaz could put down")

        Returns:
            Dict[str, int]: Term frequency map
        """
        tf = {}
        for term in text.split():
            if not self.features or term in self.features:
                tf[term] = tf.get(term, 0) + 1
        return tf

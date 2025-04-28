# src/classifier/multinomial_nb.py
import numpy as np
import math
from collections import defaultdict, Counter
from typing import Dict, List, Set, Tuple

class MultinomialNB:
    # TODO: Implement this for real. This is just a rough sketch.
    
    def __init__(self, alpha=0.3):
        """
        Initialize the Multinomial Naive Bayes classifier.
        
        Args:
            alpha: Smoothing parameter (default=0.3 for Laplace smoothing)
        """
        self.alpha = alpha
        self.class_priors = {}
        self.term_probs = defaultdict(dict)  # P(t|c)
        self.vocab = set()
        self.classes = set()
        
    def fit(self, index, doc_labels):
        """
        Train the classifier on an indexed set of documents.
        
        Args:
            index: Document index with term frequencies
            doc_labels: Dict mapping doc_ids to class labels (star ratings)
        """
        # Count class occurrences
        class_counts = Counter(doc_labels.values())
        total_docs = len(doc_labels)
        
        # Calculate class priors
        for c in class_counts:
            self.classes.add(c)
            self.class_priors[c] = math.log(class_counts[c] / total_docs)
        
        # Set up term counts
        term_counts = {c: defaultdict(int) for c in self.classes}
        total_terms = {c: 0 for c in self.classes}
        
        # Count terms per class
        for doc_id, class_label in doc_labels.items():
            if doc_id not in index.doc_term_freqs:
                continue
                
            for term, freq in index.doc_term_freqs[doc_id].items():
                self.vocab.add(term)
                term_counts[class_label][term] += freq
                total_terms[class_label] += freq
        
        # Calculate term probabilities with Laplace smoothing
        vocab_size = len(self.vocab)
        
        for c in self.classes:
            denominator = total_terms[c] + self.alpha * vocab_size
            
            for term in self.vocab:
                numerator = term_counts[c].get(term, 0) + self.alpha
                self.term_probs[term][c] = math.log(numerator / denominator)
    
    def predict(self, doc):
        """
        Predict the class (star rating) for a document.
        
        Args:
            doc: Dict mapping terms to their frequencies
            
        Returns:
            Predicted class label (star rating)
        """
        scores = {c: self.class_priors[c] for c in self.classes}
        
        for term, freq in doc.items():
            if term in self.vocab:
                for c in self.classes:
                    if term in self.term_probs and c in self.term_probs[term]:
                        scores[c] += self.term_probs[term][c] * freq
        
        # Return the class with the highest score
        return max(scores.items(), key=lambda x: x[1])[0]
    
    def evaluate(self, index, test_labels):
        """
        Evaluate the classifier on test data.
        
        Args:
            index: Document index with term frequencies
            test_labels: Dict mapping doc_ids to true class labels
            
        Returns:
            Dict with accuracy and per-class metrics
        """
        correct = 0
        predictions = {}
        
        for doc_id, true_label in test_labels.items():
            if doc_id not in index.doc_term_freqs:
                continue
                
            doc = index.doc_term_freqs[doc_id]
            pred_label = self.predict(doc)
            predictions[doc_id] = pred_label
            
            if pred_label == true_label:
                correct += 1
        
        accuracy = correct / len(test_labels) if test_labels else 0
        
        # TODO: Calculate F1 scores per class
        
        
        return {
            "accuracy": accuracy,
            "predictions": predictions,
            # Additional metrics
        }
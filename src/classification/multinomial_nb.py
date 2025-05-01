# src/classifier/multinomial_nb.py
import numpy as np
from math import log, exp
from collections import defaultdict, Counter
from typing import Dict, List, Set, Tuple

class MultinomialNB:
    # TODO: Implement this for real. This is just a rough sketch.
    def __init__(self, alpha=1.0):
        self.alpha = alpha
        self.vocab = set()
        self.prior = {}  # P(c)
        self.condprob = {}  # P(t|c), nested dict: condprob[t][c]
        self.classes = set()

    def fit(self, index, doc_labels):
        """
        Train the Multinomial Naive Bayes model according to Algorithm 1.

        Args:
            index: object with .doc_term_freqs and .term_doc_freqs
            doc_labels: dict mapping doc_id -> class label
        """
        print(doc_labels)
        self.classes = set(doc_labels.values())
        self.vocab = set(index.term_doc_freqs.keys())
        N = len(doc_labels)  # total number of documents

        # Step 1: Count docs per class
        class_doc_counts = defaultdict(int)
        for c in doc_labels.values():
            class_doc_counts[c] += 1

        # Step 2: Compute priors
        for c in self.classes:
            self.prior[c] = class_doc_counts[c] / N

        # Step 3: Accumulate token counts per class
        T_ct = defaultdict(lambda: defaultdict(int))  # T_ct[t][c]
        total_tokens_in_class = defaultdict(int)

        for doc_id, label in doc_labels.items():
            doc_terms = index.doc_term_freqs[doc_id]
            for t, freq in doc_terms.items():
                T_ct[t][label] += freq
                total_tokens_in_class[label] += freq

        # Step 4: Compute conditional probabilities
        self.condprob = defaultdict(dict)

        print(f"Classes: {self.classes}")
        print(f"Vocab size: {len(self.vocab)}")
        print(f"Sample vocab: {list(self.vocab)[:5]}")

        print(f"T_ct keys: {list(T_ct.keys())[:5]}")
        print(f"Token count for one class: {total_tokens_in_class}")

        for t in self.vocab:
            for c in self.classes:
                numerator = T_ct[t][c] + self.alpha
                denominator = total_tokens_in_class[c] + self.alpha * len(self.vocab)
                self.condprob[t][c] = numerator / denominator

        print(self.condprob)

    def predict(self, doc_term_freqs):
        """
        Predict the class label for each input document according to Algorithm 2.

        Args:
            doc_term_freqs (dict): {doc_id: {term: freq}}

        Returns:
            dict: {doc_id: predicted_label}
        """
        predictions = {}

        for doc_id, terms in doc_term_freqs.items():
            scores = {}

            for c in self.classes:
                # Initialize with log prior
                score = log(self.prior[c])

                for t in terms:
                    if t in self.vocab:
                        score += terms[t] * log(self.condprob.get(t, {}).get(c, 1e-10))

                scores[c] = score

            # Choose class with highest score
            predictions[doc_id] = max(scores.items(), key=lambda x: x[1])[0]

        return predictions

#
# def __init__(self, alpha=0.3):
#     """
#     Initialize the Multinomial Naive Bayes classifier.
#
#     Args:
#         alpha: Smoothing parameter (default=0.3 for Laplace smoothing)
#     """
#     self.alpha = alpha
#     self.class_priors = {}
#     self.term_probs = defaultdict(dict)  # P(t|c)
#     self.vocab = set()
#     self.classes = set()
#
# def fit(self, index, doc_labels):
#     """
#     Train the classifier on an indexed set of documents.
#
#     Args:
#         index: Document index with term frequencies
#         doc_labels: Dict mapping doc_ids to class labels (star ratings)
#     """
#     # Count class occurrences
#     class_counts = Counter(doc_labels.values())
#     total_docs = len(doc_labels)
#
#     # Calculate class priors
#     for c in class_counts:
#         self.classes.add(c)
#         self.class_priors[c] = math.log(class_counts[c] / total_docs)
#
#     # Set up term counts
#     term_counts = {c: defaultdict(int) for c in self.classes}
#     total_terms = {c: 0 for c in self.classes}
#
#     # Count terms per class
#     for doc_id, class_label in doc_labels.items():
#         if doc_id not in index.doc_term_freqs:
#             continue
#
#         for term, freq in index.doc_term_freqs[doc_id].items():
#             self.vocab.add(term)
#             term_counts[class_label][term] += freq
#             total_terms[class_label] += freq
#
#     # Calculate term probabilities with Laplace smoothing
#     vocab_size = len(self.vocab)
#
#     for c in self.classes:
#         denominator = total_terms[c] + self.alpha * vocab_size
#
#         for term in self.vocab:
#             numerator = term_counts[c].get(term, 0) + self.alpha
#             self.term_probs[term][c] = math.log(numerator / denominator)
#
# def predict(self, doc):
#     """
#     Predict the class (star rating) for a document.
#
#     Args:
#         doc: Dict mapping terms to their frequencies
#
#     Returns:
#         Predicted class label (star rating)
#     """
#     scores = {c: self.class_priors[c] for c in self.classes}
#
#     for term, freq in doc.items():
#         if term in self.vocab:
#             for c in self.classes:
#                 if term in self.term_probs and c in self.term_probs[term]:
#                     scores[c] += self.term_probs[term][c] * freq
#
#     # Return the class with the highest score
#     return max(scores.items(), key=lambda x: x[1])[0]
#
# def evaluate(self, index, test_labels):
#     """
#     Evaluate the classifier on test data.
#
#     Args:
#         index: Document index with term frequencies
#         test_labels: Dict mapping doc_ids to true class labels
#
#     Returns:
#         Dict with accuracy and per-class metrics
#     """
#     correct = 0
#     predictions = {}
#
#     for doc_id, true_label in test_labels.items():
#         if doc_id not in index.doc_term_freqs:
#             continue
#
#         doc = index.doc_term_freqs[doc_id]
#         pred_label = self.predict(doc)
#         predictions[doc_id] = pred_label
#
#         if pred_label == true_label:
#             correct += 1
#
#     accuracy = correct / len(test_labels) if test_labels else 0
#
#     # TODO: Calculate F1 scores per class
#
#
#     return {
#         "accuracy": accuracy,
#         "predictions": predictions,
#         # Additional metrics
#     }
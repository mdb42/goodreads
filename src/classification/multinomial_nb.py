# src/classifier/multinomial_nb.py
from math import log
from collections import defaultdict

class MultinomialNB:
    def __init__(self, alpha=1.0):
        self.alpha = alpha
        self.vocab = set()
        self.prior = {}  # P(c)
        self.condprob = {}  # P(t|c)
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
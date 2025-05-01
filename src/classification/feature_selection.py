# src/classifier/feature_selection.py
import math
import numpy as np
from collections import defaultdict, Counter

def mutual_information(index, doc_labels, k=1000):
    # TODO: Implment this for real. This is just a rough sketch.
    classes = set(doc_labels.values())
    vocab = set(index.term_doc_freqs.keys())
    
    # Document counts for MI calculation
    N = len(doc_labels)
    N_c = {c: sum(1 for label in doc_labels.values() if label == c) for c in classes}
    
    # Initialize term presence counts
    N_t = defaultdict(int)      # Documents containing term t
    N_tc = defaultdict(lambda: defaultdict(int))  # Documents with term t and class c
    
    # Calculate counts
    for doc_id, class_label in doc_labels.items():
        doc_terms = set(index.doc_term_freqs.get(doc_id, {}).keys())
        
        for term in vocab:
            if term in doc_terms:
                N_t[term] += 1
                N_tc[term][class_label] += 1
    
    # Calculate MI scores for each term
    mi_scores = {}
    for term in vocab:
        mi = 0
        
        for c in classes:
            # N11: docs with term t and class c
            N11 = N_tc[term][c]
            # N10: docs with term t but not class c
            N10 = N_t[term] - N11
            # N01: docs without term t but with class c
            N01 = N_c[c] - N11
            # N00: docs without term t and not in class c
            N00 = N - N11 - N10 - N01
            
            # Skip if any count is zero (to avoid log(0))
            if N11 == 0 or N10 == 0 or N01 == 0 or N00 == 0:
                continue
            
            # Calculate each component of the MI formula
            part1 = (N11 / N) * math.log2((N * N11) / ((N11 + N10) * (N11 + N01)))
            part2 = (N01 / N) * math.log2((N * N01) / ((N01 + N00) * (N11 + N01)))
            part3 = (N10 / N) * math.log2((N * N10) / ((N11 + N10) * (N10 + N00)))
            part4 = (N00 / N) * math.log2((N * N00) / ((N10 + N00) * (N01 + N00)))
            
            mi += part1 + part2 + part3 + part4
        
        mi_scores[term] = mi
    
    # Select top k terms
    selected_terms = sorted(mi_scores.items(), key=lambda x: x[1], reverse=True)[:k]
    return {term for term, _ in selected_terms}

def chi_square(index, doc_labels, k=1000):
    # TODO: Implement Chi-Square feature selection
    classes = set(doc_labels.values())
    vocab = set(index.term_doc_freqs.keys())
    N = len(doc_labels)

    # Precompute doc counts per class
    N_c = {c: sum(1 for label in doc_labels.values() if label == c) for c in classes}

    # Precompute term counts per class
    chi2_scores = {}

    for term in vocab:
        term_docs = index.term_doc_freqs[term]
        N_t = len(term_docs)

        # Compute class-wise counts
        N_tc = defaultdict(int)
        for doc_id in term_docs:
            label = doc_labels.get(doc_id)
            if label is not None:
                N_tc[label] += 1

        max_score = 0
        for c in classes:
            N11 = N_tc[c]  # Term present in doc; doc is in class c
            N10 = N_t - N11  # Term present in doc; doc is NOT in class c
            N01 = N_c[c] - N11  # Term NOT present in doc; doc is in class c
            N00 = N - N11 - N10 - N01  # Term NOT present in doc; doc is NOT in class c

            numerator = (N11 * N00 - N10 * N01) ** 2 * N
            denominator = (N11 + N01) * (N10 + N00) * (N11 + N10) * (N01 + N00)

            if denominator > 0:
                score = numerator / denominator
                max_score = max(max_score, score)

        chi2_scores[term] = max_score

    selected_terms = sorted(chi2_scores.items(), key=lambda x: x[1], reverse=True)[:k]
    return {term for term, _ in selected_terms}

def frequency_based(index, doc_labels, k=1000):
    # TODO: Implement frequency-based feature selection
    class_term_counts = defaultdict(Counter)

    for doc_id, label in doc_labels.items():
        term_freqs = index.doc_term_freqs.get(doc_id, {})
        for term in term_freqs:
            class_term_counts[label][term] += 1

    combined_counter = Counter()
    for class_counter in class_term_counts.values():
        combined_counter.update(class_counter)

    most_common = combined_counter.most_common(k)
    return {term for term, _ in most_common}
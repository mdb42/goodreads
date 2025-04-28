# src/classifier/feature_selection.py
import math
import numpy as np
from collections import defaultdict, Counter

def mutual_information(index, doc_labels, k=1000):
    """
    Select top k features using Mutual Information.
    
    Args:
        index: Document index with term frequencies
        doc_labels: Dict mapping doc_ids to class labels
        k: Number of features to select
        
    Returns:
        Set of selected feature names
    """
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
    """
    Select top k features using Chi-Square test.
    
    [Implementation based on course materials]
    """
    # Similar implementation based on the chi-square formula from your slides
    pass

def frequency_based(index, doc_labels, k=1000):
    """
    Select top k features based on simple document frequency.
    
    [Implementation based on course materials]
    """
    # Simpler frequency-based selection
    pass
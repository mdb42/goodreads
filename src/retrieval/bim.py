# src/retrieval/bim.py
"""
CSC790 Information Retrieval - Final Project
Goodreads Sentiment Analysis and Information Retrieval System

Module: bim.py

This module implements the Binary Independence Model (BIM) for probabilistic
information retrieval. BIM ranks documents based on the probability of their
relevance to a query, using a binary representation of terms.

Authors:
    Matthew D. Branson (branson773@live.missouristate.edu)
    James R. Brown (brown926@live.missouristate.edu)

Missouri State University
Department of Computer Science
May 1, 2025
"""

import math
from typing import List, Tuple, Set, Dict
from contextlib import nullcontext

class RetrievalBIM:
    """
    Binary Independence Model for probabilistic information retrieval.
    
    This class implements the Binary Independence Model (BIM), which ranks
    documents based on their probability of relevance to a query. The model
    assumes term independence and uses binary term weights (present/absent)
    rather than frequency counts.
    
    The core of BIM is the Retrieval Status Value (RSV) scoring function,
    which estimates document relevance based on the probability theory.
    
    Attributes:
        index: An inverted index providing access to document terms
        profiler: Optional performance profiler for timing operations
        logger: Optional logger for status and error messages
        relevance_labels: Dictionary mapping filenames to relevance judgments
        doc_count: Total number of documents in the collection
    """

    def __init__(self, index, profiler=None, logger=None):
        """
        Initialize a BIM retrieval model.

        Args:
            index: An inverted index instance providing term access methods
            profiler: Optional profiler for performance monitoring
            logger: Optional logger instance for status messages
        """
        self.index = index
        self.profiler = profiler
        self.logger = logger
        self.relevance_labels: Dict[str, int] = {}
        self.doc_count = self.index.doc_count

        if self.logger:
            self.logger.info("[+] RetrievalBIM initialized.")

    def load_relevance_labels(self, filepath: str):
        """
        Load document relevance judgments from a file.
        
        These relevance judgments can be used for evaluation purposes,
        but are not required for the basic search functionality.
        
        The expected file format is CSV with two columns:
        document_id,relevance_value (0 or 1)

        Args:
            filepath (str): Path to the relevance judgments file
        """
        if self.logger:
            self.logger.info(f"[+] Loading relevance labels from {filepath}...")

        try:
            cleaned_path = filepath.strip().strip('"\'')
            with open(cleaned_path, 'r') as f:
                self.relevance_labels = {
                    line.split(',')[0].strip(): int(line.split(',')[1].strip())
                    for line in f if ',' in line
                }
            if self.logger:
                self.logger.info(f"[+] Loaded {len(self.relevance_labels):,} relevance labels.")
        except FileNotFoundError:
            if self.logger:
                self.logger.error(f"[X] Relevance labels file '{filepath}' not found.")
            self.relevance_labels = {}
        except Exception as e:
            if self.logger:
                self.logger.error(f"[X] Failed to load relevance labels: {e}")
            self.relevance_labels = {}

    def get_relevance_label(self, filename: str) -> int:
        """
        Get the relevance judgment for a document.
        
        Returns the known relevance judgment for the document if available,
        or "?" if unknown.

        Args:
            filename (str): Document filename

        Returns:
            int or str: Relevance judgment (0/1) or "?" if unknown
        """
        # Extract base ID by removing file extension
        normalized_filename = filename.rsplit('.', 1)[0]
        return self.relevance_labels.get(normalized_filename, "?")

    def search(self, query: str, k: int = 10) -> List[Tuple[str, float]]:
        """
        Search for documents relevant to the query.
        
        This method:
        1. Processes the query using the same pipeline as documents
        2. Identifies candidate documents containing query terms
        3. Computes relevance scores using the BIM model
        4. Returns the top-k results sorted by relevance score

        Args:
            query (str): Query text to search for
            k (int): Number of top results to return (default: 10)

        Returns:
            List[Tuple[str, float]]: List of (document_id, score) pairs
                sorted by descending relevance score
        """
        if self.logger:
            self.logger.info(f"[+] Starting BIM search for query: '{query}'")

        # Use profiler if available, otherwise use nullcontext
        with self.profiler.timer("BIM Search") if self.profiler else nullcontext():
            # Process query using the same pipeline as documents
            processed_query = self._process_query(query)
            if not processed_query:
                if self.logger:
                    self.logger.warning("[!] Query processing returned no valid terms.")
                return []

            # Get documents containing at least one query term
            candidate_docs = self._get_candidate_documents(processed_query)
            if not candidate_docs:
                if self.logger:
                    self.logger.warning("[!] No candidate documents found for query.")
                return []

            # Compute relevance scores for each candidate document
            document_scores = [
                (self.index.filenames[doc_id], self._compute_rsv(processed_query, doc_id))
                for doc_id in candidate_docs
            ]

            # Sort by score in descending order and take top-k
            sorted_results = sorted(document_scores, key=lambda x: x[1], reverse=True)[:k]

        if self.logger:
            self.logger.info(f"[+] BIM search complete. Top {len(sorted_results)} results retrieved.")

        return sorted_results

    def _compute_rsv(self, query_terms: List[str], doc_id: int) -> float:
        """
        Compute the Retrieval Status Value (RSV) score for a document.
        
        The RSV score represents the estimated probability of relevance
        for the document given the query terms.

        Args:
            query_terms (List[str]): Processed query terms
            doc_id (int): Internal document ID

        Returns:
            float: RSV score (higher is more relevant)
        """
        # Get the document's terms
        doc_terms = self.index.doc_term_freqs.get(doc_id, {})
        
        # Sum the weights for query terms present in the document
        return sum(
            self._calculate_term_weight(term)
            for term in query_terms if term in doc_terms
        )

    def _calculate_term_weight(self, term: str) -> float:
        """
        Calculate a term's weight contribution to the RSV score.
        
        This implements the core BIM weighting formula:
        w_t = log((p_t * (1 - u_t)) / ((1 - p_t) * u_t))
        
        Where:
        - p_t is the probability of term t in relevant documents
        - u_t is the probability of term t in non-relevant documents

        Args:
            term (str): Term to calculate weight for

        Returns:
            float: Term weight value
        """
        # Get document frequency (number of docs containing the term)
        df = len(self.index.term_doc_freqs.get(term, {}))
        # Total number of documents
        N = self.doc_count
        # Smoothing constants to avoid zero probabilities
        s, S = 0.5, 1.0  

        # Probability of term in relevant docs (with smoothing)
        p_t = s / S
        # Probability of term in non-relevant docs (with smoothing)
        u_t = (df + 0.5) / (N + 1)

        # Cap u_t to avoid division by zero or negative values
        if u_t >= 1.0:
            u_t = 0.9999

        # Calculate and return term weight
        return math.log10((p_t * (1 - u_t)) / ((1 - p_t) * u_t))

    def _get_candidate_documents(self, query_terms: List[str]) -> Set[int]:
        """
        Get documents containing at least one query term.
        
        This method efficiently identifies candidate documents by
        using the inverted index to find documents containing any
        of the query terms.

        Args:
            query_terms (List[str]): Processed query terms

        Returns:
            Set[int]: Set of candidate document IDs
        """
        # Use set comprehension with flattening to get unique document IDs
        return {
            doc_id
            for term in query_terms
            if term in self.index.term_doc_freqs
            for doc_id in self.index.term_doc_freqs[term].keys()
        }

    def _process_query(self, query: str) -> List[str]:
        """
        Preprocess the query using the same pipeline as documents.
        
        This ensures consistency between document and query representations.
        The processing typically includes tokenization, stopword removal,
        and stemming.

        Args:
            query (str): Raw query text

        Returns:
            List[str]: List of processed query terms
        """
        try:
            # Use the index's preprocessing method for consistency
            return self.index._preprocess_text(query)
        except Exception as e:
            if self.logger:
                self.logger.error(f"[X] Failed to process query: {e}")
            return []
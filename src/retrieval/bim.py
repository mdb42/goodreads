import math
from typing import List, Tuple, Set, Dict
from contextlib import nullcontext

class RetrievalBIM:
    """
    Binary Independence Model for probabilistic information retrieval.
    """

    def __init__(self, index, profiler=None, logger=None):
        """
        Initialize a BIM retrieval model.

        Args:
            index: An inverted index instance
            profiler: Optional profiler
            logger: Optional logger instance
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

        Args:
            filename (str): Document filename

        Returns:
            int or str: Relevance judgment (0/1) or "?" if unknown
        """
        normalized_filename = filename.rsplit('.', 1)[0]
        return self.relevance_labels.get(normalized_filename, "?")

    def search(self, query: str, k: int = 10) -> List[Tuple[str, float]]:
        """
        Search for documents relevant to the query.

        Args:
            query (str): Query text
            k (int): Number of results

        Returns:
            List[Tuple[str, float]]: (doc_id, score) pairs
        """
        if self.logger:
            self.logger.info(f"[+] Starting BIM search for query: '{query}'")

        with self.profiler.timer("BIM Search") if self.profiler else nullcontext():
            processed_query = self._process_query(query)
            if not processed_query:
                if self.logger:
                    self.logger.warning("[!] Query processing returned no valid terms.")
                return []

            candidate_docs = self._get_candidate_documents(processed_query)
            if not candidate_docs:
                if self.logger:
                    self.logger.warning("[!] No candidate documents found for query.")
                return []

            document_scores = [
                (self.index.filenames[doc_id], self._compute_rsv(processed_query, doc_id))
                for doc_id in candidate_docs
            ]

            sorted_results = sorted(document_scores, key=lambda x: x[1], reverse=True)[:k]

        if self.logger:
            self.logger.info(f"[+] BIM search complete. Top {len(sorted_results)} results retrieved.")

        return sorted_results

    def _compute_rsv(self, query_terms: List[str], doc_id: int) -> float:
        """
        Compute the RSV score for a document.

        Args:
            query_terms (List[str]): Query terms
            doc_id (int): Internal document ID

        Returns:
            float: RSV score
        """
        doc_terms = self.index.doc_term_freqs.get(doc_id, {})
        return sum(
            self._calculate_term_weight(term)
            for term in query_terms if term in doc_terms
        )

    def _calculate_term_weight(self, term: str) -> float:
        """
        Calculate a term's contribution to RSV.

        Args:
            term (str): Term

        Returns:
            float: Weight
        """
        df = len(self.index.term_doc_freqs.get(term, {}))
        N = self.doc_count
        s, S = 0.5, 1.0  # smoothing

        p_t = s / S
        u_t = (df + 0.5) / (N + 1)

        if u_t >= 1.0:
            u_t = 0.9999

        return math.log10((p_t * (1 - u_t)) / ((1 - p_t) * u_t))

    def _get_candidate_documents(self, query_terms: List[str]) -> Set[int]:
        """
        Get documents containing at least one query term.

        Args:
            query_terms (List[str])

        Returns:
            Set[int]: Candidate document IDs
        """
        return {
            doc_id
            for term in query_terms
            if term in self.index.term_doc_freqs
            for doc_id in self.index.term_doc_freqs[term].keys()
        }

    def _process_query(self, query: str) -> List[str]:
        """
        Preprocess the query.

        Args:
            query (str)

        Returns:
            List[str]: Processed terms
        """
        try:
            return self.index._preprocess_text(query)
        except Exception as e:
            if self.logger:
                self.logger.error(f"[X] Failed to process query: {e}")
            return []

# src/index/standard_index.py

import os
import re
import json
import pickle
from sys import getsizeof
from collections import defaultdict, Counter
from threading import Lock
from typing import List, Tuple, Dict, Any, Optional

import nltk
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize

from src.index.base import BaseIndex

class StandardIndex(BaseIndex):
    def __init__(self, documents_dir=None, stopwords_file=None, special_chars_file=None, profiler=None, logger=None):
        super().__init__(documents_dir, stopwords_file, special_chars_file, profiler)

        self.logger = logger or self._default_logger()
        self.stopwords = self._load_stopwords(stopwords_file)
        self.special_chars = self._load_special_chars(special_chars_file)

        self.special_chars_pattern = None
        if self.special_chars:
            self.special_chars_pattern = re.compile(f'[{re.escape("".join(self.special_chars))}]')

        try:
            nltk.data.find('tokenizers/punkt')
        except LookupError:
            nltk.download('punkt', quiet=True)

        self.stemmer = PorterStemmer()
        self.term_doc_freqs = defaultdict(dict)
        self.doc_term_freqs = defaultdict(dict)
        self.filenames = {}
        self._doc_count = 0
        self._lock = Lock()

    def _default_logger(self):
        class SimpleLogger:
            def info(self, msg): print(msg)
            def warning(self, msg): print(msg)
            def error(self, msg): print(msg)
        return SimpleLogger()

    def _load_stopwords(self, filepath):
        if not filepath:
            return set()
        try:
            with open(filepath, encoding="utf-8") as file:
                return {line.strip().lower() for line in file if line.strip()}
        except Exception as e:
            self.logger.warning(f"[!] Failed to load stopwords: {e}")
            return set()

    def _load_special_chars(self, filepath):
        if not filepath:
            return set()
        try:
            with open(filepath, encoding="utf-8") as file:
                return {line.strip() for line in file if line.strip()}
        except Exception as e:
            self.logger.warning(f"[!] Failed to load special characters: {e}")
            return set()

    def _preprocess_text(self, text: str) -> List[str]:
        tokens = word_tokenize(text.lower())
        if self.special_chars_pattern:
            tokens = [self.special_chars_pattern.sub('', t) for t in tokens]
        tokens = [t for t in tokens if t.isalpha() and t not in self.stopwords]
        tokens = [self.stemmer.stem(t) for t in tokens]
        return tokens

    def _process_single_file(self, filepath: str) -> Optional[Tuple[str, Dict[str, int]]]:
        try:
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                text = f.read().strip()
            if not text:
                return None
            tokens = self._preprocess_text(text)
            if not tokens:
                return None
            filename = os.path.basename(filepath)
            return filename, Counter(tokens)
        except Exception as e:
            self.logger.error(f"[!] Error processing file {filepath}: {e}")
            return None

    def build_index(self):
        if not self.documents_dir:
            raise ValueError("[!] Cannot build index: documents_dir not specified.")

        self.logger.info(f"[+] Building index from directory: {self.documents_dir}")

        filepaths = [entry.path for entry in os.scandir(self.documents_dir)
                     if entry.is_file() and entry.name.endswith('.txt')]

        results = [self._process_single_file(fp) for fp in filepaths]
        results = [r for r in results if r]

        for filename, term_freqs in results:
            self.add_document(term_freqs, filename)

        self._vocab_size = len(self.term_doc_freqs)
        self.logger.info(f"[+] Index build complete. Documents processed: {len(self.doc_term_freqs)}")
        return self

    def add_document(self, term_freqs: dict, filename: str = None) -> int:
        with self._lock:
            doc_id = self._doc_count
            self.doc_term_freqs[doc_id] = term_freqs
            if filename:
                self.filenames[doc_id] = filename
            for term, freq in term_freqs.items():
                self.term_doc_freqs[term][doc_id] = freq
            self._doc_count += 1
            return doc_id

    def save(self, filepath: str) -> None:
        with open(filepath, 'wb') as f:
            pickle.dump({
                'term_doc_freqs': dict(self.term_doc_freqs),
                'doc_term_freqs': dict(self.doc_term_freqs),
                'filenames': self.filenames,
                'vocab_size': self._vocab_size
            }, f)
        self.logger.info(f"[+] Index successfully saved to {filepath}")

    @classmethod
    def load(cls, filepath: str, logger=None):
        if logger is None:
            raise ValueError("Logger is required to load StandardIndex.")
        index = cls(logger=logger)
        with open(filepath, 'rb') as f:
            data = pickle.load(f)
            index.term_doc_freqs = defaultdict(dict, data['term_doc_freqs'])
            index.doc_term_freqs = defaultdict(dict, data['doc_term_freqs'])
            index.filenames = data['filenames']
            index._doc_count = len(index.doc_term_freqs)
            index._vocab_size = data['vocab_size']
        logger.info(f"[+] Index loaded from {filepath}")
        return index

    def get_document_lengths(self):
        """
        Get the length of each document and the average document length.
        
        Returns:
            Tuple[Dict[int, int], float]: Document lengths dictionary and average length
        """
        doc_lengths = {
            doc_id: sum(term_freqs.values())
            for doc_id, term_freqs in self.doc_term_freqs.items()
        }
        
        avg_length = sum(doc_lengths.values()) / max(len(doc_lengths), 1)
        
        return doc_lengths, avg_length

    def get_term_freq(self, term: str, doc_id: int) -> int:
        """
        Get the frequency of a term in a specific document.
        
        Args:
            term (str): The term to look up
            doc_id (int): The document ID
            
        Returns:
            int: The frequency of the term in the document, or 0 if not found
        """
        return self.doc_term_freqs.get(doc_id, {}).get(term, 0)

    def get_doc_freq(self, term: str) -> int:
        """
        Get the document frequency of a term (number of documents containing the term).
        
        Args:
            term (str): The term to look up
            
        Returns:
            int: The number of documents containing the term
        """
        return len(self.term_doc_freqs.get(term, {}))
    
    def get_most_frequent_terms(self, n: int = 10) -> List[Tuple[str, int]]:
        """
        Get the most frequent terms in the collection.
        
        Args:
            n (int, optional): Number of top terms to return. Default is 10.
            
        Returns:
            List[Tuple[str, int]]: List of (term, frequency) pairs for the top n terms,
                sorted by decreasing frequency
                
        Note:
            Uses efficient heap-based selection to avoid sorting the entire vocabulary.
        """
        import heapq
        
        # Calculate total frequency of each term across all documents
        term_totals = {}
        for term, doc_freqs in self.term_doc_freqs.items():
            term_totals[term] = sum(doc_freqs.values())

        # Use heapq.nlargest for efficient top-n selection (O(n log k) complexity)
        return heapq.nlargest(n, term_totals.items(), key=lambda x: x[1])

    def get_memory_usage(self) -> Dict[str, int]:
        """
        Calculate the memory usage of different components of the index.
        
        Returns:
            Dict[str, int]: Dictionary with memory usage information in bytes for:
                - Term-Doc Index: Size of the inverted index
                - Doc-Term Index: Size of the forward index
                - Filenames: Size of the filenames list
                - Total Memory Usage: Sum of the above components
                - Pickled Size: Size of the serialized index
        """
        def sizeof_iterative(obj):
            """Calculate size of complex objects iteratively to avoid recursion limits."""
            seen = set()
            to_process = [obj]
            total_size = 0
            
            while to_process:
                current = to_process.pop()
                if id(current) in seen:
                    continue
                    
                seen.add(id(current))
                total_size += getsizeof(current)
                
                # Add contained objects to processing queue
                if isinstance(current, dict):
                    to_process.extend(current.keys())
                    to_process.extend(current.values())
                elif isinstance(current, (list, tuple, set)):
                    to_process.extend(current)
                    
            return total_size
        
        # Calculate sizes of main components
        term_doc_size = sizeof_iterative(self.term_doc_freqs)
        doc_term_size = sizeof_iterative(self.doc_term_freqs)
        filenames_size = sizeof_iterative(self.filenames)
        total_size = term_doc_size + doc_term_size + filenames_size
        
        # Calculate serialized size for comparison
        sample_data = {
            'term_doc_freqs': dict(self.term_doc_freqs),
            'doc_term_freqs': dict(self.doc_term_freqs),
            'filenames': self.filenames
        }
        pickled_size = len(pickle.dumps(sample_data))
        
        return {
            "Term-Doc Index": term_doc_size,
            "Doc-Term Index": doc_term_size,
            "Filenames": filenames_size,
            "Total Memory Usage": total_size,
            "Pickled Size": pickled_size
        }
    
    def export_json(self, filepath: str = None) -> Optional[str]:
        """
        Export the index to a JSON format for inspection or troubleshooting.
        
        Args:
            filepath (str, optional): Path where the JSON file should be saved.
                If None, the JSON string is returned instead of saving to a file.
                
        Returns:
            Optional[str]: JSON string representation of the index if filepath is None,
                otherwise None
        """
        # Prepare export data - convert defaultdicts to regular dicts for serialization
        export_data = {
            "term_doc_freqs": {term: dict(docs) for term, docs in self.term_doc_freqs.items()},
            "document_count": self.doc_count,
            "vocabulary_size": self.vocab_size,
            "top_terms": self.get_most_frequent_terms(20),
            "filenames": self.filenames
        }
        
        # Convert to JSON string with pretty formatting
        json_str = json.dumps(export_data, indent=2)
        
        # Save to file if filepath is provided
        if filepath:
            if not filepath.endswith(".json"):
                filepath += ".json"
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(json_str)
            return None
        else:
            return json_str
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive statistics about the index.
        
        Returns:
            Dict[str, Any]: Dictionary containing various statistics about the index
        """
        # Calculate document lengths (number of terms in each document)
        doc_lengths = [sum(terms.values()) for terms in self.doc_term_freqs.values()]
        
        # Calculate average document length
        avg_doc_length = sum(doc_lengths) / max(1, len(doc_lengths))
        
        # Calculate term frequency statistics
        term_counts = []
        for term, docs in self.term_doc_freqs.items():
            term_counts.append(sum(docs.values()))
        
        if term_counts:
            avg_term_freq = sum(term_counts) / len(term_counts)
            max_term_freq = max(term_counts)
            min_term_freq = min(term_counts)
        else:
            avg_term_freq = max_term_freq = min_term_freq = 0
        
        # Calculate document frequency statistics
        doc_freqs = [len(docs) for docs in self.term_doc_freqs.values()]
        
        if doc_freqs:
            avg_doc_freq = sum(doc_freqs) / len(doc_freqs)
            max_doc_freq = max(doc_freqs)
            min_doc_freq = min(doc_freqs)
        else:
            avg_doc_freq = max_doc_freq = min_doc_freq = 0
        
        # Update document count and vocabulary size
        self._vocab_size = len(self.term_doc_freqs)

        # Return comprehensive statistics
        return {
            "document_count": self.doc_count,
            "vocabulary_size": self.vocab_size,
            "avg_doc_length": avg_doc_length,
            "max_doc_length": max(doc_lengths) if doc_lengths else 0,
            "min_doc_length": min(doc_lengths) if doc_lengths else 0,
            "avg_term_freq": avg_term_freq,
            "max_term_freq": max_term_freq,
            "min_term_freq": min_term_freq,
            "avg_doc_freq": avg_doc_freq,
            "max_doc_freq": max_doc_freq,
            "min_doc_freq": min_doc_freq,
            "memory_usage": self.get_memory_usage()
        }
    
    @property
    def doc_count(self):
        return self._doc_count

    @doc_count.setter
    def doc_count(self, value):
        self._doc_count = value

    @property
    def vocab_size(self):
        if hasattr(self, '_vocab_size'):
            return self._vocab_size
        else:
            return len(self.term_doc_freqs)

    @vocab_size.setter
    def vocab_size(self, value):
        self._vocab_size = value

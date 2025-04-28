# src/index/parallel_zip_index.py
"""
CSC790 Information Retrieval - Final Project
Goodreads Sentiment Analysis and Information Retrieval System

Module: parallel_zip_index.py

This module implements a parallel indexing solution for compressed document collections.
It processes documents directly from zip archives using multiple CPU cores, avoiding
the need to extract files to disk and enabling efficient processing of large corpora
like the Goodreads dataset.

Authors:
    Matthew D. Branson (branson773@live.missouristate.edu)
    James R. Brown (brown926@live.missouristate.edu)

Missouri State University
Department of Computer Science
May 1, 2025
"""

import pickle
import zipfile
import re
from pathlib import Path
from multiprocessing import Pool, cpu_count
from collections import Counter
from typing import List, Tuple, Dict

from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize

from src.index import StandardIndex
from src.utils import ZipCorpusReader

class ParallelZipIndex(StandardIndex):
    """
    Parallel implementation of a document index built from compressed .zip corpora.
    
    This class extends StandardIndex to process documents directly from zip archives
    using parallel processing. It avoids filesystem explosion by never extracting all
    files to disk, instead streaming document content directly from the compressed
    archive and distributing work across multiple CPU cores.
    
    The indexing process works by:
    1. Reading document names from the zip archive
    2. Splitting documents into chunks for parallel processing
    3. Distributing chunks to worker processes
    4. Processing documents within each chunk (tokenization, stemming, etc.)
    5. Merging results into a unified index
    
    Attributes:
        corpus_reader (ZipCorpusReader): Reader for accessing zip archives
        num_workers (int): Number of parallel worker processes to use
        chunk_size (int): Number of documents to process in each batch
        logger: Logger for status and error messages
        profiler: Optional performance profiler
    """
    def __init__(self, documents_zip_path, num_workers=None, chunk_size=None, logger=None, profiler=None):
        """
        Initialize the parallel zip index.
        
        Args:
            documents_zip_path (str): Path to the zip archive containing documents
            num_workers (int, optional): Number of parallel workers to use.
                Defaults to half of available CPU cores.
            chunk_size (int, optional): Number of documents to process in each batch.
                If None, automatically calculated based on document count and workers.
            logger: Logger for status and error messages
            profiler: Optional performance profiler for timing operations
            
        Raises:
            ValueError: If logger is not provided
            FileNotFoundError: If the zip file doesn't exist
        """
        if not logger:
            raise ValueError("Logger is required for ParallelZipIndex.")
            
        # Initialize with no documents_dir since we're using a zip archive instead
        super().__init__(documents_dir=None)
        
        # Initialize corpus reader and parallel processing parameters
        self.corpus_reader = ZipCorpusReader(documents_zip_path)
        self.num_workers = num_workers or max(1, cpu_count() // 2)
        self.chunk_size = chunk_size
        self.logger = logger
        self.profiler = profiler
        self._doc_count = 0
        self._vocab_size = 0

    def build_index(self):
        """
        Build the index by parallel processing documents from the zip archive.
        
        This method:
        1. Lists all documents in the zip archive
        2. Divides documents into chunks for parallel processing
        3. Processes chunks in parallel using a multiprocessing pool
        4. Merges results into the unified index
        
        The progress is displayed during indexing, and timing statistics
        are collected if a profiler is provided.
        """
        self.logger.info(f"[+] Starting index build from compressed corpus with {self.num_workers} workers...")

        # Get list of all documents in the zip archive
        documents = self.corpus_reader.list_documents()
        total_docs = len(documents)
        self.logger.info(f"[+] Found {total_docs:,} documents to index.")
        self.logger.info(f"[+] Using {self.num_workers} workers for parallel processing.")
        
        # Calculate appropriate chunk size if not specified
        if self.chunk_size is None:
            self.chunk_size = max(1000, total_docs // (self.num_workers * 4))

        self.logger.info(f"[+] Chunk size set to {self.chunk_size:,} documents per batch.")
        self.logger.info(f"[+] Total batches to process: {total_docs // self.chunk_size + (1 if total_docs % self.chunk_size > 0 else 0):,}")
        self.logger.info(f"[+] Beginning chunked processing...") 
        
        # Split documents into chunks for parallel processing
        chunks = [documents[i:i + self.chunk_size] for i in range(0, total_docs, self.chunk_size)]
        args_list = [(self.corpus_reader.zip_path, chunk) for chunk in chunks]

        results = []
        
        def process_batches():
            """Process document batches in parallel and collect results."""
            with Pool(processes=self.num_workers) as pool:
                for idx, batch_result in enumerate(pool.imap_unordered(self._worker_process_batch, args_list), 1):
                    results.extend(batch_result)
                    percent = (idx * self.chunk_size) / total_docs * 100
                    print(f"\rProgress: {min(idx * self.chunk_size, total_docs):,} / {total_docs:,} ({percent:.2f}%)", end="")
                print("\n")

        # Process batches with timing if profiler is available
        if self.profiler:
            with self.profiler.timer("Parallel Indexing"):
                process_batches()
        else:
            process_batches()
        
        # Merge results into the unified index
        self.logger.info(f"[+] Merging {len(results):,} term frequency results into index...")
        for doc_name, term_freqs in results:
            self.add_document(term_freqs, doc_name)

        self.logger.info(f"[+] Index build complete. Total documents processed: {total_docs:,}")


    @staticmethod
    def _worker_process_batch(args: Tuple[Path, List[str]]) -> List[Tuple[str, Dict[str, int]]]:
        """
        Process a batch of documents in a worker process.
        
        This static method runs in separate processes to parallelize document processing.
        It opens the zip file, reads each document, tokenizes and stems the content,
        and returns term frequency counts.
        
        Args:
            args (Tuple[Path, List[str]]): Tuple containing:
                - Path to the zip archive
                - List of document names to process
                
        Returns:
            List[Tuple[str, Dict[str, int]]]: List of tuples containing:
                - Document name
                - Dictionary mapping terms to their frequencies
                
        Note:
            Exceptions during processing of individual documents are caught
            and those documents are skipped to ensure batch processing continues.
        """
        zip_path, doc_names = args
        batch_results = []

        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                for doc_name in doc_names:
                    try:
                        # Read document from zip archive
                        text = ZipCorpusReader.read_document(zf, doc_name)
                        if not text.strip():
                            continue
                            
                        # Process document text (tokenize, stem, etc.)
                        stemmer = PorterStemmer()
                        tokens = word_tokenize(text.lower())
                        tokens = [re.sub(r'[^a-zA-Z]', '', t) for t in tokens]
                        tokens = [t for t in tokens if t and len(t) > 1]
                        tokens = [stemmer.stem(t) for t in tokens]
                        
                        if tokens:
                            batch_results.append((doc_name, Counter(tokens)))
                    except Exception:
                        # Skip documents that cause errors
                        continue
        except Exception as e:
            from src.utils.logger import get_logger
            logger = get_logger()
            logger.warning(f"[!] Batch processing failure: {e}")

        return batch_results

    def save(self, filepath: str):
        """
        Save the index to a file.
        
        Args:
            filepath (str): Path where the index should be saved
        """
        self.logger.info(f"[+] Preparing to save index to {filepath}...")

        # Prepare data for serialization
        index_data = {
            'doc_term_freqs': self.doc_term_freqs,
            'term_doc_freqs': self.term_doc_freqs,
            'doc_count': self.doc_count,
            'vocab_size': self.vocab_size,
            'filenames': self.filenames,
            'source_zip': str(self.corpus_reader.zip_path)
        }
        
        try:
            with open(filepath, 'wb') as f:
                pickle.dump(index_data, f)
            self.logger.info(f"[+] Index successfully saved to {filepath}")
        except Exception as e:
            self.logger.error(f"[!] Failed to save index: {e}")

    @classmethod
    def load(cls, filepath: str, logger=None):
        """
        Load an index from a file.
        
        Args:
            filepath (str): Path to the saved index file
            logger: Logger for status and error messages
            
        Returns:
            ParallelZipIndex: The loaded index instance
            
        Raises:
            ValueError: If logger is not provided
            Exception: If loading fails
        """
        if not logger:
            raise ValueError("Logger is required to load ParallelZipIndex.")

        try:
            with open(filepath, 'rb') as f:
                index_data = pickle.load(f)

            # Create new instance and restore state
            instance = cls(documents_zip_path=index_data.get('source_zip', ''), logger=logger)
            instance.doc_term_freqs = index_data['doc_term_freqs']
            instance.term_doc_freqs = index_data['term_doc_freqs']
            instance.doc_count = index_data['doc_count']
            instance.vocab_size = index_data['vocab_size']
            instance.filenames = index_data.get('filenames', {})

            logger.info(f"[+] Index loaded from {filepath}")
            return instance
        except Exception as e:
            logger.error(f"[!] Failed to load index: {e}")
            raise
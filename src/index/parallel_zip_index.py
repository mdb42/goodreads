# src/index/parallel_zip_index.py

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
    """
    def __init__(self, documents_zip_path, num_workers=None, chunk_size=None, logger=None, profiler=None):
        if not logger:
            raise ValueError("Logger is required for ParallelZipIndex.")
        super().__init__(documents_dir=None)
        self.corpus_reader = ZipCorpusReader(documents_zip_path)
        self.num_workers = num_workers or max(1, cpu_count() // 2)
        self.chunk_size = chunk_size
        self.logger = logger
        self.profiler = profiler
        self._doc_count = 0
        self._vocab_size = 0

    def build_index(self):
        self.logger.info(f"[+] Starting index build from compressed corpus with {self.num_workers} workers...")

        documents = self.corpus_reader.list_documents()
        total_docs = len(documents)
        self.logger.info(f"[+] Found {total_docs:,} documents to index.")
        self.logger.info(f"[+] Using {self.num_workers} workers for parallel processing.")
        
        if self.chunk_size is None:
            self.chunk_size = max(1000, total_docs // (self.num_workers * 4))

        self.logger.info(f"[+] Chunk size set to {self.chunk_size:,} documents per batch.")
        self.logger.info(f"[+] Total batches to process: {total_docs // self.chunk_size + (1 if total_docs % self.chunk_size > 0 else 0):,}")
        self.logger.info(f"[+] Beginning chunked processing...") 
        chunks = [documents[i:i + self.chunk_size] for i in range(0, total_docs, self.chunk_size)]
        args_list = [(self.corpus_reader.zip_path, chunk) for chunk in chunks]

        results = []
        
        def process_batches():
            with Pool(processes=self.num_workers) as pool:
                for idx, batch_result in enumerate(pool.imap_unordered(self._worker_process_batch, args_list), 1):
                    results.extend(batch_result)
                    percent = (idx * self.chunk_size) / total_docs * 100
                    print(f"\rProgress: {min(idx * self.chunk_size, total_docs):,} / {total_docs:,} ({percent:.2f}%)", end="")
                print("\n")

        if self.profiler:
            with self.profiler.timer("Parallel Indexing"):
                process_batches()
        else:
            process_batches()
        
        self.logger.info(f"[+] Merging {len(results):,} term frequency results into index...")
        for doc_name, term_freqs in results:
            self.add_document(term_freqs, doc_name)

        self.logger.info(f"[+] Index build complete. Total documents processed: {total_docs:,}")


    @staticmethod
    def _worker_process_batch(args: Tuple[Path, List[str]]) -> List[Tuple[str, Dict[str, int]]]:
        zip_path, doc_names = args
        batch_results = []

        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                for doc_name in doc_names:
                    try:
                        text = ZipCorpusReader.read_document(zf, doc_name)
                        if not text.strip():
                            continue
                        stemmer = PorterStemmer()
                        tokens = word_tokenize(text.lower())
                        tokens = [re.sub(r'[^a-zA-Z]', '', t) for t in tokens]
                        tokens = [t for t in tokens if t and len(t) > 1]
                        tokens = [stemmer.stem(t) for t in tokens]
                        if tokens:
                            batch_results.append((doc_name, Counter(tokens)))
                    except Exception:
                        continue
        except Exception as e:
            from src.utils.logger import get_logger
            logger = get_logger()
            logger.warning(f"[!] Batch processing failure: {e}")

        return batch_results

    def save(self, filepath: str):
        self.logger.info(f"[+] Preparing to save index to {filepath}...")

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
        if not logger:
            raise ValueError("Logger is required to load ParallelZipIndex.")

        try:
            with open(filepath, 'rb') as f:
                index_data = pickle.load(f)

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

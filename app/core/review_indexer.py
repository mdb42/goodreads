# app/core/review_indexer.py

from collections import Counter, defaultdict
import nltk
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize
import math
import re

class ReviewIndex:
    """
    Specialized inverted index for Goodreads reviews.
    
    This index builds on the StandardIndex pattern but is optimized for
    database-stored review text rather than files.
    """
    
    def __init__(self, db_connection, stopwords_file=None, special_chars_file=None):
        """Initialize the review index with a database connection."""
        self.db = db_connection
        
        # Load stopwords and special characters
        self.stopwords = self._load_stopwords(stopwords_file)
        self.special_chars = self._load_special_chars(special_chars_file)
        
        # Compile regex pattern once for efficiency
        self.special_chars_pattern = None
        if self.special_chars:
            self.special_chars_pattern = re.compile(f'[{re.escape("".join(self.special_chars))}]')
        
        # Ensure NLTK data is available
        try:
            nltk.data.find('tokenizers/punkt')
        except LookupError:
            nltk.download('punkt', quiet=True)
        
        self.stemmer = PorterStemmer()
        
        # Initialize index data structures
        self.term_doc_freqs = defaultdict(dict)  # term -> {doc_id -> freq}
        self.doc_term_freqs = defaultdict(dict)  # doc_id -> {term -> freq}
        self.review_metadata = {}  # doc_id -> review metadata
        self._doc_count = 0
    
    def _load_stopwords(self, filepath):
        """Load stopwords from a file."""
        if not filepath:
            return set()
        try:
            with open(filepath, encoding="utf-8") as file:
                return {line.strip().lower() for line in file if line.strip()}
        except Exception as e:
            print(f"Warning: Failed to load stopwords. {e}")
            return set()

    def _load_special_chars(self, filepath):
        """Load special characters from a file."""
        if not filepath:
            return set()
        try:
            with open(filepath, encoding="utf-8") as file:
                return {line.strip() for line in file if line.strip()}
        except Exception as e:
            print(f"Warning: Failed to load special characters. {e}")
            return set()
    
    def _preprocess_text(self, text):
        """Preprocess text by tokenizing, removing special characters, and stemming."""
        if not text:
            return []
            
        # Tokenize and convert to lowercase
        tokens = word_tokenize(text.lower())

        # Remove special characters using pre-compiled regex
        if self.special_chars_pattern:
            tokens = [self.special_chars_pattern.sub('', t) for t in tokens]

        # Filter alphabetic tokens and remove stopwords
        tokens = [t for t in tokens if t.isalpha() and t not in self.stopwords]
        
        # Apply stemming
        tokens = [self.stemmer.stem(t) for t in tokens]
        return tokens
    
    def build_index_from_db(self, limit=None, filter_conditions=None):
        """
        Build the index by processing reviews from the database.
        
        Args:
            limit (int, optional): Maximum number of reviews to index
            filter_conditions (str, optional): SQL WHERE clause for filtering reviews
        """
        # Build query with optional filtering
        query = "SELECT id, review_id, book_id, user_id, rating, review_text FROM review"
        if filter_conditions:
            query += f" WHERE {filter_conditions}"
        if limit:
            query += f" LIMIT {limit}"
        
        # Execute query and process reviews
        cursor = self.db.execute(query)
        
        progress_count = 0
        for row in cursor.fetchall():
            db_id, review_id, book_id, user_id, rating, text = row
            if not text:
                continue
                
            # Process review text
            tokens = self._preprocess_text(text)
            if not tokens:
                continue
                
            # Count term frequencies
            term_counts = Counter(tokens)
            
            # Add to index and store metadata
            doc_id = self.add_document(term_counts, f"review_{review_id}")
            self.review_metadata[doc_id] = {
                "db_id": db_id,
                "review_id": review_id,
                "book_id": book_id,
                "user_id": user_id,
                "rating": rating
            }
            
            progress_count += 1
            if progress_count % 1000 == 0:
                print(f"Indexed {progress_count} reviews...")
        
        print(f"Completed indexing {progress_count} reviews")
        return self
    
    def add_document(self, term_freqs, doc_id_str=None):
        """
        Add a processed document to the index.
        
        Args:
            term_freqs (dict): Dictionary of {term: frequency} for the document
            doc_id_str (str, optional): String identifier for the document
            
        Returns:
            int: The assigned document ID
        """
        # Assign document ID and store term frequencies
        doc_id = self._doc_count
        self.doc_term_freqs[doc_id] = term_freqs
        
        # Store reference to avoid repeated lookups
        term_doc_freqs = self.term_doc_freqs
        
        # Update inverted index (term -> doc)
        for term, freq in term_freqs.items():
            term_doc_freqs[term][doc_id] = freq
        
        # Increment document counter
        self._doc_count += 1
        return doc_id
    
    def compute_tfidf(self, doc_id, term):
        """
        Compute TF-IDF score for a term in a document.
        
        Args:
            doc_id (int): Document ID
            term (str): Term to compute score for
            
        Returns:
            float: TF-IDF score
        """
        # Term frequency in document
        tf = self.doc_term_freqs.get(doc_id, {}).get(term, 0)
        if tf == 0:
            return 0.0
        
        # Document frequency (number of documents containing the term)
        df = len(self.term_doc_freqs.get(term, {}))
        if df == 0:
            return 0.0
        
        # Calculate IDF with smoothing to prevent division by zero
        idf = math.log((self._doc_count + 1) / (df + 1)) + 1
        
        # Sublinear TF scaling (reduces the weight of high-frequency terms)
        tf = 1 + math.log(tf)
        
        return tf * idf
    
    def get_document_vector(self, doc_id):
        """
        Get the TF-IDF vector for a document.
        
        Args:
            doc_id (int): Document ID
            
        Returns:
            dict: Dictionary mapping terms to TF-IDF scores
        """
        if doc_id not in self.doc_term_freqs:
            return {}
        
        vector = {}
        for term in self.doc_term_freqs[doc_id]:
            vector[term] = self.compute_tfidf(doc_id, term)
        
        return vector
    
    def compute_similarity(self, doc_id1, doc_id2):
        """
        Compute cosine similarity between two documents.
        
        Args:
            doc_id1 (int): First document ID
            doc_id2 (int): Second document ID
            
        Returns:
            float: Cosine similarity score (0-1)
        """
        vec1 = self.get_document_vector(doc_id1)
        vec2 = self.get_document_vector(doc_id2)
        
        # Find common terms
        common_terms = set(vec1.keys()) & set(vec2.keys())
        
        # Calculate dot product for common terms
        dot_product = sum(vec1[term] * vec2[term] for term in common_terms)
        
        # Calculate magnitudes
        mag1 = math.sqrt(sum(score ** 2 for score in vec1.values()))
        mag2 = math.sqrt(sum(score ** 2 for score in vec2.values()))
        
        # Avoid division by zero
        if mag1 == 0 or mag2 == 0:
            return 0.0
        
        return dot_product / (mag1 * mag2)
    
    def find_similar_reviews(self, doc_id, top_n=10):
        """
        Find the most similar reviews to a given document.
        
        Args:
            doc_id (int): Document ID to find similar reviews for
            top_n (int): Number of similar reviews to return
            
        Returns:
            list: List of (doc_id, similarity_score) tuples
        """
        if doc_id not in self.doc_term_freqs:
            return []
        
        similarities = []
        for other_id in self.doc_term_freqs:
            if other_id != doc_id:
                sim = self.compute_similarity(doc_id, other_id)
                similarities.append((other_id, sim))
        
        # Sort by similarity (descending)
        similarities.sort(key=lambda x: x[1], reverse=True)
        
        # Return top N
        return similarities[:top_n]
    
    @property
    def doc_count(self):
        """Get the number of documents in the index."""
        return self._doc_count
    
    @property
    def vocab_size(self):
        """Get the size of the vocabulary."""
        return len(self.term_doc_freqs)
    
    def get_statistics(self):
        """
        Get comprehensive statistics about the index.
        
        Returns:
            dict: Dictionary containing various statistics
        """
        # User statistics
        user_counts = {}
        for doc_id, metadata in self.review_metadata.items():
            user_id = metadata.get("user_id")
            if user_id:
                user_counts[user_id] = user_counts.get(user_id, 0) + 1
        
        # Calculate average term frequency across all documents
        all_freqs = []
        for term, docs in self.term_doc_freqs.items():
            for doc_id, freq in docs.items():
                all_freqs.append(freq)
        
        avg_term_freq = sum(all_freqs) / max(1, len(all_freqs))
        
        return {
            "document_count": self._doc_count,
            "vocabulary_size": self.vocab_size,
            "unique_users": len(user_counts),
            "avg_reviews_per_user": sum(user_counts.values()) / max(1, len(user_counts)),
            "avg_term_freq": avg_term_freq,
            "avg_terms_per_review": sum(len(terms) for terms in self.doc_term_freqs.values()) / max(1, self._doc_count)
        }
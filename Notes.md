```python
import math
import zipfile
import pandas as pd
from collections import defaultdict, Counter
from sklearn.model_selection import KFold
from sklearn.feature_extraction.text import CountVectorizer

class Classifier(BaseClassifier):
    def train(self, k_folds: int = 5, **kwargs) -> Dict[str, float]:
        self.logger.info(f"[+] Training classifier with {k_folds}-fold cross-validation")

        # Step 1: Load metadata
        df = pd.read_csv(self.metadata_path)
        df = df[df['rating'].between(1, 5)]  # Only use ratings 1–5

        # Step 2: Extract text from ZIP archive (based on metadata IDs)
        with zipfile.ZipFile(self.zip_path, 'r') as archive:
            def read_text(row):
                try:
                    with archive.open(row['review_path']) as f:
                        return f.read().decode('utf-8')
                except:
                    return ""
            df["text"] = df.apply(read_text, axis=1)

        # Step 3: Set up output structures
        self.vocabulary = set()
        self.class_priors = defaultdict(float)
        self.cond_probs = defaultdict(lambda: defaultdict(float))  # cond_probs[t][c]

        # Step 4: Concatenate text by class
        class_docs = defaultdict(list)
        for _, row in df.iterrows():
            class_docs[row["rating"]].append(row["text"])

        # Step 5: Build vocabulary
        all_text = [" ".join(texts) for texts in class_docs.values()]
        vectorizer = CountVectorizer()
        vectorizer.fit(all_text)
        self.vocabulary = vectorizer.get_feature_names_out()

        # Step 6: Count class priors and conditional term probabilities
        total_docs = len(df)
        for c in class_docs:
            docs = class_docs[c]
            self.class_priors[c] = len(docs) / total_docs

            # Get token counts for this class
            term_counts = Counter()
            for doc in docs:
                tokens = vectorizer.build_tokenizer()(doc.lower())
                term_counts.update(tokens)

            total_term_count = sum(term_counts.values())
            for t in self.vocabulary:
                T_ct = term_counts[t]
                self.cond_probs[t][c] = (T_ct + 1) / (total_term_count + len(self.vocabulary))  # Laplace smoothing

        self.logger.info("[+] Model trained with vocabulary size: %d", len(self.vocabulary))
        self.models = [("vocab", self.vocabulary), ("priors", self.class_priors), ("cond_probs", self.cond_probs)]
        return {"accuracy": 0.0, "precision": 0.0, "recall": 0.0, "f1": 0.0}  # Placeholder for now
```

---


## Benchmarks

| **Field** | **goodreads _120k** | **goodreads _full** |
|-----------|--------------------|---------------------|
| **Total documents** | 119,997 | 1,260,776 |
| **Vocabulary size** | 121,520 | 540,135 |
| **Average doc length (terms)** | 197.02 | 196.74 |
| **Max doc length** | 2,975 | 3,417 |
| **Min doc length** | 3 | 1 |
| **Avg term frequency** | 194.55 | 459.22 |
| **Avg document frequency** | 106.46 | 251.31 |
| **Term-Doc index size** | 533.20 MB | 5.50 GB |
| **Doc-Term index size** | 914.86 MB | 9.36 GB |
| **Filenames size** | 17.02 MB | 178.27 MB |
| **Total memory usage** | 1.43 GB | 15.04 GB |
| **Pickled size** | 198.57 MB | 2.17 GB |
| **Indexing time** | 64.69 s | 715.58 s |
| **Saving index time** | 4.44 s | 87.92 s |
| **BIM search time** | 0.137 s | 1.727 s |
| **Single query time** | 0.168 s | 1.779 s |
| **Tracked operations total** | 69.43 s | 807.01 s |



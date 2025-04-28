import os
import zipfile
import pandas as pd
from datetime import datetime
from src.utils import ZipCorpusReader

def run_search_session(retrieval_model, profiler, logger, metadata_path, zip_path):
    logger.info("[+] Starting search phase...")

    outputs_dir = "outputs"
    os.makedirs(outputs_dir, exist_ok=True)

    # Load metadata
    try:
        metadata_df = pd.read_csv(metadata_path)
        required_columns = ['review_id', 'user_id', 'rating']
        missing = [col for col in required_columns if col not in metadata_df.columns]
        if missing:
            raise ValueError(f"Metadata file missing columns: {missing}")

        metadata = {
            row['review_id']: {
                'user': row['user_id'],
                'rating': row['rating']
            }
            for _, row in metadata_df.iterrows()
        }
        logger.info(f"[+] Loaded metadata with {len(metadata):,} entries.")
    except Exception as e:
        logger.error(f"[X] Failed to load metadata: {e}")
        metadata = {}

    corpus_reader = ZipCorpusReader(zip_path)

    query_counter = 1

    while True:
        query = input("\n[?] Enter search query (or type 'exit' to quit): ").strip()
        
        if not query:
            logger.warning("[!] Empty query entered. Please try again.")
            continue
        if query.lower() == 'exit':
            logger.info("[+] Exiting search session.")
            break

        logger.info(f"[+] Searching for query: '{query}'")
        with profiler.timer("Single Query"):
            results = retrieval_model.search(query, k=10)

        if not results:
            logger.warning("[!] No results found.")
            continue

        logger.info("[+] Top Results:")

        # Prepare output
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        output_lines = [
            f"# Search Results\n\n",
            f"**Query Time:** {timestamp}\n\n",
            f"**Query:** `{query}`\n\n",
            f"---\n\n",
        ]

        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                for rank, (doc_id, score) in enumerate(results, 1):
                    review_text = corpus_reader.read_document(zf, doc_id)

                    base_id = doc_id.rsplit('.', 1)[0]
                    meta = metadata.get(base_id, {"user": "Unknown", "rating": "?"})
                    user = meta.get('user', "Unknown")
                    rating = meta.get('rating', "?")

                    if isinstance(rating, (int, float, str)) and str(rating).isdigit():
                        rating = int(rating)
                        stars = "★" * rating + "☆" * (5 - rating)
                    else:
                        stars = "N/A"

                    output_lines.append(
                        f"## Result {rank}\n\n"
                        f"**Filename:** `{doc_id}`\n\n"
                        f"**Score:** `{score:.4f}`\n\n"
                        f"**User ID:** `{user}`\n\n"
                        f"**Rating:** `{stars}` ({rating} stars)\n\n"
                        f"**Predicted Rating:** *Pending*\n\n"
                        f"**Review:**\n\n"
                        f"> {review_text.strip().replace('\n', '\n> ')}\n\n"
                        f"---\n\n"
                    )

                    logger.info(f"{rank}. {doc_id} (Score: {score:.4f})")

        except Exception as e:
            logger.error(f"[X] Failed reading compressed documents: {e}")
            continue

        output_filename = os.path.join(outputs_dir, f"search_{query_counter:03d}.md")
        try:
            with open(output_filename, "w", encoding="utf-8") as f:
                f.writelines(output_lines)
            logger.info(f"[+] Saved search results to {output_filename}")
        except Exception as e:
            logger.error(f"[X] Failed to save search results: {e}")

        query_counter += 1

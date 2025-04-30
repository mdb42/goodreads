# src/retrieval/search.py
"""
CSC790 Information Retrieval - Final Project
Goodreads Sentiment Analysis and Information Retrieval System

Module: search.py

This module provides an interactive search session interface for the Goodreads
review corpus. It processes user queries, retrieves relevant documents using
the BIM model, and presents results with metadata from the original reviews.

Authors:
    Matthew D. Branson (branson773@live.missouristate.edu)
    James R. Brown (brown926@live.missouristate.edu)

Missouri State University
Department of Computer Science
May 1, 2025
"""

import os
import zipfile
import pandas as pd
from datetime import datetime
from src.utils import ZipCorpusReader

def run_search_session(retrieval_model, profiler, logger, metadata_path, zip_path):
    """
    Run an interactive search session for the Goodreads review corpus.
    
    This function provides a command-line interface for searching the corpus.
    It processes user queries, retrieves relevant documents using the provided
    retrieval model, and presents results with metadata (user ID, rating) from
    the original reviews. Results are displayed in the terminal and also saved
    as markdown files for later reference.
    
    Args:
        retrieval_model: The retrieval model to use for searches (e.g., RetrievalBIM)
        profiler: Performance profiler for timing operations
        logger: Logger for status and error messages
        metadata_path (str): Path to the CSV file containing review metadata
        zip_path (str): Path to the ZIP archive containing the review documents
    """
    logger.info("[+] Starting search phase...")

    # Create directory for saving search results
    outputs_dir = "outputs"
    os.makedirs(outputs_dir, exist_ok=True)

    # Load metadata from CSV file
    try:
        # Read metadata CSV into pandas DataFrame
        metadata_df = pd.read_csv(metadata_path)
        # Verify required columns are present
        required_columns = ['review_id', 'user_id', 'rating']
        missing = [col for col in required_columns if col not in metadata_df.columns]
        if missing:
            raise ValueError(f"Metadata file missing columns: {missing}")

        # Convert DataFrame to dictionary for efficient lookup
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

    # Initialize corpus reader for accessing review text
    corpus_reader = ZipCorpusReader(zip_path)

    # Counter for naming exported result files
    query_counter = 1

    # Interactive search loop
    while True:
        # Get query from user
        query = input("\n[?] Enter search query (or type 'exit' to quit): ").strip()
        
        # Handle empty queries
        if not query:
            logger.warning("[!] Empty query entered. Please try again.")
            continue
        # Check for exit command
        if query.lower() == 'exit':
            logger.info("[+] Exiting search session.")
            break

        # Log query and time the search operation
        logger.info(f"[+] Searching for query: '{query}'")
        with profiler.timer("Single Query"):
            results = retrieval_model.search(query, k=10)

        # Handle no results case
        if not results:
            logger.warning("[!] No results found.")
            continue

        logger.info("[+] Top Results:")

        # Prepare output markdown format
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        output_lines = [
            f"# Search Results\n\n",
            f"**Query Time:** {timestamp}\n\n",
            f"**Query:** `{query}`\n\n",
            f"---\n\n",
        ]

        try:
            # Open ZIP archive to access review text
            with zipfile.ZipFile(zip_path, 'r') as zf:
                # Process each result
                for rank, (doc_id, score) in enumerate(results, 1):
                    # Read the original review text
                    review_text = corpus_reader.read_document(zf, doc_id)

                    # Extract review ID from filename (remove extension)
                    base_id = doc_id.rsplit('.', 1)[0]
                    # Look up metadata for this review
                    meta = metadata.get(base_id, {"user": "Unknown", "rating": "?"})
                    user = meta.get('user', "Unknown")
                    rating = meta.get('rating', "?")

                    # Format rating as stars
                    if isinstance(rating, (int, float, str)) and str(rating).isdigit():
                        rating = int(rating)
                        stars = "★" * rating + "☆" * (5 - rating)
                    else:
                        stars = "N/A"

                    # Fix by precomputing the replaced text
                    quoted_text = review_text.strip().replace('\n', '\n> ')
                    output_lines.append(
                        f"## Result {rank}\n\n"
                        f"**Filename:** `{doc_id}`\n\n"
                        f"**Score:** `{score:.4f}`\n\n"
                        f"**User ID:** `{user}`\n\n"
                        f"**Rating:** `{stars}` ({rating} stars)\n\n"
                        f"**Predicted Rating:** *Pending*\n\n"
                        f"**Review:**\n\n"
                        f"> {quoted_text}\n\n"
                        f"---\n\n"
                    )


                    # Log basic result to console
                    logger.info(f"{rank}. {doc_id} (Score: {score:.4f})")

        except Exception as e:
            logger.error(f"[X] Failed reading compressed documents: {e}")
            continue

        # Save results to markdown file
        output_filename = os.path.join(outputs_dir, f"search_{query_counter:03d}.md")
        try:
            with open(output_filename, "w", encoding="utf-8") as f:
                f.writelines(output_lines)
            logger.info(f"[+] Saved search results to {output_filename}")
        except Exception as e:
            logger.error(f"[X] Failed to save search results: {e}")

        # Increment counter for next search
        query_counter += 1
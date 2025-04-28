# main.py
"""
CSC790 Information Retrieval - Final Project
Goodreads Sentiment Analysis and Information Retrieval System

This module serves as the main entry point for the Goodreads IR system.
It handles command-line arguments, system initialization, and orchestrates
the execution of indexing, retrieval, and analysis components.

Authors:
    Matthew D. Branson (branson773@live.missouristate.edu)
    James R. Brown (brown926@live.missouristate.edu)

Missouri State University
Department of Computer Science
May 1, 2025
"""

import os
import argparse

from src.index import ParallelZipIndex
from src.retrieval import RetrievalBIM, run_search_session
from src.utils import (
    setup_logger,
    display_banner,
    load_config,
    ensure_directories_exist,
    display_memory_usage,
    download_if_missing,
    display_detailed_statistics,
    Profiler
)

def parse_arguments():
    """
    Parse command-line arguments and load configuration.
    
    Returns:
        dict: Configuration dictionary with updated values from command-line arguments.
    """
    parser = argparse.ArgumentParser(description="Goodreads IR System")
    parser.add_argument('--dataset', type=str, default="goodreads_120k", 
                        help="Dataset name (e.g., goodreads_120k, goodreads_full)")
    parser.add_argument('--setup', type=str, choices=['default', 'import', 'build'], default="default", 
                        help="Setup strategy: default (use local if available), import (download existing), build (create new)")
    parser.add_argument('--config', type=str, default='config.json', 
                        help="Path to configuration file")
    args = parser.parse_args()

    # Load and update configuration with command-line parameters
    config = load_config(args.config)
    config['selected_dataset'] = args.dataset
    config['setup_mode'] = args.setup
    return config

def main():
    """
    Main execution function for the Goodreads IR system.
    
    This function:
    1. Initializes logging and system configuration
    2. Sets up the dataset based on the specified mode
    3. Builds or loads the index
    4. Executes analysis phases (search, classification, clustering)
    5. Generates performance reports
    """
    # Initialize logging and display welcome banner
    logger = setup_logger()
    logger.info("Logger initialized.")
    display_banner()

    # Parse arguments and ensure directory structure
    config = parse_arguments()
    ensure_directories_exist()

    logger.info(f"Selected Dataset: {config['selected_dataset']}")
    logger.info(f"Setup Mode: {config['setup_mode']}")

    # Initialize performance profiling
    profiler = Profiler()
    profiler.start_global_timer()

    # Extract paths from configuration
    dataset_info = config["available_datasets"][config["selected_dataset"]]
    zip_path = dataset_info["local_zip"]
    index_path = dataset_info["local_index"]
    metadata_path = dataset_info["local_metadata"]

    index = None
    logger.info(f"[+] Downloading dataset files if missing...")
    download_if_missing(zip_path, dataset_info["zip_url"])
    download_if_missing(metadata_path, dataset_info["metadata_url"])

    try:
        # Handle different setup modes
        if config["setup_mode"] == "import":
            # Import mode: Download and load pre-built index
            logger.info("[+] Importing existing index...")
            download_if_missing(index_path, dataset_info["index_url"])
            with profiler.timer("Loading Index"):
                if not os.path.exists(index_path):
                    raise FileNotFoundError(f"[!] Index file not found at {index_path}.")
                logger.info(f"[+] Loading index from {index_path}...")
                index = ParallelZipIndex.load(index_path, logger=logger)

        elif config["setup_mode"] == "build":
            # Build mode: Create new index from scratch
            logger.info("[+] Building index from scratch...")
            index = ParallelZipIndex(zip_path, logger=logger)
            with profiler.timer("Indexing"):
                index.build_index()
            with profiler.timer("Saving Index"):
                index.save(index_path)

        elif config["setup_mode"] == "default":
            # Default mode: Use existing index if available, otherwise build
            logger.info("[+] Default setup mode selected.")
            if os.path.exists(index_path):
                # Load existing index file
                logger.info("[+] Loading existing index...")
                with profiler.timer("Loading Index"):
                    if not os.path.exists(index_path):
                        raise FileNotFoundError(f"[!] Index file not found at {index_path}.")
                    logger.info(f"[+] Loading index from {index_path}...")
                    index = ParallelZipIndex.load(index_path, logger=logger)
            else:
                # Try to download pre-built index
                logger.warning("[!] No index found. Attempting download...")
                download_if_missing(index_path, dataset_info["index_url"])
                if os.path.exists(index_path):
                    with profiler.timer("Loading Index"):
                        logger.info(f"[+] Loading index from {index_path}...")
                        index = ParallelZipIndex.load(index_path, logger=logger)
                else:
                    # Build index as last resort
                    logger.warning("[!] No prepared index available. Building manually...")
                    index = ParallelZipIndex(zip_path, logger=logger)
                    with profiler.timer("Indexing"):
                        index.build_index()
                    with profiler.timer("Saving Index"):
                        index.save(index_path)

    except Exception as e:
        logger.error(f"[X] Fatal error during setup: {e}")
        raise

    # Display index statistics and memory usage
    logger.info(f"[+] Preparing statistics and memory usage reports...")
    if index:
        display_detailed_statistics(index)
        display_memory_usage(index)

    # Classification Phase (Placeholder)
    logger.info("=== Phase: Review Classification ===")
    logger.info("Coming soon...")
    """
    REPORT EXCERPT: To construct the sentiment analyzer, we will be using a Naive Bayes classifier to make
    decisions on the sentiment "rating" of a review. This classifier will be fed information from
    a feature extraction pipeline using tf-idf over tokenized and stemmed reviews. This feature
    extraction method helps determine how "important" a word is to any given review. From these,
    we are looking to predict a star rating on a discrete 1-5 scale, matching Goodreads' review
    system.
    """

    # Clustering Phase (Placeholder)
    logger.info("=== Phase: User Clustering ===")
    logger.info("Coming soon...")
    """
    REPORT EXCERPT: After this, we will cluster reviewers based on their aggregate sentiment scores, 
    review frequency, and other behavioral metrics.
    """

    # Cross-Domain Analysis Phase (Placeholder)
    logger.info("=== Phase: Cross-Domain Analysis ===")
    logger.info("Maybe coming soon???")
    """
    REPORT EXCERPT: Once the model has been trained on the Goodreads dataset, we will proceed with applying
    the model to a set of movie reviews and examine how effective the model is in cross-domain
    applications.
    """

    # Search Phase (Implemented)
    logger.info("=== Phase: Search Reviews ===")
    retrieval = RetrievalBIM(index, profiler=profiler, logger=logger)
    run_search_session(retrieval, profiler, logger, metadata_path, zip_path)

    # Finalize and report profiling results
    profiler.end_global_timer()
    report = profiler.generate_report()
    logger.info(report)
    logger.info("[+] Execution Complete.")

if __name__ == "__main__":
    main()
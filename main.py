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
    parser = argparse.ArgumentParser(description="Goodreads IR System")
    parser.add_argument('--dataset', type=str, default="goodreads_120k", help="Dataset name (e.g., goodreads_120k)")
    parser.add_argument('--setup', type=str, choices=['default', 'import', 'build'], default="default", help="Setup strategy")
    parser.add_argument('--config', type=str, default='config.json', help="Path to configuration file")
    args = parser.parse_args()

    config = load_config(args.config)
    config['selected_dataset'] = args.dataset
    config['setup_mode'] = args.setup
    return config

def main():
    logger = setup_logger()
    logger.info("Logger initialized.")
    display_banner()

    config = parse_arguments()
    ensure_directories_exist()

    logger.info(f"Selected Dataset: {config['selected_dataset']}")
    logger.info(f"Setup Mode: {config['setup_mode']}")

    profiler = Profiler()
    profiler.start_global_timer()

    dataset_info = config["available_datasets"][config["selected_dataset"]]
    zip_path = dataset_info["local_zip"]
    index_path = dataset_info["local_index"]
    metadata_path = dataset_info["local_metadata"]

    index = None
    logger.info(f"[+] Downloading dataset files if missing...")
    download_if_missing(zip_path, dataset_info["zip_url"])
    download_if_missing(metadata_path, dataset_info["metadata_url"])

    try:
        if config["setup_mode"] == "import":
            logger.info("[+] Importing existing index...")
            download_if_missing(index_path, dataset_info["index_url"])
            with profiler.timer("Loading Index"):
                if not os.path.exists(index_path):
                    raise FileNotFoundError(f"[!] Index file not found at {index_path}.")
                logger.info(f"[+] Loading index from {index_path}...")
                index = ParallelZipIndex.load(index_path, logger=logger)

        elif config["setup_mode"] == "build":
            logger.info("[+] Building index from scratch...")
            index = ParallelZipIndex(zip_path, logger=logger)
            with profiler.timer("Indexing"):
                index.build_index()
            with profiler.timer("Saving Index"):
                index.save(index_path)

        elif config["setup_mode"] == "default":
            logger.info("[+] Default setup mode selected.")
            if os.path.exists(index_path):
                logger.info("[+] Loading existing index...")
                with profiler.timer("Loading Index"):
                    if not os.path.exists(index_path):
                        raise FileNotFoundError(f"[!] Index file not found at {index_path}.")
                    logger.info(f"[+] Loading index from {index_path}...")
                    index = ParallelZipIndex.load(index_path, logger=logger)
            else:
                logger.warning("[!] No index found. Attempting download...")
                download_if_missing(index_path, dataset_info["index_url"])
                if os.path.exists(index_path):
                    with profiler.timer("Loading Index"):
                        logger.info(f"[+] Loading index from {index_path}...")
                        index = ParallelZipIndex.load(index_path, logger=logger)
                else:
                    logger.warning("[!] No prepared index available. Building manually...")
                    index = ParallelZipIndex(zip_path, logger=logger)
                    with profiler.timer("Indexing"):
                        index.build_index()
                    with profiler.timer("Saving Index"):
                        index.save(index_path)

    except Exception as e:
        logger.error(f"[X] Fatal error during setup: {e}")
        raise

    logger.info(f"[+] Preparing statistics and memory usage reports...")
    if index:
        display_detailed_statistics(index)
        display_memory_usage(index)

    # Future Phases
    logger.info("=== Phase: Classification ===")
    logger.info("Coming soon...")

    logger.info("=== Phase: Clustering ===")
    logger.info("Coming soon...")

    # Search Phase
    logger.info("=== Phase: Search Reviews ===")
    retrieval = RetrievalBIM(index, profiler=profiler, logger=logger)
    run_search_session(retrieval, profiler, logger, metadata_path, zip_path)


    profiler.end_global_timer()

    report = profiler.generate_report()
    logger.info(report)
    logger.info("[+] Execution Complete.")

if __name__ == "__main__":
    main()

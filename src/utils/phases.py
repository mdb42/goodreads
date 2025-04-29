#!/usr/bin/env python3
"""
CSC790 Information Retrieval - Final Project
Goodreads Sentiment Analysis and Information Retrieval System

This module provides functions for running the different phases of the system:
search, classification, clustering, and cross-domain analysis. Each phase is
implemented as a separate function that can be called from the main script.

Authors:
    Matthew D. Branson (branson773@live.missouristate.edu)
    James R. Brown (brown926@live.missouristate.edu)

Missouri State University
Department of Computer Science
May 1, 2025
"""

import os

def run_classification_phase(index, profiler, logger, metadata_path, zip_path, config):
    """
    Workflow
    --------
    1. Try to load cached k-fold models from ``models/{dataset}``.
    2. If none (or retrain forced) → train, cross-validate, and save.
    3. Log evaluation metrics.
    4. If ``interactive_classification`` is True then open CLI tester.

    Args
    ----
    index: Search index (not used directly here but kept for symmetry).
    profiler: Timer/aggregator for performance stats.
    logger: Standard logger.
    metadata_path: CSV with review ratings.
    zip_path: ZIP archive of review texts.
    config: Global app settings.
    """
    logger.info("=== Phase: Review Classification ===")

    try:
        # Defer import so the app starts even if the module is missing.
        from src.classification import Classifier

        dataset_name = config["selected_dataset"]
        models_dir = os.path.join("models", dataset_name)
        os.makedirs(models_dir, exist_ok=True)

        # ----- Load or train -------------------------------------------------
        if os.listdir(models_dir) and config.get("use_existing_model", True):
            logger.info(f"[+] Loading classifier from {models_dir}")
            with profiler.timer("Load Classifier"):
                classifier = Classifier.load(zip_path, metadata_path, models_dir,
                                             logger, profiler)
        else:
            logger.info("[+] Training classifier …")
            with profiler.timer("Train Classifier"):
                classifier = Classifier(zip_path, metadata_path, models_dir,
                                         logger, profiler)
                metrics = classifier.train(k_folds=5)
                classifier.save()

            logger.info("[+] Training metrics:")
            for m, v in metrics.items():
                logger.info(f"  {m}: {v:.4f}")

        # ----- Evaluate ------------------------------------------------------
        if getattr(classifier, "models", None):
            logger.info("[+] Evaluating …")
            with profiler.timer("Evaluate Classifier"):
                eval_metrics = classifier.evaluate()
                for m, v in eval_metrics.items():
                    logger.info(f"  {m}: {v:.4f}")

            # Interactive mode for ad-hoc rating prediction
            if config.get("interactive_classification", False):
                run_interactive_classification(classifier, logger)
        else:
            logger.warning("[!] No trained models found")

    except ImportError:
        logger.info("[!] Classification module missing – skipped")
    except Exception as e:
        logger.error(f"[X] Classification phase failed: {e}")


def run_interactive_classification(classifier, logger):
    """Quick CLI for ad‑hoc rating prediction."""
    logger.info("[+] Interactive mode.  Type review text or 'exit'.")

    while True:
        txt = input("\n[?] Review: ").strip()
        if not txt:
            logger.warning("[!] Empty input – try again.")
            continue
        if txt.lower() == "exit":
            logger.info("[+] Leaving interactive mode.")
            break
        rating = classifier.predict(txt)
        stars = "★" * rating + "☆" * (5 - rating)
        logger.info(f"[+] Predicted: {stars} ({rating} stars)")


def run_clustering_phase(index, profiler, logger, metadata_path, config):
    """
    Segment reviewers with K‑means and summarise results.

    Steps
    -----
    * Load existing clusters unless ``recluster`` is True.
    * Otherwise extract features, cluster, save, and analyse.
    * Always emit basic stats and a PNG visualisation.
    """
    logger.info("=== Phase: User Clustering ===")

    try:
        from src.clustering.kmeans import KMeansClusterer
        dataset = config["selected_dataset"]
        out_dir = os.path.join("clusters", dataset)
        os.makedirs(out_dir, exist_ok=True)

        # ----- Load or fit ---------------------------------------------------
        cfg_path = os.path.join(out_dir, "config.json")
        if os.path.exists(cfg_path) and not config.get("recluster", False):
            logger.info(f"[+] Loading clusters from {out_dir}")
            with profiler.timer("Load Clusters"):
                clusterer = KMeansClusterer.load(index, metadata_path, out_dir,
                                                 logger, profiler)
        else:
            logger.info("[+] Building clusters ...")
            with profiler.timer("Init Clusterer"):
                clusterer = KMeansClusterer(index, metadata_path, out_dir,
                                             logger, profiler,
                                             k=config.get("num_clusters", 5))
            with profiler.timer("Extract Features"):
                feats = clusterer.extract_user_features()
                logger.info(f"[+] Features for {len(feats)} users")
            with profiler.timer("K-means"):
                clust = clusterer.cluster()
                logger.info(f"[+] Formed {len(clust)} clusters")
            with profiler.timer("Save Clusters"):
                clusterer.save()

        # ----- Analyse & visualise -----------------------------------------
        with profiler.timer("Analyse Clusters"):
            stats = clusterer.analyze_clusters()
            for cid, s in stats.items():
                logger.info(f"  Cluster {cid}: {s['size']} users - "
                            f"{clusterer.get_cluster_description(cid)}")
        with profiler.timer("Visualise"):
            img = os.path.join(out_dir, "visualization.png")
            clusterer.visualize(filepath=img)
            logger.info(f"[+] Plot saved → {img}")

    except ImportError:
        logger.info("[!] Clustering module missing - skipped")
    except Exception as e:
        logger.error(f"[X] Clustering phase failed: {e}")


def run_cross_domain_phase(index, profiler, logger, config):
    """Placeholder for future cross-domain transfer tests."""
    logger.info("=== Phase: Cross-Domain Analysis ===\n[+] Coming soon... Maaaaybe?")


def run_search_phase(index, profiler, logger, metadata_path, zip_path, config):
    """
    Interactive BM25/Okapi search over the corpus.

    Loads :class:`RetrievalBIM`, then launches a query loop handled by
    ``run_search_session``.
    """
    logger.info("=== Phase: Search Reviews ===")

    try:
        from src.retrieval import RetrievalBIM, run_search_session
        retrieval = RetrievalBIM(index, profiler=profiler, logger=logger)
        run_search_session(retrieval, profiler, logger, metadata_path, zip_path)
    except ImportError:
        logger.error("[!] Retrieval module missing – skipped")
    except Exception as e:
        logger.error(f"[X] Search phase failed: {e}")

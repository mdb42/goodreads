# src/utils/display.py
"""
CSC790 Information Retrieval - Final Project
Goodreads Sentiment Analysis and Information Retrieval System

Module: display.py

This module provides display utilities for showing system information,
index statistics, and memory usage in a user-friendly format.

Authors:
    Matthew D. Branson (branson773@live.missouristate.edu)
    James R. Brown (brown926@live.missouristate.edu)

Missouri State University
Department of Computer Science
May 1, 2025
"""

# --- Banner and Environment ---
def display_banner():
    """
    Display a welcome banner with project and author information.
    
    This function prints a formatted banner that includes the project name,
    authors, and date to provide a professional introduction when the
    system starts.
    """
    print("=" * 60)
    print("=" * 15 + " Goodreads Sentiment Analysis " + "=" * 15)
    print("Authors: Matthew Branson, James Brown")
    print("Date: April 25, 2025")
    print("=" * 60)

def display_index_statistics(index):
    """
    Display basic statistics about the index.
    
    Args:
        index: An index object with a get_statistics method
    """
    stats = index.get_statistics()
    print("\n=== Index Statistics ===")
    for key, value in stats.items():
        print(f"{key}: {value}")

def display_memory_usage(index):
    """
    Print the memory footprint of the loaded index.
    
    This function formats and displays the memory usage of different
    components of the index in human-readable units (KB, MB, GB).
    
    Args:
        index: An index object with a get_memory_usage method
    """
    mem_stats = index.get_memory_usage()
    if isinstance(mem_stats, dict):
        print("\n=== Index Memory Usage Breakdown ===")
        for key, bytes_val in mem_stats.items():
            print(f"{key}: {format_memory_size(bytes_val)}")
    else:
        print("\n=== Index Memory Usage ===")
        print(f"Total: {format_memory_size(mem_stats)}")

def display_vocabulary_statistics(index):
    """
    Display statistics about the index vocabulary.
    
    This function shows the vocabulary size and lists the top 10
    most frequent terms in the corpus with their frequencies.
    
    Args:
        index: An index object with vocab_size and get_most_frequent_terms methods
    """
    print(f"\nVocabulary Size: {index.vocab_size}")
    print("Top 10 Frequent Terms:")
    for i, (term, freq) in enumerate(index.get_most_frequent_terms(n=10), 1):
        print(f"  {i}. {term} ({freq:,})")

def display_detailed_statistics(index):
    """
    Display comprehensive statistics about the index.
    
    This function provides detailed information about the index, including
    document counts, vocabulary size, document length statistics, term
    frequency statistics, and memory usage.
    
    Args:
        index: An index object with a get_statistics method
    """
    stats = index.get_statistics()
    print("\n=== Detailed Index Statistics ===")
    print(f"Total Documents: {stats['document_count']:,}")
    print(f"Vocabulary Size: {stats['vocabulary_size']:,}")
    print(f"Average Doc Length: {stats['avg_doc_length']:.2f} terms")
    print(f"Max Doc Length: {stats['max_doc_length']:,}")
    print(f"Min Doc Length: {stats['min_doc_length']:,}")
    print(f"Avg Term Frequency: {stats['avg_term_freq']:.2f}")
    print(f"Avg Document Frequency: {stats['avg_doc_freq']:.2f}")
    print("\n=== Memory Usage ===")
    for key, value in stats['memory_usage'].items():
        print(f"{key}: {format_memory_size(value)}")


def format_memory_size(value: int) -> str:
    """
    Format a byte count into a human-readable size string.
    
    This function converts a raw byte count into a string with appropriate
    units (bytes, KB, MB, GB) for easier human comprehension.
    
    Args:
        value (int): Size in bytes
        
    Returns:
        str: Formatted size string with appropriate units
    """
    if value > 1024 ** 3:
        return f"{value / (1024 ** 3):.2f} GB"
    elif value > 1024 ** 2:
        return f"{value / (1024 ** 2):.2f} MB"
    elif value > 1024:
        return f"{value / 1024:.2f} KB"
    else:
        return f"{value:,} bytes"
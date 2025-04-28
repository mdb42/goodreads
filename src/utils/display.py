
# --- Banner and Environment ---
def display_banner():
    print("=" * 60)
    print("=" * 15 + " Goodreads Sentiment Analysis " + "=" * 15)
    print("Authors: Matthew Branson, James Brown")
    print("Date: April 25, 2025")
    print("=" * 60)

def display_index_statistics(index):
    stats = index.get_statistics()
    print("\n=== Index Statistics ===")
    for key, value in stats.items():
        print(f"{key}: {value}")

def display_memory_usage(index):
    """Print the memory footprint of the loaded index."""
    mem_stats = index.get_memory_usage()
    if isinstance(mem_stats, dict):
        print("\n=== Index Memory Usage Breakdown ===")
        for key, bytes_val in mem_stats.items():
            print(f"{key}: {format_memory_size(bytes_val)}")
    else:
        print("\n=== Index Memory Usage ===")
        print(f"Total: {format_memory_size(mem_stats)}")

def display_vocabulary_statistics(index):
    print(f"\nVocabulary Size: {index.vocab_size}")
    print("Top 10 Frequent Terms:")
    for i, (term, freq) in enumerate(index.get_most_frequent_terms(n=10), 1):
        print(f"  {i}. {term} ({freq:,})")

def display_detailed_statistics(index):
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
    if value > 1024 ** 3:
        return f"{value / (1024 ** 3):.2f} GB"
    elif value > 1024 ** 2:
        return f"{value / (1024 ** 2):.2f} MB"
    elif value > 1024:
        return f"{value / 1024:.2f} KB"
    else:
        return f"{value:,} bytes"

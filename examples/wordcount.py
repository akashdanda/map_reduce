"""
Word Count - The "Hello World" of MapReduce
Counts how many times each word appears in text files.
"""

import sys
import os
import time

# Add parent directory to path so we can import core
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.mapreduce import MapReduce, read_file_chunks


def map_function(text_chunk: str) -> list:
    """
    Map function: Takes a chunk of text, returns (word, 1) for each word.
    
    Input: "hello world hello"
    Output: [("hello", 1), ("world", 1), ("hello", 1)]
    """
    words = text_chunk.lower().split()
    return [(word.strip('.,!?;:'), 1) for word in words if word.strip('.,!?;:')]


def reduce_function(word: str, counts: list) -> tuple:
    """
    Reduce function: Takes a word and list of counts, returns total count.
    
    Input: ("hello", [1, 1, 1])
    Output: ("hello", 3)
    """
    return (word, sum(counts))


def main():
    print("=== Word Count with MapReduce ===\n")
    
    # Check if input file provided
    if len(sys.argv) < 2:
        print("Usage: python wordcount.py <input_file>")
        print("\nLet me create a sample file for you...")
        create_sample_file()
        input_file = "data/sample.txt"
    else:
        input_file = sys.argv[1]
    
    # Read input file into chunks
    print(f"Reading {input_file}...")
    chunks = read_file_chunks(input_file, chunk_size=500)
    print(f"Split into {len(chunks)} chunks\n")
    
    # Create MapReduce instance
    mr = MapReduce(map_func=map_function, reduce_func=reduce_function)
    
    # Run MapReduce
    start_time = time.time()
    results = mr.run(chunks)
    duration = time.time() - start_time
    
    # Sort by count (descending)
    results.sort(key=lambda x: x[1], reverse=True)
    
    # Display results
    print("\n=== RESULTS ===")
    print(f"Total unique words: {len(results)}")
    print(f"Processing time: {duration:.2f} seconds\n")
    print("Top 20 most frequent words:")
    for word, count in results[:20]:
        print(f"  {word:20s} {count:>6d}")
    
    # Save full results
    output_file = "data/wordcount_results.txt"
    with open(output_file, 'w') as f:
        for word, count in results:
            f.write(f"{word}\t{count}\n")
    print(f"\nFull results saved to {output_file}")


def create_sample_file():
    """Create a sample text file for testing"""
    os.makedirs("data", exist_ok=True)
    
    sample_text = """
    The quick brown fox jumps over the lazy dog.
    The dog was not amused by the fox.
    The fox was very quick and very brown.
    MapReduce is a programming model for processing large data sets.
    MapReduce was developed by Google for distributed computing.
    The quick fox and the lazy dog met again.
    """ * 100  # Repeat to make it bigger
    
    with open("data/sample.txt", 'w') as f:
        f.write(sample_text)
    
    print("Created data/sample.txt\n")


if __name__ == '__main__':
    main()
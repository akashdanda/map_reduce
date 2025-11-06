"""
Benchmark single-machine vs distributed processing
"""
import time
import os
from core.mapreduce import MapReduce, read_file_chunks

# Create larger test file
print("Creating 10MB test file...")
os.makedirs('data', exist_ok=True)
with open('data/benchmark.txt', 'w') as f:
    text = "the quick brown fox jumps over the lazy dog " * 100
    for i in range(10000):  # ~10MB
        f.write(text + f" iteration {i}\n")

# Single-machine version
def map_func(text):
    words = text.lower().split()
    return [(word.strip('.,!?;:'), 1) for word in words if word.strip('.,!?;:')]

def reduce_func(word, counts):
    return (word, sum(counts))

print("\n=== Single Machine ===")
chunks = read_file_chunks('data/benchmark.txt', chunk_size=1000)
mr = MapReduce(map_func, reduce_func)
start = time.time()
results = mr.run(chunks)
single_time = time.time() - start
print(f"Time: {single_time:.2f}s")

print(f"\n=== Distributed (3 workers) ===")
print("Now run: python core/master.py data/benchmark.txt")
print("And observe the time difference!")
print(f"\nExpected speedup: ~2-3x faster")
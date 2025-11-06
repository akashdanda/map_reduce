"""
Split benchmark file into chunks for distributed processing
"""
import os

print("Splitting benchmark.txt into chunks...")

os.makedirs('data/benchmark_chunks', exist_ok=True)

# Read and split into ~1000 line chunks (same as single-machine)
with open('data/benchmark.txt', 'r') as f:
    lines = f.readlines()

chunk_size = 1000
chunks = []
for i in range(0, len(lines), chunk_size):
    chunk = lines[i:i+chunk_size]
    chunk_file = f'data/benchmark_chunks/chunk-{i//chunk_size}.txt'
    with open(chunk_file, 'w') as f:
        f.writelines(chunk)
    chunks.append(chunk_file)

print(f"Created {len(chunks)} chunks")
print(f"Files: {' '.join(chunks)}")
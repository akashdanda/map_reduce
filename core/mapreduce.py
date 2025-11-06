"""
Single-machine MapReduce implementation.
This is our starting point - we'll make it distributed next!
"""

import os
from collections import defaultdict
from typing import Callable, List, Tuple, Any


class MapReduce:
    """
    Simple MapReduce framework that runs on a single machine.
    
    Users provide:
    - map_func: processes one input item -> list of (key, value) pairs
    - reduce_func: aggregates values for each key -> final result
    """
    
    def __init__(self, map_func: Callable, reduce_func: Callable):
        self.map_func = map_func
        self.reduce_func = reduce_func
        
    def run(self, input_data: List[str]) -> List[Tuple[Any, Any]]:

       #Execute MapReduce on input data.
        
        #1. MAP: Apply map_func to each input item
        #2. SHUFFLE: Group all values by key
        #3. REDUCE: Apply reduce_func to each key's values
        
        print(f"Starting MapReduce on {len(input_data)} inputs...")
        
        # MAP PHASE
        print("MAP phase...")
        mapped_data = []
        for item in input_data:
            # Each mapper returns list of (key, value) tuples
            results = self.map_func(item)
            mapped_data.extend(results)
        print(f"  Generated {len(mapped_data)} key-value pairs")
        
        # SHUFFLE PHASE
        print("SHUFFLE phase...")
        shuffled_data = defaultdict(list)
        for key, value in mapped_data:
            shuffled_data[key].append(value)
        print(f"  Grouped into {len(shuffled_data)} unique keys")
        
        # REDUCE PHASE
        print("REDUCE phase...")
        final_results = []
        for key, values in shuffled_data.items():
            result = self.reduce_func(key, values)
            final_results.append(result)
        print(f"  Reduced to {len(final_results)} final results")
        
        return final_results


def read_file_chunks(filename: str, chunk_size: int = 1000) -> List[str]:
    """
    Read a file and split into chunks (simulates splitting work across workers).
    Each chunk is a string of text.
    """
    chunks = []
    with open(filename, 'r') as f:
        chunk = []
        for line in f:
            chunk.append(line.strip())
            if len(chunk) >= chunk_size:
                chunks.append('\n'.join(chunk))
                chunk = []
        
        # Add remaining lines
        if chunk:
            chunks.append('\n'.join(chunk))
    
    return chunks
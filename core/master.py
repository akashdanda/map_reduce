"""
Master Server - Coordinates all workers and tasks
Runs a Flask server that workers connect to
"""

from flask import Flask, request, jsonify
import requests
import time
import threading
from collections import defaultdict
import json

class Master:
    def __init__(self, num_reduce_tasks=4):
        self.workers = []  # List of registered workers
        self.map_tasks = []  # Tasks to be assigned
        self.reduce_tasks = []
        self.task_status = {}  # task_id -> status
        self.intermediate_files = defaultdict(list)  # reducer_id -> [files]
        self.num_reduce_tasks = num_reduce_tasks
        self.completed_maps = 0
        self.completed_reduces = 0
        self.app = Flask(__name__)
        self.setup_routes()
        
    def setup_routes(self):
        """Setup Flask endpoints"""
        
        @self.app.route('/register', methods=['POST'])
        def register_worker():
            """Worker announces itself to master"""
            data = request.json
            worker_info = {
                'id': data['worker_id'],
                'address': data['address'],
                'status': 'idle'
            }
            self.workers.append(worker_info)
            print(f"✓ Worker {data['worker_id']} registered at {data['address']}")
            return jsonify({'status': 'registered', 'master': 'ready'})
        
        @self.app.route('/map_complete', methods=['POST'])
        def map_complete():
            """Worker reports map task completion"""
            data = request.json
            task_id = data['task_id']
            intermediate = data['intermediate_files']
            
            print(f"✓ Map task {task_id} completed")
            
            # Store intermediate files grouped by reducer
            for reducer_id, files in intermediate.items():
                self.intermediate_files[int(reducer_id)].extend(files)
            
            self.completed_maps += 1
            
            # If all maps done, start reduce phase
            if self.completed_maps == len(self.map_tasks):
                print("\n=== All MAP tasks complete! Starting REDUCE phase ===\n")
                threading.Thread(target=self.start_reduce_phase).start()
            
            return jsonify({'status': 'acknowledged'})
        
        @self.app.route('/reduce_complete', methods=['POST'])
        def reduce_complete():
            """Worker reports reduce task completion"""
            data = request.json
            task_id = data['task_id']
            output_file = data['output_file']
            
            print(f"✓ Reduce task {task_id} completed -> {output_file}")
            
            self.completed_reduces += 1
            
            if self.completed_reduces == self.num_reduce_tasks:
                print("\n=== All REDUCE tasks complete! ===\n")
                self.merge_results()
            
            return jsonify({'status': 'acknowledged'})
        
        @self.app.route('/status', methods=['GET'])
        def status():
            """Return current job status"""
            return jsonify({
                'workers': len(self.workers),
                'map_progress': f"{self.completed_maps}/{len(self.map_tasks)}",
                'reduce_progress': f"{self.completed_reduces}/{self.num_reduce_tasks}"
            })
    
    def start_map_phase(self, input_files):
        """Distribute map tasks to workers"""
        print(f"\n=== Starting MAP phase with {len(input_files)} tasks ===\n")
        
        self.map_tasks = input_files
        
        # Assign tasks to workers (round-robin)
        for i, input_file in enumerate(input_files):
            worker = self.workers[i % len(self.workers)]
            task_id = f"map-{i}"
            
            threading.Thread(
                target=self.assign_map_task,
                args=(worker, task_id, input_file, i)
            ).start()
            
            time.sleep(0.1)  # Small delay to avoid overwhelming workers
    
    def assign_map_task(self, worker, task_id, input_file, task_num):
        """Send map task to a worker"""
        try:
            print(f"→ Assigning map task {task_id} to Worker {worker['id']}")
            
            response = requests.post(
                f"{worker['address']}/map",
                json={
                    'task_id': task_id,
                    'input_file': input_file,
                    'num_reducers': self.num_reduce_tasks,
                    'task_num': task_num
                },
                timeout=300
            )
            
            if response.status_code != 200:
                print(f"✗ Map task {task_id} failed on Worker {worker['id']}")
                
        except Exception as e:
            print(f"✗ Error assigning map task {task_id}: {e}")
    
    def start_reduce_phase(self):
        """Distribute reduce tasks to workers"""
        print(f"Starting REDUCE phase with {self.num_reduce_tasks} reducers\n")
        
        # Assign reduce tasks
        for reducer_id in range(self.num_reduce_tasks):
            worker = self.workers[reducer_id % len(self.workers)]
            task_id = f"reduce-{reducer_id}"
            
            threading.Thread(
                target=self.assign_reduce_task,
                args=(worker, task_id, reducer_id)
            ).start()
            
            time.sleep(0.1)
    
    def assign_reduce_task(self, worker, task_id, reducer_id):
        """Send reduce task to a worker"""
        try:
            input_files = self.intermediate_files[reducer_id]
            
            print(f"→ Assigning reduce task {task_id} to Worker {worker['id']} ({len(input_files)} files)")
            
            response = requests.post(
                f"{worker['address']}/reduce",
                json={
                    'task_id': task_id,
                    'reducer_id': reducer_id,
                    'input_files': input_files
                },
                timeout=300
            )
            
            if response.status_code != 200:
                print(f"✗ Reduce task {task_id} failed")
                
        except Exception as e:
            print(f"✗ Error assigning reduce task {task_id}: {e}")
    
    def merge_results(self):
        """Merge all reduce outputs into final result"""
        print("Merging final results...")
        
        final_results = []
        for i in range(self.num_reduce_tasks):
            output_file = f"data/output/reduce-{i}.txt"
            try:
                with open(output_file, 'r') as f:
                    for line in f:
                        if line.strip():
                            word, count = line.strip().split('\t')
                            final_results.append((word, int(count)))
            except FileNotFoundError:
                print(f"Warning: {output_file} not found")
        
        # Sort by count
        final_results.sort(key=lambda x: x[1], reverse=True)
        
        # Write final output
        with open('data/output/final_results.txt', 'w') as f:
            for word, count in final_results:
                f.write(f"{word}\t{count}\n")
        
        print(f"\n✓ Final results written to data/output/final_results.txt")
        print(f"✓ Total unique words: {len(final_results)}")
        print(f"\nTop 10 words:")
        for word, count in final_results[:10]:
            print(f"  {word:20s} {count:>8d}")
    
    def run(self, host='127.0.0.1', port=5000):
        """Start the master server"""
        print(f"\n{'='*50}")
        print(f"MASTER SERVER starting on {host}:{port}")
        print(f"{'='*50}\n")
        self.app.run(host=host, port=port, debug=False, threaded=True)


def main():
    """Start master and wait for workers"""
    import sys
    import os
    
    # Create output directory
    os.makedirs('data/output', exist_ok=True)
    os.makedirs('data/intermediate', exist_ok=True)
    
    master = Master(num_reduce_tasks=4)
    
    # Start in background thread so we can schedule tasks
    threading.Thread(target=master.run, daemon=True).start()
    
    # Wait for workers to register
    print("Waiting for workers to register...")
    print("(Start workers in separate terminals)\n")
    
    while len(master.workers) < 3:
        time.sleep(1)
    
    print(f"\n✓ {len(master.workers)} workers registered!\n")
    
    # Start the job
    if len(sys.argv) < 2:
        print("Creating sample data chunks...")
        # Create test chunks
        os.makedirs('data/chunks', exist_ok=True)
        sample_text = "the quick brown fox jumps over the lazy dog " * 100
        
        input_files = []
        for i in range(6):
            chunk_file = f'data/chunks/chunk-{i}.txt'
            with open(chunk_file, 'w') as f:
                f.write(sample_text * (i + 1))
            input_files.append(chunk_file)
    else:
        # Use provided files
        input_files = sys.argv[1:]
    
    # Start map phase
    master.start_map_phase(input_files)
    
    # Keep running
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down...")


if __name__ == '__main__':
    main()
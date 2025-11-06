"""
Worker Process - Executes map and reduce tasks
Each worker is a Flask server that receives tasks from master
"""

from flask import Flask, request, jsonify
import requests
import sys
import os
from collections import defaultdict
import json


class Worker:
    def __init__(self, worker_id, master_address, map_func, reduce_func):
        self.worker_id = worker_id
        self.master_address = master_address
        self.map_func = map_func
        self.reduce_func = reduce_func
        self.app = Flask(__name__)
        self.setup_routes()
        
    def setup_routes(self):
        """Setup Flask endpoints"""
        
        @self.app.route('/map', methods=['POST'])
        def execute_map():
            """Execute a map task"""
            data = request.json
            task_id = data['task_id']
            input_file = data['input_file']
            num_reducers = data['num_reducers']
            task_num = data['task_num']
            
            print(f"[Worker {self.worker_id}] Executing {task_id}")
            
            try:
                # Read input
                with open(input_file, 'r') as f:
                    text = f.read()
                
                # Execute map function
                mapped_data = self.map_func(text)
                
                print(f"[Worker {self.worker_id}]   Mapped {len(mapped_data)} items")
                
                # Partition by key for reducers
                partitions = defaultdict(list)
                for key, value in mapped_data:
                    # Hash key to determine which reducer
                    reducer_id = hash(key) % num_reducers
                    partitions[reducer_id].append((key, value))
                
                # Write intermediate files
                intermediate_files = {}
                for reducer_id, items in partitions.items():
                    filename = f'data/intermediate/map-{task_num}-reduce-{reducer_id}.txt'
                    with open(filename, 'w') as f:
                        for key, value in items:
                            f.write(f"{key}\t{value}\n")
                    
                    intermediate_files.setdefault(reducer_id, []).append(filename)
                
                # Notify master
                requests.post(
                    f"{self.master_address}/map_complete",
                    json={
                        'task_id': task_id,
                        'intermediate_files': intermediate_files
                    }
                )
                
                return jsonify({'status': 'completed'})
                
            except Exception as e:
                print(f"[Worker {self.worker_id}] Error in map: {e}")
                return jsonify({'status': 'failed', 'error': str(e)}), 500
        
        @self.app.route('/reduce', methods=['POST'])
        def execute_reduce():
            """Execute a reduce task"""
            data = request.json
            task_id = data['task_id']
            reducer_id = data['reducer_id']
            input_files = data['input_files']
            
            print(f"[Worker {self.worker_id}] Executing {task_id}")
            
            try:
                # Read all intermediate files and group by key
                grouped_data = defaultdict(list)
                for filename in input_files:
                    with open(filename, 'r') as f:
                        for line in f:
                            if line.strip():
                                key, value = line.strip().split('\t')
                                grouped_data[key].append(int(value))
                
                print(f"[Worker {self.worker_id}]   Reducing {len(grouped_data)} keys")
                
                # Execute reduce function
                results = []
                for key, values in grouped_data.items():
                    result = self.reduce_func(key, values)
                    results.append(result)
                
                # Write output
                output_file = f'data/output/reduce-{reducer_id}.txt'
                with open(output_file, 'w') as f:
                    for key, value in results:
                        f.write(f"{key}\t{value}\n")
                
                # Notify master
                requests.post(
                    f"{self.master_address}/reduce_complete",
                    json={
                        'task_id': task_id,
                        'output_file': output_file
                    }
                )
                
                return jsonify({'status': 'completed'})
                
            except Exception as e:
                print(f"[Worker {self.worker_id}] Error in reduce: {e}")
                return jsonify({'status': 'failed', 'error': str(e)}), 500
    
    def register_with_master(self, worker_address):
        """Announce ourselves to the master"""
        try:
            response = requests.post(
                f"{self.master_address}/register",
                json={
                    'worker_id': self.worker_id,
                    'address': worker_address
                }
            )
            if response.status_code == 200:
                print(f"✓ Worker {self.worker_id} registered with master")
            else:
                print(f"✗ Failed to register: {response.text}")
        except Exception as e:
            print(f"✗ Cannot connect to master: {e}")
            sys.exit(1)
    
    def run(self, host='127.0.0.1', port=5001):
        """Start the worker server"""
        worker_address = f"http://{host}:{port}"
        
        print(f"\n{'='*50}")
        print(f"WORKER {self.worker_id} starting on {host}:{port}")
        print(f"{'='*50}\n")
        
        # Register with master
        self.register_with_master(worker_address)
        
        # Start Flask server
        self.app.run(host=host, port=port, debug=False, threaded=True)


def create_worker(worker_id, port, master_address='http://127.0.0.1:5000'):
    """Factory function to create a worker with word count functions"""
    
    def map_func(text):
        """Word count map function"""
        words = text.lower().split()
        return [(word.strip('.,!?;:'), 1) for word in words if word.strip('.,!?;:')]
    
    def reduce_func(word, counts):
        """Word count reduce function"""
        return (word, sum(counts))
    
    worker = Worker(worker_id, master_address, map_func, reduce_func)
    worker.run(port=port)


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: python worker.py <worker_id> <port>")
        print("Example: python worker.py 0 5001")
        sys.exit(1)
    
    worker_id = int(sys.argv[1])
    port = int(sys.argv[2])
    
    create_worker(worker_id, port)
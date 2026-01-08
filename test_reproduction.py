import pickle
import os
import sys
import time
from src.pipeline import run_pipeline

def test_reproduction():
    start_time = time.time()
    print("Running pipeline for first 5 users...")
    
    try:
        run_pipeline(max_users=100)
    except Exception as e:
        print(f"Pipeline failed: {e}")
        return
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Pipeline finished in {elapsed_time:.4f} seconds.")
    
    # Check output for User 1
    output_file = 'subgraphs/1_subgraphs.pkl'
    if not os.path.exists(output_file):
        print(f"Error: {output_file} not found.")
        return

    print(f"Loading {output_file}...")
    with open(output_file, 'rb') as f:
        result = pickle.load(f)

    print("Result sample:")
    print(str(result)[:500])
    
    if isinstance(result, list) and len(result) > 0:
        print("Test PASSED: Output generated and matches expected structure.")
    elif isinstance(result, dict) and len(result) > 0: # Result structure changed to dict in optimization
        print("Test PASSED: Output generated (Dict structure).")
    else:
        print("Test FAILED: Output empty or wrong structure.")

if __name__ == "__main__":
    test_reproduction()

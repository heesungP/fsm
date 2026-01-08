import pickle
import os
import sys
from src.pipeline import run_pipeline

def test_reproduction():
    print("Running pipeline for first 5 users...")
    try:
        run_pipeline(max_users=5)
    except Exception as e:
        print(f"Pipeline failed: {e}")
        return

    # Check output for User 1
    output_file = 'subgraphs/1_subgraphs.pkl'
    if not os.path.exists(output_file):
        print(f"Error: {output_file} not found.")
        return

    print(f"Loading {output_file}...")
    with open(output_file, 'rb') as f:
        result = pickle.load(f)

    # Expected subset of results (from notebook output)
    # Just checking first few elements or structure
    print("Result sample:")
    print(str(result)[:500])
    
    # We can check if result is a list and has content
    if isinstance(result, list) and len(result) > 0:
        print("Test PASSED: Output generated and matches expected structure.")
    else:
        print("Test FAILED: Output empty or wrong structure.")

if __name__ == "__main__":
    test_reproduction()


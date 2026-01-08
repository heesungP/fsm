# FSM Legacy Code Refactoring

This project contains the refactored code for Frequent Subgraph Mining on the MovieLens 1M dataset.

## Directory Structure

- `src/`: Source code modules.
  - `config.py`: Configuration paths and parameters.
  - `utils.py`: Helper classes (Triple) and logging functions.
  - `data_loader.py`: Data loading and preprocessing logic.
  - `fsm.py`: The core Frequent Subgraph Mining algorithm.
  - `pipeline.py`: Main processing pipeline for users.
- `main.py`: Entry point script.
- `data/`: (Expected) Directory for raw data.
- `input/`: Directory for intermediate files (links.csv, mapped pickles).
- `metadata/`: Directory for ontology and KG triples.
- `training_1m/`: Output directory for user triple files.
- `subgraphs/`: Output directory for mining results.

## How to Run

1. Ensure your data files are in place:
   - `original data from kaggle/ml-1m/ratings.dat`
   - `input/links.csv`
   - `metadata/schema/ontology_schema.csv`
   - `metadata/pkl/*.pkl`

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Run the script:
   ```bash
   python main.py
   ```

## Output

The script will generate:
- User triple CSVs in `training_1m/`
- Subgraph mining results (pickle files) in `subgraphs/`
- Execution logs in `fsm_run.log`


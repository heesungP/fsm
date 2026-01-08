import os

# Paths
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT_DIR, 'original data from kaggle', 'ml-1m')
INPUT_DIR = os.path.join(ROOT_DIR, 'input')
METADATA_DIR = os.path.join(ROOT_DIR, 'metadata')
METADATA_PICKLE_DIR = os.path.join(METADATA_DIR, 'pkl')
SCHEMA_FILE = os.path.join(METADATA_DIR, 'schema', 'ontology_schema.csv')
TRAINING_FOLDER = os.path.join(ROOT_DIR, 'training_1m')
SUBGRAPHS_FOLDER = os.path.join(ROOT_DIR, 'subgraphs')

# Files
RATINGS_FILE = os.path.join(DATA_DIR, 'ratings.dat')
LINKS_FILE = os.path.join(INPUT_DIR, 'links.csv')
MAPPED_DATA_FILE = os.path.join(INPUT_DIR, 'mapped_ml_1m.pkl')

# Hyperparameters
RECENT = 100  # Number of movies to use for learning
START_CLASS = 'WatchingEvent'
END_CLASS_LIST = ['User', 'Rating', 'Collection', 'Genre', 'Company', 'Country', 'Keyword', 'Person',
                  'Budget', 'Popularity', 'Revenue', 'Runtime', 'Vote_Average', 'Vote_Count']
OPTION_CLASS_LIST = ['Movie']  # Classes to be abstracted
MAX_DEPTH = 10


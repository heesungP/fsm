import pandas as pd
import pickle
import os
from collections import defaultdict
from .config import (
    RATINGS_FILE, LINKS_FILE, MAPPED_DATA_FILE, METADATA_PICKLE_DIR, INPUT_DIR
)

def load_and_preprocess_data():
    """
    Loads ratings and links, merges them, handles nulls, and saves/returns the mapped dataframe.
    """
    # Ensure input directory exists
    if not os.path.exists(INPUT_DIR):
        os.makedirs(INPUT_DIR)

    # 1. Load Ratings
    print("Loading ratings...")
    ml_1m = pd.read_csv(RATINGS_FILE, sep='::', engine='python', header=None)
    ml_1m.columns = ['userId', 'movieId', 'rating', 'timestamp']

    # 2. Load Links
    print("Loading links...")
    links = pd.read_csv(LINKS_FILE)

    # 3. Merge
    print("Merging data...")
    ml_1m_merged = pd.merge(ml_1m, links, left_on='movieId', right_on='movieId', sort=False)

    # 4. Filter Null tmdbId
    ml_1m_merged = ml_1m_merged.loc[:, ['userId', 'tmdbId', 'rating', 'timestamp']].copy()
    ml_1m_merged = ml_1m_merged[ml_1m_merged['tmdbId'].notnull()].copy()
    
    # 5. Type Conversion
    ml_1m_merged['tmdbId'] = ml_1m_merged['tmdbId'].astype(int)

    # 6. Sort
    mapped_ml_1m = ml_1m_merged.sort_values(['userId', 'timestamp'])

    # 7. Save
    print(f"Saving mapped data to {MAPPED_DATA_FILE}...")
    with open(MAPPED_DATA_FILE, 'wb') as f:
        pickle.dump(mapped_ml_1m, f)

    return mapped_ml_1m

def load_mapped_data():
    if os.path.exists(MAPPED_DATA_FILE):
        print(f"Loading mapped data from {MAPPED_DATA_FILE}...")
        with open(MAPPED_DATA_FILE, 'rb') as f:
            return pickle.load(f)
    else:
        return load_and_preprocess_data()

def load_metadata_triples():
    """
    Loads all metadata triples from pickle files and returns a dictionary of dictionaries.
    """
    print("Loading metadata triples...")
    metadata_files = [
        'collection_triples.pkl', 'genre_triples.pkl', 'company_triples.pkl',
        'country_triples.pkl', 'budget_triples.pkl', 'popularity_triples.pkl',
        'revenue_triples.pkl', 'runtime_triples.pkl', 'voav_triples.pkl',
        'voco_triples.pkl', 'cast_triples.pkl', 'crew_triples.pkl',
        'keyword_triples.pkl'
    ]
    
    metadata_dicts = {}
    
    for filename in metadata_files:
        key = filename.replace('.pkl', '') + '_dict'  # e.g., collection_triples_dict
        filepath = os.path.join(METADATA_PICKLE_DIR, filename)
        
        if os.path.exists(filepath):
            with open(filepath, 'rb') as f:
                triples = pickle.load(f)
            
            # Create dictionary hashed by movie ID (usually index 1 in the triple tuple)
            # Assuming structure: (idx, MOVI_ID, ...)
            # Based on notebook: collection_triples_dict[t[1]].append(t)
            triples_dict = defaultdict(list)
            for t in triples:
                triples_dict[t[1]].append(t)
            
            metadata_dicts[key] = triples_dict
        else:
            print(f"Warning: Metadata file {filename} not found.")
            metadata_dicts[key] = defaultdict(list)
            
    return metadata_dicts


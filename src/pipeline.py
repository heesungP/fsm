import os
import math
import pickle
import copy
from datetime import datetime
from collections import defaultdict
import pandas as pd
import logging
from joblib import Parallel, delayed, cpu_count

from .config import (
    TRAINING_FOLDER, SUBGRAPHS_FOLDER, SCHEMA_FILE,
    START_CLASS, END_CLASS_LIST, OPTION_CLASS_LIST, MAX_DEPTH,
    RECENT
)
from .utils import log_data
from .fsm import FSMEngine
from .data_loader import load_mapped_data, load_metadata_triples

def process_single_user(user_data, schema_data, metadata_dicts):
    user_id, user_series = user_data
    original_property_dict, original_ontology_graph, original_ontology_path_list, original_path_property_set = schema_data
    
    start_datetime = datetime.now()
    watched_movie_len = len(user_series)
    print(f"User: {user_id} | Number of Watching Events: {watched_movie_len}")

    # Threshold Calculation
    min_support = 0
    if watched_movie_len > 100:
        min_support = 4
    elif 8 <= watched_movie_len <= 100:
        min_support = int(math.log(watched_movie_len))
    elif 3 <= watched_movie_len < 8:
        min_support = 2
    else:
        print(f"Skipping User {user_id} (Not enough data)")
        return

    print(f'threshold: {min_support}')

    target_data = user_series.iloc[:-1].copy()
    
    # --- Triple Generation (Vectorized & Memory-based) ---
    total_triples = list()
    
    # Vectorized preparation
    uids = target_data['userId'].astype(int).astype(str).tolist()
    mids = target_data['tmdbId'].astype(int).astype(str).tolist()
    # ratings = target_data['rating'].tolist() # Not used in triple generation logic provided
    
    for uid, mid in zip(uids, mids):
        user_node = "USER_" + uid
        movie_node = "MOVI_" + mid
        event_node = "U" + uid + "_M" + mid
        
        # Triple 1: User -> WatchingEvent
        total_triples.append((0, "User", user_node, "UserWatching", "WatchingEvent", event_node))
        
        # Triple 3: WatchingEvent -> Movie
        total_triples.append((0, "WatchingEvent", event_node, "WatchingMovie", "Movie", movie_node))
        
        # Metadata augmentation
        if movie_node in metadata_dicts: # metadata_dicts is flattened? No, it's dict of dicts.
             # Wait, metadata_dicts in original code was: 
             # for meta_type, meta_dict in metadata_dicts.items():
             #    if movi_key in meta_dict: total_triples.extend(...)
             # We should optimize this.
             pass

    # Re-implement metadata logic efficiently
    # To ensure identical triple ID generation as the original code, we must iterate 
    # metadata_dicts in the exact same order. The original code used:
    # for meta_type, meta_dict in metadata_dicts.items():
    #
    # Since metadata_dicts is loaded from files, the insertion order into the dict 
    # determines the iteration order in Python 3.7+.
    # However, to be absolutely safe and consistent across runs/processes, 
    # we should rely on the original iteration order or sorted keys if appropriate.
    # The original code didn't sort, so it relied on insertion order.
    # We will use the same iteration logic.
    
    # Pre-fetch metadata keys to ensure stable order if needed, but original code just did .items()
    # metadata_dicts is passed as an argument.
    
    for uid, mid in zip(uids, mids):
        movi_key = "MOVI_" + mid
        
        # Original logic:
        # for meta_type, meta_dict in metadata_dicts.items():
        #     if movi_key in meta_dict:
        #         total_triples.extend(meta_dict[movi_key])
        
        for meta_type, meta_dict in metadata_dicts.items():
            if movi_key in meta_dict:
                 total_triples.extend(meta_dict[movi_key])

    # Assign Triple IDs and Format for Engine
    triples_for_engine = []
    triple_no = 0
    for t in total_triples:
        # Check logic to match original behavior exactly:
        # Original: instance_triple = f'{triple_no}^{total_triple[0]}^{total_triple[1]}^{total_triple[2]}^{total_triple[3]}^{total_triple[4]}'
        # It always takes the first 5 elements regardless of length.
        
        if len(t) >= 5:
            # Construct row for store_triples: [id, subj_cl, subj_inst, prop, obj_cl, obj_inst]
            # Use t[0]~t[4] to ensure identical behavior with original code
            row = [str(triple_no), t[0], t[1], t[2], t[3], t[4]]
            triples_for_engine.append(row)
            triple_no += 1

    mid_datetime = datetime.now()

    # --- Run FSM for User ---
    engine = FSMEngine()
    
    # Restore schema info
    engine.property_dict = copy.deepcopy(original_property_dict)
    engine.ontology_graph = original_ontology_graph # Graph is not modified usually
    engine.ontology_path_list = copy.deepcopy(original_ontology_path_list)
    engine.path_property_set = copy.deepcopy(original_path_property_set)

    # 3. Store Triples (Memory-based)
    start_instance_list, triple_dict, prop_triples_dict = engine.store_triples(triples_for_engine, START_CLASS)
    engine.prop_triples_dict = prop_triples_dict

    # Filter schema based on available data
    prop_str_list = [property_info[1] for property_id, property_info in engine.property_dict.items()]
    
    # Use dict comprehension for filtering (faster than pop in loop)
    triple_dict = {tid: t for tid, t in triple_dict.items() if t.prop in prop_str_list}

    engine.property_dict = {pid: val for pid, val in engine.property_dict.items() 
                            if val[1] in prop_triples_dict.keys()}
    engine.ontology_path_list = [op for op in engine.ontology_path_list 
                                 if set(op).issubset(set(engine.property_dict.keys()))]
    engine.path_property_set = engine.path_property_set.intersection(set(engine.property_dict.keys()))

    # 4. Triple Paths
    triple_paths_dict = {start_instance: engine.find_triple_paths(START_CLASS, start_instance)
                         for start_instance in start_instance_list}
    
    transaction_triple = {start_instance: set(sum(triple_paths, [])) 
                          for start_instance, triple_paths in triple_paths_dict.items()}
    
    it_trs = defaultdict(set)
    for start_instance, triple_set in transaction_triple.items():
        for tid in triple_set:
            it_trs[tid].add(start_instance)
            
    # Filter triple_dict
    triple_dict = {tid: t for tid, t in triple_dict.items() if tid in it_trs}
            
    itid_tr = {tid: list(start_instance)[0] for tid, start_instance in it_trs.items()}
    
    # 5. Chunk Type
    engine.prop_chunk_type_dict = engine.get_chunking_type()
    
    # 6. Generate Candidates
    it_hash = {k: v.copy() for k, v in triple_dict.items()} # Shallow copy of dict, copy of objects
    candi_it_tr, same_itids = engine.generate_candidate(it_hash=it_hash, itid_tr=itid_tr, threshold=min_support)
    
    # Set same code
    same_code_number = 1
    for tid, iso_trip_lst in same_itids.items():
        if it_hash[tid].same_code == '':
            same_code = f"same_{same_code_number}"
            for iso_trip in iso_trip_lst:
                if it_hash[iso_trip].same_code == '':
                    it_hash[iso_trip].set_same_code(same_code)
            same_code_number += 1
            
    if len(list(candi_it_tr.keys())) > 0:
        sampled_candidate = list(candi_it_tr.keys())[0]
        candidates = same_itids[sampled_candidate]
        
        engine.chunking(candidates=candidates, it_hash=it_hash, itid_tr=itid_tr, threshold=min_support)
        
        # Post processing results
        subjects = set(v[1] for k, v in engine.Chunking_Result.items())
        objects = set(v[3] for k, v in engine.Chunking_Result.items())
        instance_as_chunk = [i for i in subjects.union(objects) if i.isdigit()]
        
        for triple_id, triple_info in engine.Chunking_Result.items():
            if triple_id in instance_as_chunk:
                triple_info[5] = ''
                engine.chunking_result_final[triple_id] = triple_info
            else:
                engine.chunking_result_final[triple_id] = triple_info
                
        chunk_stack_list = list()
        for triple_id, triple_info in engine.chunking_result_final.items():
            if triple_info[5] == '1':
                engine.chunk_stack.append(engine.ITID_Freq_depth[triple_id][0])
                engine.chunk_stack.append(itid_tr[triple_id])
                
                engine.find_result(triple_id)
                
                chunk_stack_list.append(engine.chunk_stack.copy())
                engine.chunk_stack.clear()
                
        # Save Results
        with open(f'{SUBGRAPHS_FOLDER}/{user_id}_triples_in_subgraphs.pkl', 'wb') as f:
            pickle.dump(engine.chunking_result_final, f)
        with open(f'{SUBGRAPHS_FOLDER}/{user_id}_subgraphs.pkl', 'wb') as f:
            pickle.dump(chunk_stack_list, f)
            
    end_datetime = datetime.now()
    print(f'User {user_id} - Triple Collection: {(mid_datetime - start_datetime).seconds}s, Subgraph Mining: {(end_datetime - mid_datetime).seconds}s')


def run_pipeline(max_users=None):
    # Setup folders
    if not os.path.exists(TRAINING_FOLDER):
        os.makedirs(TRAINING_FOLDER)
    if not os.path.exists(SUBGRAPHS_FOLDER):
        os.makedirs(SUBGRAPHS_FOLDER)
    
    # Logging Setup
    logging.basicConfig(filename='fsm_run.log', format='%(message)s', filemode='w', level=logging.INFO)

    # 1. Load Data
    mapped_ml_1m = load_mapped_data()
    metadata_dicts = load_metadata_triples()
    
    # 2. Schema Loading (Once)
    print("Loading schema...")
    engine = FSMEngine()
    property_dict, ontology_graph, class_dict = engine.load_schema(SCHEMA_FILE)
    
    print("Finding ontology paths...")
    ontology_path_list, path_property_set = engine.find_ontology_paths(
        START_CLASS, END_CLASS_LIST, ontology_graph, MAX_DEPTH
    )
    
    # Pack schema data to pass to workers
    schema_data = (property_dict, ontology_graph, ontology_path_list, path_property_set)

    print("Starting user processing...")
    
    # Prepare User Groups
    user_groups = []
    for user_id, user_series in mapped_ml_1m.groupby('userId'):
        if max_users is not None and int(user_id) > max_users:
            break
        user_groups.append((user_id, user_series))
        
    # Parallel Execution
    n_jobs = max(1, cpu_count() - 1) # Ensure at least 1 job, leave 1 core free
    print(f"Running on {n_jobs} cores...")
    
    Parallel(n_jobs=n_jobs)(
        delayed(process_single_user)(user_group, schema_data, metadata_dicts) 
        for user_group in user_groups
    )
    
    print("Pipeline completed.")

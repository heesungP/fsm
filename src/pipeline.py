import os
import math
import pickle
import copy
from datetime import datetime
from collections import defaultdict
import pandas as pd
import logging

from .config import (
    TRAINING_FOLDER, SUBGRAPHS_FOLDER, SCHEMA_FILE,
    START_CLASS, END_CLASS_LIST, OPTION_CLASS_LIST, MAX_DEPTH,
    RECENT
)
from .utils import log_data
from .fsm import FSMEngine
from .data_loader import load_mapped_data, load_metadata_triples

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
    
    # 2. Iterate Users
    movie_log = dict()
    
    # Create an engine instance to reuse schema loading
    engine = FSMEngine()
    
    # Load schema once
    print("Loading schema...")
    engine.property_dict, engine.ontology_graph, class_dict = engine.load_schema(SCHEMA_FILE)
    
    # Store clean property dict for re-use (because it gets filtered per user)
    original_property_dict = copy.deepcopy(engine.property_dict)
    
    # Find Ontology Paths (Schema Level) - doing this once
    print("Finding ontology paths...")
    original_ontology_path_list, original_path_property_set = engine.find_ontology_paths(
        START_CLASS, END_CLASS_LIST, engine.ontology_graph, MAX_DEPTH
    )

    print("Starting user processing...")
    for user_id, user_series in mapped_ml_1m.groupby('userId'):
        if max_users is not None and int(user_id) > max_users:
            break
        
        start_datetime = datetime.now()
        
        watched_movie_len = len(user_series)
        print(f"User: {user_id} | Number of Watching Events: {watched_movie_len}")
        
        target_data = user_series.iloc[:-1].copy()
        one_left = user_series.tail(1)
        
        seen_movies = list(one_left['tmdbId'].values)
        unseen_movie = list(target_data['tmdbId'].values)
        movie_log[user_id] = (seen_movies, unseen_movie)
        
        # --- Triple Generation for User ---
        total_triples = list()
        for idx, row in target_data.iterrows():
            uid = str(int(row['userId']))
            mid = str(int(row['tmdbId']))
            rating = row['rating']
            timestamp = str(int(row['timestamp']))
            
            triple1 = ["User", "USER_" + uid, "UserWatching", "WatchingEvent", ''.join(["U", uid, "_M", mid])]
            # triple2 = ["Movie", "MOVI_" + mid, "MovieRating", "Rating", ''.join(["RATI_", str("%02d" % (rating*10))])]
            triple3 = ["WatchingEvent", ''.join(["U", uid, "_M", mid]), "WatchingMovie", "Movie", "MOVI_" + mid]
            
            total_triples.append(triple1)
            total_triples.append(triple3)
            
            # Metadata augmentation
            movi_key = "MOVI_" + mid
            for meta_type, meta_dict in metadata_dicts.items():
                if movi_key in meta_dict:
                    total_triples.extend(meta_dict[movi_key])
                    
        # Write User Triples to File
        triple_no = 0
        triples_to_write = list()
        for total_triple in total_triples:
            # Ensure triple has at least 5 elements
            if len(total_triple) >= 5:
                instance_triple = f'{triple_no}^{total_triple[0]}^{total_triple[1]}^{total_triple[2]}^{total_triple[3]}^{total_triple[4]}'
                triples_to_write.append(instance_triple)
                triple_no += 1
        
        instance = pd.Series(triples_to_write)
        triple_file = f'{TRAINING_FOLDER}/{user_id}.csv'
        instance.to_csv(triple_file, index=False, header=False)
        
        # --- Threshold Calculation ---
        min_support = 0
        if watched_movie_len > 100:
            min_support = 4
        elif 8 <= watched_movie_len <= 100:
            min_support = int(math.log(watched_movie_len))
        elif 3 <= watched_movie_len < 8:
            min_support = 2
        else:
            # pass_case += 1
            print(f"Skipping User {user_id} (Not enough data)")
            continue
            
        print(f'threshold: {min_support}')
        mid_datetime = datetime.now()
        
        # --- Run FSM for User ---
        # Reset Engine State for new user
        engine.ChunkID_Label = {}
        engine.ITID_Freq_depth = {}
        engine.depth_chunk = 0
        engine.Chunking_Result = {}
        engine.chunking_result_final = {}
        engine.chunk_stack = []
        
        # Restore original schema info
        engine.property_dict = copy.deepcopy(original_property_dict)
        engine.ontology_path_list = copy.deepcopy(original_ontology_path_list)
        engine.path_property_set = copy.deepcopy(original_path_property_set)
        
        # 3. Store Triples
        start_instance_list, triple_dict, prop_triples_dict = engine.store_triples(triple_file, START_CLASS)
        engine.prop_triples_dict = prop_triples_dict
        
        # Filter schema based on available data
        prop_str_list = [property_info[1] for property_id, property_info in engine.property_dict.items()]
        triple_dict_temp = copy.deepcopy(triple_dict)
        for tid, triple in triple_dict_temp.items():
            if triple.prop not in prop_str_list:
                triple_dict.pop(tid)

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
        triple_dict_keys = list(triple_dict.keys())
        for tid in triple_dict_keys:
            if tid not in it_trs:
                triple_dict.pop(tid)
                
        itid_tr = {tid: list(start_instance)[0] for tid, start_instance in it_trs.items()}
        
        # 5. Chunk Type
        engine.prop_chunk_type_dict = engine.get_chunking_type()
        
        # 6. Generate Candidates
        it_hash = copy.deepcopy(triple_dict)
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
        print('Triple Collection (sec):', (mid_datetime - start_datetime).seconds)
        print('Subgraph Mining (sec):', (end_datetime - mid_datetime).seconds)
        print('---------------------------------------')


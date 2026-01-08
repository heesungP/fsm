import copy
import logging
from collections import defaultdict
from .utils import Triple, CustomError, log_data
from .config import OPTION_CLASS_LIST

class FSMEngine:
    def __init__(self, mapper):
        self.mapper = mapper
        self.ontology_path_list = []
        self.property_dict = {}
        self.prop_triples_dict = {}
        self.prop_chunk_type_dict = {}
        self.ChunkID_Label = {}
        self.ITID_Freq_depth = {}
        self.depth_chunk = 0
        self.Chunking_Result = {}
        self.chunking_result_final = {}
        self.chunk_stack = []
        self.path_property_set = set()
        self.option_class_ids = set()

    def load_schema(self, file):
        prop_dict = dict()
        ont_graph = dict()
        cl_dict = dict()

        cid = 0
        with open(file, 'r') as f:
            while True:
                line = f.readline().rstrip()
                if not line:
                    break

                idx, dom, prop, ran = line.split('^')
                
                # Integer Indexing
                idx_id = self.mapper.get_id(idx)
                dom_id = self.mapper.get_id(dom)
                prop_id = self.mapper.get_id(prop)
                ran_id = self.mapper.get_id(ran)

                prop_dict[idx_id] = [dom_id, prop_id, ran_id]

                if dom_id not in cl_dict:
                    cl_dict[dom_id] = cid
                    cid += 1
                if ran_id not in cl_dict:
                    cl_dict[ran_id] = cid
                    cid += 1

                if dom_id == ran_id:
                    continue

                if dom_id in ont_graph:
                    ont_graph[dom_id].append([idx_id, ran_id])
                else:
                    ont_graph[dom_id] = [[idx_id, ran_id]]

                if ran_id in ont_graph:
                    ont_graph[ran_id].append([idx_id, dom_id])
                else:
                    ont_graph[ran_id] = [[idx_id, dom_id]]
        
        # Cache Option Class IDs
        self.option_class_ids = {self.mapper.get_id(c) for c in OPTION_CLASS_LIST}

        log_data('property_dict', str(prop_dict)) # Log string rep
        log_data('ontology_graph', str(ont_graph))
        log_data('cl_dict', str(cl_dict))

        return prop_dict, ont_graph, cl_dict

    def find_ontology_paths(self, start, end, graph, max_depth):
        # Convert start/end to IDs
        start_id = self.mapper.get_id(start)
        end_ids = {self.mapper.get_id(e) for e in end}
        
        properties = set()
        stack = [(start_id, [], [start_id])]
        result = []

        while stack:
            n, path, cl = stack.pop()
            if len(path) >= max_depth:
                continue
            if n in end_ids:
                result.append(path)
                for i in path:
                    properties.add(i)
            else:
                if n in graph:
                    for m in graph[n]:
                        # m is [idx_id, ran_id/dom_id]
                        if m[0] not in path:
                            stack.append((m[1], path + [m[0]], cl + [m[1]]))

        log_data('ontology_path_list', str(result))
        log_data('Number of schema-level paths', len(result))
        log_data('path_property_set', str(properties))

        return result, properties

    def store_triples(self, triples_data, start_cl):
        """
        triples_data: list of raw strings/values. 
        We convert them to IDs here.
        """
        start_cl_id = self.mapper.get_id(start_cl)
        
        start_instances = list()
        triple_dict = dict()
        prop_triples_dict = dict()

        for row in triples_data:
            # row: [triple_id_str, subj_cl_str, subj_inst_str, prop_str, obj_cl_str, obj_inst_str]
            # Convert ALL to IDs
            triple_id = self.mapper.get_id(row[0])
            subj_cl = self.mapper.get_id(row[1])
            subj_inst = self.mapper.get_id(row[2])
            prop = self.mapper.get_id(row[3])
            obj_cl = self.mapper.get_id(row[4])
            obj_inst = self.mapper.get_id(row[5])
            
            temp_triple = Triple(triple_id, subj_cl, subj_inst, prop, obj_cl, obj_inst)

            if obj_cl == start_cl_id and obj_inst not in start_instances:
                start_instances.append(obj_inst)
            if subj_cl == start_cl_id and subj_inst not in start_instances:
                start_instances.append(subj_inst)

            triple_dict[triple_id] = temp_triple # Key is int ID

            if prop not in prop_triples_dict:
                prop_triples_dict[prop] = [temp_triple]
            else:
                prop_triples_dict[prop].append(temp_triple)

        log_data('start_instance_list', str(start_instances))
        log_data('triple_dict', str(triple_dict))
        log_data('prop_triples_dict', str(prop_triples_dict))

        return start_instances, triple_dict, prop_triples_dict

    def find_triple_paths(self, start_cl, start_instance):
        # start_cl and start_instance should be IDs
        start_cl_id = self.mapper.get_id(start_cl) if isinstance(start_cl, str) else start_cl
        
        # Logging with strings for debugging
        logging.info('----------------- STARTING POINT: {a}, {b} -----------------'.format(
            a=self.mapper.get_str(start_cl_id), 
            b=self.mapper.get_str(start_instance)))
            
        triple_paths = []
        
        ontology_path_list = self.ontology_path_list
        property_dict = self.property_dict
        prop_triples_dict = self.prop_triples_dict

        for ont_path in ontology_path_list:
            # logging.info('ONT_PATH: %s' % ont_path) # ont_path contains IDs
            queue = []

            first_property = property_dict[ont_path[0]]
            first_subj_cl, first_prop, first_obj_cl = first_property

            if first_prop in prop_triples_dict:
                for triple in prop_triples_dict[first_prop]:
                    if first_subj_cl == start_cl_id:
                        second_cl = first_obj_cl
                    else:
                        second_cl = first_subj_cl

                    if start_instance == triple.get_instance_of(start_cl_id):
                        queue.append((second_cl, triple.get_instance_of(second_cl), [triple.idx]))
            else:
                continue

            # logging.info('- FIRST QUEUE: %s' % queue)

            for idx in range(1, len(ont_path)):
                next_property = property_dict[ont_path[idx]]
                next_subj_cl, next_prop, next_obj_cl = next_property

                if next_prop in prop_triples_dict:
                    for j in range(len(queue)):
                        first_cl, first_inst, path = queue.pop(0)

                        for n_triple in prop_triples_dict[next_prop]:
                            if next_subj_cl == first_cl:
                                second_cl = next_obj_cl
                            else:
                                second_cl = next_subj_cl

                            if first_inst == n_triple.get_instance_of(first_cl):
                                queue.append((second_cl, n_triple.get_instance_of(second_cl), path + [n_triple.idx]))
                else:
                    queue = []
                    break
                # logging.info('- NEXT QUEUE: %s' % queue)

            for q in queue:
                triple_paths.append(q[2])

        return triple_paths

    def get_chunking_type(self):
        prop_type_dict = {'either': [], 'both': []}

        for property_id in self.path_property_set:
            dom, prop, ran = self.property_dict[property_id]

            # Use cached option_class_ids
            if (dom in self.option_class_ids and ran not in self.option_class_ids) or \
               (ran in self.option_class_ids and dom not in self.option_class_ids):
                prop_type_dict['either'].append(property_id)
            if dom in self.option_class_ids and ran in self.option_class_ids:
                prop_type_dict['both'].append(property_id)

        log_data('prop_chunk_type_dict', str(prop_type_dict))
        return prop_type_dict

    def make_freq_depth(self, triple, transactions):
        freq_depth = [len(transactions), '0', 0, '0']

        # Convert back to string to parse depth
        sbj_inst_str = self.mapper.get_str(triple.subj_inst)
        obj_inst_str = self.mapper.get_str(triple.obj_inst)
        
        sbj_depth = 0
        if ':' in sbj_inst_str:
            try:
                sbj_depth = int(sbj_inst_str[sbj_inst_str.find('_') + 1:sbj_inst_str.find(':')])
            except ValueError:
                pass
        obj_depth = 0
        if ':' in obj_inst_str:
            try:
                obj_depth = int(obj_inst_str[obj_inst_str.find('_') + 1:obj_inst_str.find(':')])
            except ValueError:
                pass

        freq_depth[2] = max(sbj_depth, obj_depth)
        return freq_depth

    def generate_candidate(self, it_hash, itid_tr, threshold):
        ChunkID_Label = self.ChunkID_Label
        ITID_Freq_depth = self.ITID_Freq_depth
        depth_chunk = self.depth_chunk

        it_hash_temp = {k: v.copy() for k, v in it_hash.items()}

        for tid, triple in it_hash_temp.items():
            sbj_cl = triple.subj_cl
            obj_cl = triple.obj_cl
            sbj_inst = triple.subj_inst
            obj_inst = triple.obj_inst

            # Use IDs
            if sbj_cl in self.option_class_ids:
                triple.set_subj_inst(sbj_cl)
            if obj_cl in self.option_class_ids:
                triple.set_obj_inst(obj_cl)

            if sbj_inst in ChunkID_Label:
                triple.set_subj_inst(ChunkID_Label[sbj_inst])
            if obj_inst in ChunkID_Label:
                triple.set_obj_inst((ChunkID_Label[obj_inst]))

        # Use tuple_code instead of str_code for hashing
        it_hash_temp_code = {tid: triple.tuple_code() for tid, triple in it_hash_temp.items()}

        iso_triples_dict = defaultdict(list)
        for tid, code in it_hash_temp_code.items():
            iso_triples_dict[code].append(tid)

        if not iso_triples_dict:
            return {}, {}

        max_freq = max([len(triples) for _, triples in iso_triples_dict.items()])

        same_triples_dict = {tid: tid_list for _, tid_list in iso_triples_dict.items() for tid in tid_list}

        ITID_Trs = {tid: set(itid_tr[t] for t in iso_trip_lst) for tid, iso_trip_lst in same_triples_dict.items()}

        for tid, iso_trip_trans in ITID_Trs.items():
            ITID_Freq_depth[tid] = self.make_freq_depth(it_hash_temp[tid], iso_trip_trans)

        min_depth = min([freq_depth[2] for _, freq_depth in ITID_Freq_depth.items() if
                         (freq_depth[0] == max_freq) and (freq_depth[1] != '1')], default=0)

        label_no = 0
        num_of_candidates = 0
        candi_it_tr = dict()
        same_itids = dict()

        for tid, iso_trip_trans in ITID_Trs.items():
            freq_depth = ITID_Freq_depth[tid]
            frequency, depth = freq_depth[0], freq_depth[2]
            if frequency >= threshold:
                if frequency == max_freq and depth == min_depth:
                    candi_it_tr[tid] = itid_tr[tid]
                    ITID_Freq_depth[tid] = [frequency, '1', depth, None]
                    num_of_candidates += 1
                    same_itids[tid] = set(same_triples_dict[tid])

        candi_triples_list = []
        for iso_trip_lst in same_itids.values():
            if iso_trip_lst not in candi_triples_list:
                candi_triples_list.append(iso_trip_lst)

        for candi_triples in candi_triples_list:
            # Generate labels using string manipulation then convert to ID
            # label string: "_{(depth_chunk + 1)}:{label_no}"
            label_str = f"_{(depth_chunk + 1)}:{label_no}"
            label_id = self.mapper.get_id(label_str)
            
            for candi_triple in candi_triples:
                # candi_triple is TID (int)
                new_IID_str = f"_{(depth_chunk + 1)}:{candi_triple}"
                new_IID = self.mapper.get_id(new_IID_str)
                ChunkID_Label[new_IID] = label_id
            label_no += 1

        return candi_it_tr, same_itids

    def chunking(self, candidates, it_hash, itid_tr, threshold):
        self.depth_chunk += 1
        
        it_hash_temp = {k: v.copy() for k, v in it_hash.items()}
        itid_tr_temp = itid_tr.copy()

        Tr_IT_hash = dict()
        for triple, transaction in itid_tr_temp.items():
            if transaction in Tr_IT_hash:
                Tr_IT_hash[transaction] = Tr_IT_hash[transaction] + [triple]
            else:
                Tr_IT_hash[transaction] = [triple]

        number_of_chunk = 0
        for candidate in candidates:
            # Create new IID string then convert to ID
            new_IID_str = f"_{self.depth_chunk}:{candidate}"
            new_IID = self.mapper.get_id(new_IID_str)
            
            tr_of_candidate = itid_tr_temp[candidate]
            cand_subj_inst = it_hash_temp[candidate].subj_inst
            cand_obj_inst = it_hash_temp[candidate].obj_inst

            number_of_chunk += 1

            cand_triple = it_hash[candidate]
            left_i = cand_triple.subj_inst
            right_i = cand_triple.obj_inst
            left_cl = cand_triple.subj_cl
            right_cl = cand_triple.obj_cl

            # String processing for 'Chunking_Result' (keeping IDs, logic needs check)
            # Logic: if ':' in left_i... 
            # We need to check string representation
            left_i_str = self.mapper.get_str(left_i)
            right_i_str = self.mapper.get_str(right_i)
            
            if ':' in left_i_str:
                # split and take [1]
                parts = left_i_str.split(':')
                if len(parts) > 1:
                    left_i_str = parts[1]
                    # Note: We probably need the ID of this part?
                    # The original code stored string in Chunking_Result.
                    # We can store ID if we map back later.
                    # For now let's store ID corresponding to that part string.
                    left_i = self.mapper.get_id(left_i_str)
            
            if ':' in right_i_str:
                parts = right_i_str.split(':')
                if len(parts) > 1:
                    right_i_str = parts[1]
                    right_i = self.mapper.get_id(right_i_str)
            
            if left_cl in self.option_class_ids:
                left_i = left_cl
            if right_cl in self.option_class_ids:
                right_i = right_cl

            # Chunking_Result stores IDs. We will map them back to strings at the end.
            self.Chunking_Result[candidate] = [str(self.depth_chunk), left_i, cand_triple.prop, right_i, tr_of_candidate, '1']

            if tr_of_candidate in Tr_IT_hash:
                if candidate in Tr_IT_hash[tr_of_candidate]:
                     Tr_IT_hash[tr_of_candidate].remove(candidate)
            
            it_hash_temp.pop(candidate, None)
            itid_tr_temp.pop(candidate, None)

            if tr_of_candidate in Tr_IT_hash:
                for other_triple_id in Tr_IT_hash[tr_of_candidate]:
                    triple = it_hash_temp[other_triple_id]
                    share_subj_condition = triple.subj_inst == cand_subj_inst or triple.subj_inst == cand_obj_inst
                    share_obj_condition = triple.obj_inst == cand_subj_inst or triple.obj_inst == cand_obj_inst

                    if share_subj_condition and share_obj_condition:
                        triple.subj_inst = new_IID
                        triple.subj_cl = self.ChunkID_Label[new_IID]
                        it_hash_temp[other_triple_id] = triple
                    elif share_subj_condition:
                        triple.subj_inst = new_IID
                        triple.subj_cl = self.ChunkID_Label[new_IID]
                        it_hash_temp[other_triple_id] = triple
                    elif share_obj_condition:
                        triple.obj_inst = new_IID
                        triple.obj_cl = self.ChunkID_Label[new_IID]
                        it_hash_temp[other_triple_id] = triple
                    else:
                        pass

        candi_it_tr_t, same_itids = self.generate_candidate(it_hash=it_hash_temp, itid_tr=itid_tr_temp, threshold=threshold)

        if len(candi_it_tr_t) == 0:
            self.depth_chunk -= 1
            return
        else:
            sampled_candidate = list(candi_it_tr_t.keys())[0]
            next_candidates = same_itids[sampled_candidate]
            self.chunking(candidates=next_candidates, it_hash=it_hash_temp, itid_tr=itid_tr_temp, threshold=threshold)
            self.depth_chunk -= 1
            return

    def find_result(self, triple_id):
        self.chunk_stack.append(triple_id)

        if triple_id not in self.chunking_result_final:
            return 

        left = self.chunking_result_final[triple_id][1]
        right = self.chunking_result_final[triple_id][3]

        if left in self.chunking_result_final:
            self.find_result(left)
        if right in self.chunking_result_final:
            self.find_result(right)

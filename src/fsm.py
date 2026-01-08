import copy
import logging
from collections import defaultdict
from .utils import Triple, CustomError, log_data
from .config import OPTION_CLASS_LIST

class FSMEngine:
    def __init__(self):
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
                prop_dict[idx] = [dom, prop, ran]

                if dom not in cl_dict.keys():
                    cl_dict[dom] = cid
                    cid += 1
                if ran not in cl_dict.keys():
                    cl_dict[ran] = cid
                    cid += 1

                if dom == ran:
                    continue

                if dom in ont_graph.keys():
                    ont_graph[dom].append([idx, ran])
                else:
                    ont_graph[dom] = [[idx, ran]]

                if ran in ont_graph.keys():
                    ont_graph[ran].append([idx, dom])
                else:
                    ont_graph[ran] = [[idx, dom]]

        log_data('property_dict', prop_dict)
        log_data('ontology_graph', ont_graph)
        log_data('cl_dict', cl_dict)

        return prop_dict, ont_graph, cl_dict

    def find_ontology_paths(self, start, end, graph, max_depth):
        properties = set()
        stack = [(start, [], [start])]
        result = []

        while stack:
            n, path, cl = stack.pop()
            if len(path) >= max_depth:
                continue
            if n in end:
                result.append(path)
                for i in path:
                    properties.add(i)
            else:
                if n in graph:
                    for m in graph[n]:
                        if m[0] not in path:
                            stack.append((m[1], path + [m[0]], cl + [m[1]]))

        log_data('ontology_path_list', result)
        log_data('Number of schema-level paths', len(result))
        log_data('path_property_set', properties)

        return result, properties

    def store_triples(self, file, start_cl):
        start_instances = list()
        triple_dict = dict()
        prop_triples_dict = dict()

        with open(file, 'r') as f:
            while True:
                line = f.readline().rstrip()
                if not line:
                    break

                triple_id, subj_cl, subj_inst, prop, obj_cl, obj_inst = line.split('^')
                temp_triple = Triple(triple_id, subj_cl, subj_inst, prop, obj_cl, obj_inst)

                if obj_cl == start_cl and obj_inst not in start_instances:
                    start_instances.append(obj_inst)
                if subj_cl == start_cl and subj_inst not in start_instances:
                    start_instances.append(subj_inst)

                triple_dict[triple_id] = temp_triple

                if prop not in prop_triples_dict.keys():
                    prop_triples_dict[prop] = [temp_triple]
                else:
                    prop_triples_dict[prop].append(temp_triple)

        log_data('start_instance_list', start_instances)
        log_data('triple_dict', triple_dict)
        log_data('prop_triples_dict', prop_triples_dict)

        return start_instances, triple_dict, prop_triples_dict

    def find_triple_paths(self, start_cl, start_instance):
        logging.info('----------------- STARTING POINT: {a}, {b} -----------------'.format(a=start_cl, b=start_instance))
        triple_paths = []
        
        # Access class state instead of globals
        ontology_path_list = self.ontology_path_list
        property_dict = self.property_dict
        prop_triples_dict = self.prop_triples_dict

        for ont_path in ontology_path_list:
            logging.info('-----------------')
            logging.info('ONT_PATH: %s' % ont_path)
            queue = []

            first_property = property_dict[ont_path[0]]
            logging.info('%s %s' % (ont_path[0], first_property))
            first_subj_cl, first_prop, first_obj_cl = first_property

            if first_prop in prop_triples_dict.keys():
                for triple in prop_triples_dict[first_prop]:
                    if first_subj_cl == start_cl:
                        second_cl = first_obj_cl
                    else:
                        second_cl = first_subj_cl

                    if start_instance == triple.get_instance_of(start_cl):
                        queue.append((second_cl, triple.get_instance_of(second_cl), [triple.idx]))
            else:
                continue

            logging.info('- FIRST QUEUE: %s' % queue)

            for idx in range(1, len(ont_path)):
                next_property = property_dict[ont_path[idx]]
                logging.info('%s %s' % (ont_path[idx], next_property))
                next_subj_cl, next_prop, next_obj_cl = next_property

                if next_prop in prop_triples_dict.keys():
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
                logging.info('- NEXT QUEUE: %s' % queue)

            for q in queue:
                triple_paths.append(q[2])

        return triple_paths

    def get_chunking_type(self):
        prop_type_dict = {'either': [], 'both': []}

        for property_id in self.path_property_set:
            dom, prop, ran = self.property_dict[property_id]

            if (dom in OPTION_CLASS_LIST and ran not in OPTION_CLASS_LIST) or \
               (ran in OPTION_CLASS_LIST and dom not in OPTION_CLASS_LIST):
                prop_type_dict['either'].append(property_id)
            if dom in OPTION_CLASS_LIST and ran in OPTION_CLASS_LIST:
                prop_type_dict['both'].append(property_id)

        log_data('prop_chunk_type_dict', prop_type_dict)
        return prop_type_dict

    def make_freq_depth(self, triple, transactions):
        freq_depth = [len(transactions), '0', 0, '0']

        sbj_inst = triple.subj_inst
        obj_inst = triple.obj_inst
        sbj_depth = 0
        if ':' in sbj_inst:
            try:
                sbj_depth = int(sbj_inst[sbj_inst.find('_') + 1:sbj_inst.find(':')])
            except ValueError:
                pass
        obj_depth = 0
        if ':' in obj_inst:
            try:
                obj_depth = int(obj_inst[obj_inst.find('_') + 1:obj_inst.find(':')])
            except ValueError:
                pass

        freq_depth[2] = max(sbj_depth, obj_depth)
        return freq_depth

    def generate_candidate(self, it_hash, itid_tr, threshold):
        # Access class state
        ChunkID_Label = self.ChunkID_Label
        ITID_Freq_depth = self.ITID_Freq_depth
        depth_chunk = self.depth_chunk

        it_hash_temp = copy.deepcopy(it_hash)

        for tid, triple in it_hash_temp.items():
            sbj_cl = triple.subj_cl
            obj_cl = triple.obj_cl
            sbj_inst = triple.subj_inst
            obj_inst = triple.obj_inst

            if sbj_cl in OPTION_CLASS_LIST:
                triple.set_subj_inst(sbj_cl)
            if obj_cl in OPTION_CLASS_LIST:
                triple.set_obj_inst(obj_cl)

            if sbj_inst in ChunkID_Label:
                triple.set_subj_inst(ChunkID_Label[sbj_inst])
            if obj_inst in ChunkID_Label:
                triple.set_obj_inst((ChunkID_Label[obj_inst]))

        it_hash_temp_str = {tid: triple.str_code() for tid, triple in it_hash_temp.items()}

        iso_triples_dict = defaultdict(list)
        for tid, str_code in it_hash_temp_str.items():
            iso_triples_dict[str_code].append(tid)

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
            label = f"_{(depth_chunk + 1)}:{label_no}"
            for candi_triple in candi_triples:
                new_IID = f"_{(depth_chunk + 1)}:{candi_triple}"
                ChunkID_Label[new_IID] = label
            label_no += 1

        return candi_it_tr, same_itids

    def chunking(self, candidates, it_hash, itid_tr, threshold):
        self.depth_chunk += 1
        
        it_hash_temp = copy.deepcopy(it_hash)
        itid_tr_temp = copy.deepcopy(itid_tr)

        Tr_IT_hash = dict()
        for triple, transaction in itid_tr_temp.items():
            if transaction in Tr_IT_hash.keys():
                Tr_IT_hash[transaction] = Tr_IT_hash[transaction] + [triple]
            else:
                Tr_IT_hash[transaction] = [triple]

        number_of_chunk = 0
        for candidate in candidates:
            new_IID = f"_{self.depth_chunk}:{candidate}"
            tr_of_candidate = itid_tr_temp[candidate]
            cand_subj_inst = it_hash_temp[candidate].subj_inst
            cand_obj_inst = it_hash_temp[candidate].obj_inst

            number_of_chunk += 1

            cand_triple = it_hash[candidate]
            left_i = cand_triple.subj_inst
            right_i = cand_triple.obj_inst
            left_cl = cand_triple.subj_cl
            right_cl = cand_triple.obj_cl

            if ':' in left_i:
                left_i = left_i.split(':')[1]
            if ':' in right_i:
                right_i = right_i.split(':')[1]
            if left_cl in OPTION_CLASS_LIST:
                left_i = left_cl
            if right_cl in OPTION_CLASS_LIST:
                right_i = right_cl

            self.Chunking_Result[candidate] = [str(self.depth_chunk), left_i, cand_triple.prop, right_i, tr_of_candidate, '1']

            if tr_of_candidate in Tr_IT_hash: # Check existence before removal to be safe
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


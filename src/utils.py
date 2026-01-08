import logging

class Triple:
    """ 
    Optimized Triple class using Integer IDs.
    """
    __slots__ = ['idx', 'subj_cl', 'subj_inst', 'prop', 'obj_cl', 'obj_inst', 'same_code']

    def __init__(self, idx, subj_cl, subj_inst, prop, obj_cl, obj_inst):
        self.idx = idx            # int
        self.subj_cl = subj_cl    # int
        self.subj_inst = subj_inst# int
        self.prop = prop          # int
        self.obj_cl = obj_cl      # int
        self.obj_inst = obj_inst  # int
        self.same_code = 0        # int (0 means empty/none)

    def set_same_code(self, same_code):
        self.same_code = same_code
        return self

    def __str__(self):
        # For debugging, just print IDs. Use Mapper externally to see strings.
        return f"{self.idx}^{self.subj_cl}^{self.subj_inst}^{self.prop}^{self.obj_cl}^{self.obj_inst}^{self.same_code}"

    def print_triple(self):
        print(self.__str__())

    def get_instance_of(self, cl):
        if cl == self.subj_cl and cl != self.obj_cl:
            return self.subj_inst
        elif cl != self.subj_cl and cl == self.obj_cl:
            return self.obj_inst
        elif cl == self.subj_cl and cl == self.obj_cl:
            return self.subj_inst, self.obj_inst
        else:
            # We can't format class name here easily without mapper, just show ID
            raise CustomError(f"There is no instance of the class ID '{cl}' in this triple.")

    def set_subj_inst(self, new_subj_inst):
        self.subj_inst = new_subj_inst
        return self

    def set_obj_inst(self, new_obj_inst):
        self.obj_inst = new_obj_inst
        return self

    def copy(self):
        new_triple = Triple(self.idx, self.subj_cl, self.subj_inst, self.prop, self.obj_cl, self.obj_inst)
        new_triple.same_code = self.same_code
        return new_triple

    def tuple_code(self):
        """Replaces str_code. Returns a hashable tuple of IDs."""
        return (self.subj_cl, self.subj_inst, self.prop, self.obj_cl, self.obj_inst)

    def __eq__(self, other):
        return (self.subj_cl == other.subj_cl) \
                and (self.subj_inst == other.subj_inst) \
                and (self.prop == other.prop) \
                and (self.obj_inst == other.obj_inst) \
                and (self.obj_cl == other.obj_cl)

class CustomError(Exception):
    pass

def log_data(var_name, data):
    logging.info(var_name)
    # Logging complex structures with IDs might be unreadable without mapping back,
    # but for performance logs we keep it simple or convert if necessary.
    logging.info(str(data))

import logging

class Triple:
    """ 22.05.12 """

    def __init__(self, idx, subj_cl, subj_inst, prop, obj_cl, obj_inst):
        self.idx = idx
        self.subj_cl = subj_cl
        self.subj_inst = subj_inst
        self.prop = prop
        self.obj_cl = obj_cl
        self.obj_inst = obj_inst
        self.same_code = ''

    def set_same_code(self, same_code):
        self.same_code = same_code
        return self

    def __str__(self):
        return '^'.join([self.idx, self.subj_cl, self.subj_inst, self.prop, self.obj_cl, self.obj_inst, self.same_code])

    def print_triple(self):
        print('^'.join([self.idx, self.subj_cl, self.subj_inst, self.prop, self.obj_cl, self.obj_inst, self.same_code]))

    def get_instance_of(self, cl):
        if cl == self.subj_cl and cl != self.obj_cl:
            return self.subj_inst
        elif cl != self.subj_cl and cl == self.obj_cl:
            return self.obj_inst
        elif cl == self.subj_cl and cl == self.obj_cl:
            return self.subj_inst, self.obj_inst
        else:
            raise CustomError("There is no instance of the class '{a}' in this triple.".format(a=cl))

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

    def str_code(self):
        return '^'.join([self.subj_cl, self.subj_inst, self.prop, self.obj_cl, self.obj_inst])

    def __eq__(self, other):
        if (self.subj_cl == other.subj_cl) \
                and (self.subj_inst == other.subj_inst) \
                and (self.prop == other.prop) \
                and (self.obj_inst == other.obj_inst) \
                and (self.obj_cl == other.obj_cl):
            return True
        else:
            return False

class CustomError(Exception):
    pass

def log_data(var_name, data):
    logging.info(var_name)
    logging.info(data)


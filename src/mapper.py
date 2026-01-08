class StringMapper:
    def __init__(self):
        self.str_to_int = {}
        self.int_to_str = {}
        self.counter = 0

    def get_id(self, s):
        """Get integer ID for string s. Create new if not exists."""
        if s not in self.str_to_int:
            self.counter += 1
            self.str_to_int[s] = self.counter
            self.int_to_str[self.counter] = s
        return self.str_to_int[s]
    
    def get_str(self, idx):
        """Get string for integer ID idx."""
        return self.int_to_str.get(idx, str(idx))

    def load_schema_terms(self, terms):
        """Pre-load schema terms to ensure consistent IDs for classes/properties."""
        for term in terms:
            self.get_id(term)


class StringMapper:
    def __init__(self):
        self.str_to_int = {}
        # Optimization: Use list for int_to_str (index = ID). 
        # ID starts at 1, so index 0 is None.
        self.int_to_str = [None] 
        self.counter = 0

    def get_id(self, s):
        """Get integer ID for string s. Create new if not exists."""
        if s not in self.str_to_int:
            self.counter += 1
            self.str_to_int[s] = self.counter
            self.int_to_str.append(s)
        return self.str_to_int[s]
    
    def get_str(self, idx):
        """Get string for integer ID idx."""
        # Direct list access is faster than dict lookup
        if idx < len(self.int_to_str):
            return self.int_to_str[idx]
        return str(idx) # Fallback if something is wrong

    def load_schema_terms(self, terms):
        """Pre-load schema terms to ensure consistent IDs for classes/properties."""
        for term in terms:
            self.get_id(term)


class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        return self.num_records < 1024

    def write(self, value):
        if not self.has_capacity():
            raise ValueError("Page is full.")
        record_size = len(value)
        if record_size > 4096:
            raise ValueError("Record size too big.")
        start_index = self.num_records * 4
        self.data[start_index:start_index + record_size] = value
        self.num_records += 1
        

class BasePage:
    def __init__(self, num_columns):
        self.num_columns= num_columns
        self.records=[]
    def has_space(self):
        return len(self.records)+1<=4096
    def get_len(self):
        return len(self.records)

class TailPage:
    def __init__(self, num_columns):
        self.num_columns= num_columns
        self.records=[]

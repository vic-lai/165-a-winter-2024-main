
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        pass

    def write(self, value):
        self.num_records += 1
        pass

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

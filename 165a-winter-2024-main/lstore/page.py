import sys

PAGE_SIZE = 4096
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(PAGE_SIZE)

    def has_capacity(self):
        return self.num_records < 4

    def write(self, value):
        if not self.has_capacity():
            raise ValueError("Page is full.")
        record_size = sys.getsizeof(value)
        if record_size > PAGE_SIZE:
            raise ValueError("Record size too big.")
        start_index = self.num_records * 1024
        self.data[start_index:start_index + record_size] = value.to_bytes(record_size, byteorder="big")
        self.num_records += 1
        


class BasePage(Page):
    def __init__(self, num_columns):
        super().__init__()
        self.num_columns= num_columns
        self.records=[[] for _ in range(num_columns+4)]
    def has_space(self):
        return self.has_capacity()
    def get_len(self):
        return len(self.records[0])

class TailPage(Page):
    def __init__(self, num_columns):
        super().__init__()
        self.num_columns= num_columns
        self.records=[[] for _ in range(num_columns+5)]
    def has_space(self):
        return self.has_capacity()
    def get_len(self):
        return len(self.records[0])
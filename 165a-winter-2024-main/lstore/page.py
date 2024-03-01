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
    BASE_RID_COLUMN_INDEX = -5  # Index of the column storing BaseRID
    
    def __init__(self, num_columns):
        super().__init__()
        self.num_columns= num_columns
        self.records=[[] for _ in range(num_columns+4)]
        self.tps = 2 ** 64  # Initialize TPS to 2^64

    def has_space(self):
        return self.has_capacity()
    def get_len(self):
        return len(self.records[0])

    def apply_tail_record(self, tail_record):
        base_rid = tail_record[BasePage.BASE_RID_COLUMN_INDEX]
        for i in range(0, self.num_columns):
            self.records[i].append(tail_record[i])
        self.records[-4].append(base_rid) # indirection
        self.records[-3].append(tail_record[-3]) # rid
        self.records[-2].append(tail_record[-2]) # timestamp
        self.records[-1].append(tail_record[-1]) # schema
        self.tps = base_rid  # Update TPS with the last applied tail record's RID


class TailPage(Page):
    def __init__(self, num_columns):
        super().__init__()
        self.num_columns= num_columns
        self.records=[[] for _ in range(num_columns+5)]
    def has_space(self):
        return self.has_capacity()
    def get_len(self):
        return len(self.records[0])


class PageDirectory:
    def __init__(self):
        self.page_map = {}

    # Find the index of the base page within the specified page range.
    # def find_base_page_index(self, page_range_id, base_page_id):    
    #     if page_range_id in self.page_map:
    #         page_range = self.page_map[page_range_id]
    #         for index, base_page in enumerate(page_range.base_pages):
    #             if base_page.page_id == base_page_id:
    #                 return index
    #     return -1  # Base page index not found

    # # Update the base pages for the specified page range.
    # def update(self, page_range_id, new_base_pages):    
    #     if page_range_id in self.page_map:
    #         self.page_map[page_range_id].base_pages = new_base_pages
    #     else:
    #         self.page_map[page_range_id] = PageRange(new_base_pages)

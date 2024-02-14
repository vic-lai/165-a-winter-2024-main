from lstore.index import Index
from lstore.page import Page
from time import time

# -1 = delete
INDIRECTION_COLUMN = 0
# starts at 0, increments by 1
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {} # stores RIDS of pages
        self.index = Index(self)
        self.num_records = 0
        
        # a 2d-map of record metadata: indirection, rid, timestamp, schema
        self.record_metadata = {}
        # map of pages: 
            # "base" has a list of base page ranges for each col
                # {0(col0):[page 0, page 1], 1:[page 0], 2:[page 0]}
            # "tail" has a list of tail page ranges for each col
        self.pages = {"base": {}, 
                      "tail": {}}
        # create a base page for each column, put it in pages
        self.create_base_pages(num_columns)
        pass

    def __merge(self):
        print("merge is happening")
        pass

    def insert(self, indirection, base_rid, timestamp, schema_encoding, columns):
        self.record_metadata[base_rid] = [indirection, base_rid, timestamp, schema_encoding]
        columns = list(columns)
        for col, val in enumerate(columns):
            # write each col value onto the column's base page
            page_index, r_index = self.write_to_page(self, "base", col, val)
            self.page_directory[base_rid][col] = page_index, r_index
        self.num_records += 1


    def write_to_page(self, type, col, value):
        page_index = len(self.pages[type][col]) - 1
        # if last page has capacity
        if self.pages[type][col][page_index - 1].has_capacity():
            r_index = self.pages[type][col][page_index].write(value)
        # else no space
        else:
            # create a new base page
            page_index += 1
            self.create_page(type, col)
            r_index = self.pages[type][col][page_index].write(value)
        return page_index, r_index
 
    def create_base_pages(self, num_columns):
        for i in range(num_columns):
            self.create_page("base", i)

    def create_page(self, type, col):
        self.pages[type][col] = Page()

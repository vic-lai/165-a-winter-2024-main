from lstore.index import Index
from lstore.page import BasePage, TailPage
from time import time

INDIRECTION_COLUMN = 0
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
        self.base_page=[BasePage(num_columns)]
        self.tail_page=[TailPage(num_columns)]
        self.num_records = 0
        # self.base_page=[[]]
        # self.tail_page=[[]]
        pass

    def __merge(self):
        print("merge is happening")
        pass
 

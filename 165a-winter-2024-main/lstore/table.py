import threading
from lstore.index import Index
from lstore.page import BasePage, TailPage, PageDirectory
from time import time
import csv
import io
import collections
from lstore.page import Page
import copy
import time

BUFFER_SIZE = 100
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
        self.page_directory = PageDirectory() # stores RIDS of pages
        self.index = Index(self)
        self.base_page=[BasePage(num_columns)]
        self.tail_page=[TailPage(num_columns)]
        self.num_records = 0
        # self.base_page=[[]]
        # self.tail_page=[[]]
        self.record_metadata = {} # {rid:[indirection, rid, timestamp, schema_encoding]}
        self.bufferpool=Bufferpool()
        self.update_counter = 0 # counter to track number of updates
        self.merge_threshold = 500 # update threshoold for triggering merge
        self.merge_thread = None

    def getRecord(self, page_type, page_index, row_index):
        page = None
        if page_type == "base":
            page = self.base_page
        else:
            page = self.tail_page
        record = []
        for col in page[page_index].records:
            record.append(col[row_index])
        return record # returns [columns, (if tail: base_rid), indirection, rid, timestamp, schema encoding]
        
    def start_merge_thread(self):
        if self.merge_thread is None or not self.merge_thread.is_alive():
            self.merge_thread = threading.Thread(target=self.__merge)
            self.merge_thread.start()
        else:
            print("Merge thread is already running.")

    def __merge(self):
        print("merge is happening")
        
        base_pages_copy = self.copy_base_pages()

        for j, curr_tail_page in enumerate(reversed(self.tail_page)):
            for i in reversed(range(curr_tail_page.get_len())):
                # get tail record
                tail_record = self.getRecord("tail", j, i)
                tail_rid = tail_record[-3]
                # get base record
                base_rid = tail_record[-5]
                base_page_type, base_record_page_index, base_record_row_index, base_record_type = self.table.page_directory.page_map[base_rid]
                base_record = self.getRecord(base_page_type, base_record_page_index, base_record_row_index)
                # check if the tail page is the latest version via indirection
                if base_record[-4] == tail_record[-3]:
                    timestamp = time.time()
                    tail_record[-2] = timestamp
                    if (base_pages_copy[base_record_page_index].has_space()):
                        base_pages_copy[base_record_page_index].apply_tail_record(tail_record)
                        self.page_directory.page_map[tail_rid][0] = "page"
                        self.page_directory.page_map[tail_rid][1] = base_record_page_index
                        self.page_directory.page_map[tail_rid][2] = base_pages_copy[base_record_page_index].get_len()-1
                        self.page_directory.page_map[tail_rid][3] = "tail"
                    else:
                        # make a new base page
                        new_base_page=BasePage(self.table.num_columns)
                        # append it onto the table
                        base_pages_copy.append(new_base_page)
                        base_pages_copy[-1].apply_tail_record(tail_record)
                        self.page_directory.page_map[tail_rid][0] = "page"
                        self.page_directory.page_map[tail_rid][1] = len(base_pages_copy)-1
                        self.page_directory.page_map[tail_rid][2] = base_pages_copy[base_record_page_index].get_len()-1
                        self.page_directory.page_map[tail_rid][3] = "tail"
        self.base_page = base_pages_copy        


    def copy_base_pages(self):
        base_pages_copy = copy.deepcopy(self.base_page)
        return base_pages_copy
 

class Bufferpool:
    def __init__(self):
        self.buffer_size=BUFFER_SIZE
        self.dirty_page=set()
        self.pinned_count={}
        self.pages_loaded={}
        self.pageid=0
        self.lru=collections.deque()

    def read_page_disk(self,path):
        #checks for capacity
        if self.buffer_size <= len(self.pages_loaded):
            pageid=self.lru.pop()
            page=self.pages_loaded[pageid][0]
            path=self.pages_loaded[pageid][1]
            self.evict_page(pageid,path,page)
            
        file=open(path,'rb')
        content_io=io.BytesIO(file)
        content=csv.reader(io.TextIOWrapper(content_io,encoding='utf-8'))
        file.close()

        
        page=Page()
        page.data=content
        self.pages_loaded[self.pageid]=[page,path]
        self.lru.appendleft(pageid)
        self.pageid+=1
        return page
    
    def write_page_disk(page,path):
        file=open(path,'wb')
        file.write(page.data)
        file.close()

    def pin_page(self,pageid):
        if pageid in self.pinned_count:
            self.pinned_count[pageid]+=1
        else:
            self.pinned_count[pageid]=1

    def unpin_page(self,pageid):
        if pageid in self.pinned_count:
            if self.pinned_count[pageid] > 1:
                self.pinned_count[pageid]-=1
            else:
                del self.pinned_count[pageid]
    
    def evict_page(self,pageid,path,page):
        if pageid in self.pinned_count:
            return False
        if pageid in self.dirty_page:
            self.dirty_page.remove(pageid)
            self.write_page_disk(page,path)
        del self.pages_loaded[pageid]
        return True
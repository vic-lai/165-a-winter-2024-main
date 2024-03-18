"""
A data strucutre holding indices for various columns of a table. 
Key column should be indexd by default, other columns can be indexed through this object. 
Indices are usually B-Trees, but other data structures can be used as well.
"""

import threading
class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        self.lock=threading.Lock()
        pass

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        with self.lock:
            index = self.indices[column]
            if index is not None:
                return index.get(value, [])
            else:
                return []
        

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        with self.lock:
            index = self.indices[column]
            if index is not None:
                return [rid for value, rid in index.items() if begin <= value <= end]
            else:
                return[]

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        with self.lock:
            if self.indices[column_number] is None:
                self.indices[column_number] = {}
                for val in self.table.page_directory.values():
                    record = self.table.getRecord(val[0],val[1],val[2])
                    if(val[3]=="base"):
                        updated_rid = record[-4]
                        # skip record if deleted
                        if updated_record[-4] == -1:
                            continue
                        #get the latest record
                        updated_page_type, updated_record_page_index, updated_record_row_index, updated_record_type = self.table.page_directory.page_map[updated_rid]
                        updated_record = self.getRecord(updated_page_type, updated_record_page_index, updated_record_row_index)
                        if updated_record[column_number] not in self.indices[column_number]:
                            self.indices[column_number][updated_record[column_number]] = []
                        #append rid of base record
                        self.indices[column_number][updated_record[column_number]].append(record[-3])
                        

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass

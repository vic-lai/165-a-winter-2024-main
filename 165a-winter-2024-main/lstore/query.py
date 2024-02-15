from lstore.table import Table, Record
from lstore.index import Index
from lstore.page import BasePage, TailPage

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        pass

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        #check for locked
        # if primary key is locked
        for key in self.table.page_directory:
            page_index=self.table.page_directory[key][1]
            row_index=self.table.page_directory[key][2]
            key_index=self.table.key
            #check base page
            if self.table.page_directory[key][0]=="base":     
                if self.table.base_page[page_index].records[key_index][row_index]== primary_key:
                    del self.table.page_directory[key]
                    return True
            #check tail page
            elif self.table.page_directory[key][0]=="tail":
                if self.table.tail_page[page_index].records[key_index][row_index]== primary_key:
                    del self.table.page_directory[key]
                    return True

        return False
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    # def insert(self, *columns):
    #     schema_encoding = '0' * self.table.num_columns
        
    #     # get last base page
    #     current_base_page=self.table.base_page[-1]
    #     # get last record index
    #     row_index=current_base_page.get_len()-1
    #     # get last base page index
    #     page_index=len(self.table.base_page)-1

    #     # assign rid to new record
    #     rid=self.table.num_records

    #     # indirection
    #     indirection = rid
    #     # timestamp
    #     timestamp = None

    #     # if len(current_base_page)+1> 4096:
    #     #     self.table.base_page.append([])

    #     # if current base page does not have enough space
    #     if not current_base_page.has_space():
    #         # make a new base page 
    #         self.table.base_page.append(BasePage(self.table.num_columns))
    #         current_base_page = self.table.base_page[-1]
    #     record = Record(rid=rid, key=row_index, columns=list(columns))
    #     current_base_page.records.append([rid,row_index]+[record]+[schema_encoding])
    #     self.table.record_metadata[rid] = [indirection, rid, timestamp, schema_encoding]

    #     self.table.page_directory[rid]=("base", page_index,row_index)

    #     # increment num_records upon successful insertion
    #     self.table.num_records += 1
    #     return True
    
    def insert(self, *columns): 
        # assign new rid to new record
        rid=self.table.num_records
        # schema for new record
        schema_encoding = '0' * self.table.num_columns
        # timestamp: for future milestones
        timestamp = None
        # get last base page
        current_base_page=self.table.base_page[-1]
        # if current base page does not have space
        if not current_base_page.has_space():
            # make a new base page
            new_base_page=BasePage(self.table.num_columns)
            # append it onto the table
            self.table.base_page.append(new_base_page)
            # make that the current base page
            current_base_page = self.table.base_page[-1]
        # put record data in base page
        for i, value in enumerate(columns):
            current_base_page.records[i].append(value)
        
        #adding indirection, schema, rid to basepage
        current_base_page.records[-4].append(rid) # indirection
        current_base_page.records[-3].append(rid) # rid
        current_base_page.records[-2].append(timestamp) # timestamp
        current_base_page.records[-1].append(schema_encoding) # schema

        row_index=current_base_page.get_len()-1
        page_index=len(self.table.base_page)-1
        # FIXME: how do u know if the record is in the base page or tail page?
        self.table.page_directory[rid]=("base", page_index, row_index)

        # print("-- insert", self.getRecord("base", page_index, row_index))
        # print("   in ", page_index, row_index)
        # print(len(self.table.page_directory), self.table.page_directory.values())

        current_base_page.num_records += 1
        self.table.num_records += 1
        return True
    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """

    def getRecord(self, page_type, page_index, row_index):
        page = None
        if page_type == "base":
            page = self.table.base_page
        else:
            page = self.table.tail_page
        record = []
        for col in page[page_index].records:
            record.append(col[row_index])
        return record # returns [columns, indirection, rid, timestamp, schema encoding]

    def select(self, search_key, search_key_index, projected_columns_index):
        records = []
        for page_type, page_index, row_index in self.table.page_directory.values():
            record = self.getRecord(page_type, page_index, row_index)
            # print("record", record)
            if record[search_key_index] == search_key:
                projected_record = [record[i] for i in range(self.table.num_columns) if projected_columns_index[i]]
                records.append(Record(record[-3], record[-3], projected_record))
        return records

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        pass

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    # WIP: MAY HAVE MISSED SOMETHING, CHECK IF THIS IS RIGHT
    def update(self, primary_key, *columns):
        columns = list(columns)
        record_rid = None
        record = None
        # check for the record with that primary key
        for key, value in self.table.page_directory.items():
            page_type = value[0]
            page_index = value[1]
            row_index = value[2]
            page = None
            if page_type == "base":
                page = self.table.base_page
            else:
                page = self.table.tail_page
            if page[page_index].records[self.table.key][row_index] == primary_key:
                record = self.getRecord(page_type, page_index, row_index)
                record_rid = key
                break
        if not record:
            return False
        print("update record", record)
        # page_type, page_index, row_index = self.table.page_directory[record_rid]
        
        # new record, rid
        rid = self.table.num_records
        
        # get metadata of record
        record_metadata = record[-4: ]
        # previous indirection rid
        prev_indirection = record_metadata[0]

        # check if elements are changed, update schema
        schema = self.updateSchema(columns, record, record_metadata)
        print("finished schema")

        # timestamp
        timestamp = None

        # insert into tail page
        # get last tail page
        current_tail_page=self.table.tail_page[-1]
        # if current tail page does not have space
        if not current_tail_page.has_space():
            # make a new tail page
            new_tail_page=TailPage(self.table.num_columns)
            # append it onto the table
            self.table.tail_page.append(new_tail_page)
            # make that the current tail page
            current_tail_page = self.table.tail_page[-1]
        # put record data in tail page
        for i, value in enumerate(columns):
            current_tail_page.records[i].append(value)
        
        #adding indirection, schema, rid to basepage
        # indirection 
        current_tail_page.records[-4].append(prev_indirection) # indirection
        current_tail_page.records[-3].append(rid) # rid
        current_tail_page.records[-2].append(timestamp) # timestamp
        current_tail_page.records[-1].append(schema) # schema

        row_index=current_tail_page.get_len()-1
        page_index=len(self.table.tail_page)-1
        self.table.page_directory[rid]=("tail", page_index, row_index)

        # print("-- insert", self.getRecord("tail", page_index, row_index))
        # print("   in ", page_index, row_index)
        # print(len(self.table.page_directory), self.table.page_directory.values())

        current_tail_page.num_records += 1

        # change indirection of base record to the new tail record rid
        # base_rid = prev_indirection
        # while self.table.page_directory[base_rid][0] != "page":
        #     base_record = self.getRecord(self.table.page_directory[base_rid])
        #     base_rid = base_record[-4]
        # base_page_type, base_page_index, base_row_index = self.table.page_directory[base_rid]
        # self.table.base_page[base_page_index].records[-4][base_row_index] = rid

        # increment num_records upon successful update
        self.table.num_records += 1
        return True

    def updateSchema(self, columns, record, record_metadata):
        schema = record_metadata[3]
        ## get the indexes where schema[i] == 0, that means these columns stayed the same
        i = 0
        cols_unchanged = []
        while i != -1: # until find() does not find any 0's
            index_first_zero = schema[i:].find('0')
            if index_first_zero != -1:
                cols_unchanged.append(index_first_zero)
            i = index_first_zero + 1
        ## compare if these columns are being updated, update schema if they are
        for i in cols_unchanged:
            if columns[i] != record[2].columns[i]:
                schema[i] = '1'
        return schema

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        # initial value
        records_exist=False
        sum = 0
        # iterate through each key in the range and select the records that contain the key 
        for key in range(start_range, end_range + 1):
            records = self.select(key, self.table.key, [1] * self.table.num_columns)
            # if record exists select the column based on the index and add it to sum
            if records:
                value = records[0].columns[aggregate_column_index]
                sum += value
                records_exist=True
        if records_exist:
            return sum
        return False

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        pass

    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False

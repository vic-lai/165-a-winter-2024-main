"""
A data strucutre holding indices for various columns of a table. 
Key column should be indexd by default, other columns can be indexed through this object. 
Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        pass

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        index = self.indices[column]
        if index is not None:
            return index.get(value, [])
        else:
            return []
        

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        index = self.indices[column]
        if index is not None:
            return [rid for value, rid in index.items() if begin <= value <= end]
        else:
            return[]

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        if self.indices[column_number] is None:
            self.indices[column_number] = {}
            for page in self.table.page_directory.values():
                for record in page.records:
                    value = record.columns[column_number]
                    if value not in self.indices[column_number]:
                        self.indices[column_number][value] = []
                    self.indices[column_number][value].append(record.rid)
                    

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass

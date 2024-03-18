from lstore.table import Table
import os 
# import pickle


# def load_pickle_file(path):
#     f = open(path, "rb")
#     data = pickle.load(f)
#     f.close()
#     return data

# # file: file path of table's metadata
# # table: dictionary of metadata
# def update_pickle_file(file, data):
#     f = open(file, 'wb')
#     pickle.dump(data, f, pickle.HIGHEST_PROTOCOL)
#     f.close()
class Database():

    def __init__(self):
        self.tables = []
        self.path = ""
        pass

    # Not required for milestone1
    def open(self, path):
        if not os.path.exists(path=path):
            os.makedirs(path)
        self.path = path

    def close(self):
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        table = Table(name, num_columns, key_index, self.path)
        self.tables += table

        return table


    
    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        for i, table in enumerate(self.tables):
            if name == table.name:
                del self.tables[i]

                # WIP: fix this, this will just unmap table from memory which is already done automatically when code gets out of scope
                # del table
        

    
    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        # WIP: check if this is right
        for table in self.tables:
            if name == table.name:
                return table
        return {}

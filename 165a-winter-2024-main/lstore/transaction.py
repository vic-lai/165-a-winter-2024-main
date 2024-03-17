from lstore.table import Table, Record
from lstore.index import Index


class LockManager:
    def __init__(self):
        self.lock_map={}
        self.transaction_lock=0
    def exclusive_lock(self,rid,tid):
        if rid not in self.lock_map:
            self.lock_map[rid]={"exclusive":None,"shared":[]}
        exclusive_lock=self.lock_map[rid]["exclusive"]
        shared_locks=self.lock_map[rid]["shared"]
        if exclusive_lock is not None or len(shared_locks)!=0:
            return False
        self.lock_map[rid]["exclusive"]=tid
        self.transaction_lock+=1
        return True
    def unlock_exclusive_lock(self,rid,tid):
        if rid in self.lock_map:
            exclusive_lock=self.lock_map[rid]["exclusive"]
            if exclusive_lock == tid:
                self.lock_map[rid]["exclusive"]=None  
                self.transaction_lock-=1
    def shared_lock(self,rid,tid):
        if rid not in self.lock_map:
            self.lock_map[rid]={"exclusive":None,"shared":[]}
        shared_locks=self.lock_map[rid]["shared"]
        exclusive_lock=self.lock_map[rid]["exclusive"]
        if exclusive_lock is not None:
            return False
        shared_locks.add(tid)
        self.transaction_lock+=1
        return True
    def unlock_shared_lock(self,rid,tid):
        if rid in self.lock_map:
            shared_locks=self.lock_map[rid]["shared"]
            for i,lock_tid in enumerate(shared_locks):
                if lock_tid==tid:
                    shared_locks.pop(i)  
                    self.transaction_lock-=1     
            
class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, args))
        # use grades_table for aborting

        
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
        return self.commit()

    
    def abort(self):
        #TODO: do roll-back and any other necessary operations
        return False

    
    def commit(self):
        # TODO: commit to database
        return True


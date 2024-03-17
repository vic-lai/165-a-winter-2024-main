from lstore.table import Table, Record
from lstore.index import Index
import threading

class Latch:
    def __init__(self):
        self.latch=threading.Lock()
class LockManager:
    def __init__(self):
        self.lock_map={}
        self.transaction_lock=0

    def exclusive_lock(self,rid,tid):
        if rid not in self.lock_map:
            self.lock_map[rid]={"exclusive":None,"shared":[],"latch":Latch()}
        latch=self.lock_map[rid]["latch"]
        exclusive_lock=self.lock_map[rid]["exclusive"]
        shared_locks=self.lock_map[rid]["shared"]
        latch.acquire()
        with latch:
            if exclusive_lock is not None or len(shared_locks)!=0:
                latch.release()
                return False
            self.lock_map[rid]["exclusive"]=tid
            self.transaction_lock+=1
            latch.release()
            return True
    def unlock_exclusive_lock(self,rid,tid):
        if rid in self.lock_map:
            latch=self.lock_map[rid]["latch"]
            exclusive_lock=self.lock_map[rid]["exclusive"]
            if exclusive_lock == tid:
                latch.acquire()
                with latch:
                    self.lock_map[rid]["exclusive"]=None  
                    self.transaction_lock-=1
                    latch.release()
    def shared_lock(self,rid,tid):
        if rid not in self.lock_map:
            self.lock_map[rid]={"exclusive":None,"shared":[],"latch":Latch()}
        latch=self.lock_map[rid]["latch"]
        shared_locks=self.lock_map[rid]["shared"]
        exclusive_lock=self.lock_map[rid]["exclusive"]
        latch.acquire()
        with latch:
            if exclusive_lock is not None:
                latch.release()
                return False
            shared_locks.add(tid)
            self.transaction_lock+=1
            latch.release()
            return True
    def unlock_shared_lock(self,rid,tid):
        if rid in self.lock_map:
            shared_locks=self.lock_map[rid]["shared"]
            latch=self.lock_map[rid]["latch"]
            for i,lock_tid in enumerate(shared_locks):
                if lock_tid==tid:
                    latch.acquire()
                    with latch:
                        shared_locks.pop(i)  
                        self.transaction_lock-=1
                        latch.release()     
            
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
        for query, table, args in reversed(self.queries):
            self.lock_manager.transaction_lock -= 1
            self.lock_manager.transaction_lock = max(self.lock_manager.transaction_lock, 0)
            if hasattr(table, "rid"):
                self.lock_manager.unlock_exclusive_lock(table.rid, threading.current_thread().ident)
        return False

    
    def commit(self):
        # TODO: commit to database
        for query, table, args in self.queries:
            self.lock_manager.transaction_lock -= 1
            self.lock_manager.transaction_lock = max(self.lock_manager.transaction_lock, 0)
            if hasattr(table, "rid"):
                self.lock_manager.unlock_exclusive_lock(table.rid, threading.current_thread().ident)
        return True


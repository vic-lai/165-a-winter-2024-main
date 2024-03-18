from lstore.table import Table, Record
from lstore.index import Index
import threading

class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = []):
        self.stats = []
        self.transactions = transactions
        self.result = 0
        self.thread = None
        self.lock=threading.Lock()
        self.tid=0
        pass

    
    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        with self.lock:
            t.tid=self.tid+1
            self.tid+=1
        self.transactions.append(t)

        
    """
    Runs all transaction as a thread
    """
    def run(self):
        # here you need to create a thread and call __run
        x = threading.Thread(target = self.__run, args=())
        self.thread = x
        x.start()
    

    """
    Waits for the worker to finish
    """
    def join(self):
        if self.thread:
            self.thread.join()


    def __run(self):
        for transaction in self.transactions:
            # each transaction returns True if committed or False if aborted
            self.stats.append(transaction.run())
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))


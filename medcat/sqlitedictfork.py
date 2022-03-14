from sqlitedict import SqliteDict

"""
A descendant of SqliteDict that can be used after forking.
The idea is to delay opening the connection to the DB until
first use. i.e. it is no good if there some use of the DB before
forking.

Also, this will be a disaster if the DB is open in writeable modes

The constructor will still open the connection, but close it
straight away
"""


class SqliteDictFork(SqliteDict):

    def _openconnectiondecorator(func):
        # checks whether the connection is open, open if not
        def fixfork(self,  *args, **kwargs):
            if self.conn is None:
                self.conn = self._new_conn()
            return func(self,  *args, **kwargs)
        return fixfork
    
        
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # close the connection
        self.close()

    __len__ = _openconnectiondecorator(SqliteDict.__len__)
    
    __bool__ = _openconnectiondecorator(SqliteDict.__bool__)

    keys = _openconnectiondecorator(SqliteDict.keys)
    values = _openconnectiondecorator(SqliteDict.values)
    items = _openconnectiondecorator(SqliteDict.items)
    __contains__ = _openconnectiondecorator(SqliteDict.__contains__)
    __getitem__ = _openconnectiondecorator(SqliteDict.__getitem__)
    __setitem__ = _openconnectiondecorator(SqliteDict.__setitem__)
    __delitem__ = _openconnectiondecorator(SqliteDict.__delitem__)
    update = _openconnectiondecorator(SqliteDict.update)
    __iter__ = _openconnectiondecorator(SqliteDict.__iter__)
    clear = _openconnectiondecorator(SqliteDict.clear)

    
    
        
    

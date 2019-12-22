import configparser

from .utils import RWLock 

class Config( object ):
    
    def __init__(self):
        self._cnf = configparser.ConfigParser()
        self._lock = RWLock()
        self.section = "DEFAULT"
    
    def loadFile(self, fileName ):
        self._lock.writeAcquire()
        try:
            return self._cnf.read( fileName )
        finally:
            self._lock.writeRelease()
    
    def get(self, key):
        with self._lock:
            try:
                return self._cnf.get( self.section, key )
            except configparser.NoOptionError:
                raise KeyError( configparser.NoOptionError )

    def getBool(self, key):
        with self._lock:
            try:
                return self._cnf.getboolean( self.section, key )
            except configparser.NoOptionError:
                raise KeyError( configparser.NoOptionError )

    def set(self, key, value):
        self._lock.writeAcquire()
        try:
            self._cnf[ self.section ] = value
        finally:
            self._lock.writeRelease()

    def __getitem__(self, key):
        return self.get( key )

    def __setitem__(self, key, value):
        return self.set( key, value )

    def __enter__(self):
        self._lock.readAcquire()
    
    def __exit__(self, exc_type, exc_value, traceback):
        self._lock.readRelease()

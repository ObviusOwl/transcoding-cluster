import threading

class RWLock( object ):
    
    def __init__(self, lock=None ):
        self.cond = threading.Condition( lock=lock )
        self.readers = 0
    
    def readAcquire(self):
        with self.cond:
            self.readers += 1

    def readRelease(self):
        with self.cond:
            self.readers -= 1
            if self.readers == 0:
                self.cond.notify_all()

    def writeAcquire(self):
        self.cond.acquire()
        if self.readers > 0:
            self.cond.wait()

    def writeRelease(self):
        self.cond.release()

    def __enter__(self):
        self.readAcquire()
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.readRelease()

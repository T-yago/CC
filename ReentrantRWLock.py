import threading
from contextlib import contextmanager

class ReentrantRWLock():

    def __init__(self):
        self.w_lock = threading.Lock()
        self.num_r_lock = threading.Lock()
        self.num_r = 0
    
    def r_acquire(self):
        self.num_r_lock.acquire()
        self.num_r += 1
        if self.num_r == 1:
            self.w_lock.acquire()
        self.num_r_lock.release()
    
    def r_release(self):
        self.num_r_lock.acquire()
        self.num_r -= 1
        if self.num_r == 0:
            self.w_lock.release()
        self.num_r_lock.release()
    
    @contextmanager
    def r_locked(self):
        """ This method is designed to be used via the `with` statement. """
        try:
            self.r_acquire()
            yield
        finally:
            self.r_release()
    
    def w_acquire(self):
        self.w_lock.acquire()

    def w_release(self):
        self.w_lock.release()
    
    @contextmanager
    def w_locked(self):
        """ This method is designed to be used via the `with` statement. """
        try:
            self.w_acquire()
            yield
        finally:
            self.w_release()

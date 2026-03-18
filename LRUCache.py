from collections import OrderedDict
import threading


class LRUCache:
    """LRU Cache that holds n NUMBER of files"""
    def __init__(self, capacity):
        self.capacity = capacity
        self.cache = OrderedDict()
        self.lock = threading.Lock()    # need locking the file because there are multi-workers

        self.hits = 0
        self.misses = 0
        self.evicted = 0

    def get(self, key):
        """try getting data in cache"""
        with self.lock:
            if key not in self.cache:
                self.misses += 1
                return None
            
            self.cache.move_to_end(key) # move to end because most recently used
            self.hits += 1
            return self.cache[key]

    def put(self, key, value):
        """put data into cache"""
        with self.lock:
            self.cache[key] = value # insert new data to cache, value is file name (not whole path)

            # LRU eviction
            if len(self.cache) > self.capacity:
                self.cache.popitem(last=False)
                self.evicted += 1

    def stats(self):
        with self.lock:
            return {
                "hits": self.hits,
                "misses": self.misses,
                "evicted": self.evicted 
            }
        


class LRUCache_vol:
    """LRU Cache that holds n BYTES of files"""
    def __init__(self, capacity_vol):
        self.capacity = capacity_vol
        self.cache = OrderedDict()
        self.current_size = 0   # current size of cache volume
        self.lock = threading.Lock()

        self.hits = 0
        self.misses = 0
        self.evicted = 0
    
    def get(self, key):
        with self.lock:
            if key not in self.cache:
                self.misses += 1
                return None
            self.cache.move_to_end(key)
            self.hits += 1
            return self.cache[key][0]
    
    def put(self, key, value):
        data_size = len(value)  # num of bytes

        with self.lock:
            self.cache[key] = (value, data_size)
            self.current_size += data_size

            while self.current_size > self.capacity:
                k, (v, ds) = self.cache.popitem(last=False)
                self.current_size -= ds
                self.evicted += 1

    def stats(self):
        with self.lock:
            return {
                "hits": self.hits,
                "misses": self.misses,
                "evicted": self.evicted 
            }
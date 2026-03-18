from collections import OrderedDict
import threading

class twoQCache:
    def __init__(self, capacity):
        self.capacity = capacity
        self.a1_capacity = capacity // 4    # use 25% of capacity for A1
        self.am_capacity = capacity - self.a1_capacity  

        self.A1 = OrderedDict() # A1 cache is to store recently used or "cold" data
        self.Am = OrderedDict() # frequently used data can be promoted from A1 to Am, aka "hot" data
        self.lock = threading.Lock()

        self.hits = 0
        self.misses = 0
        self.evicted = 0

    def get(self, key):
        with self.lock:
            if key in self.Am:
                self.Am.move_to_end(key)
                self.hits += 1
                return self.Am[key]
            
            if key in self.A1:  # promote cold (A1) to hot (Am)
                value = self.A1.pop(key)
                self.Am[key] = value
                self.hits += 1
                if len(self.Am) > self.am_capacity:
                    self.Am.popitem(last=False)
                    self.evicted += 1
                return value
            
            self.misses +=1
            return None # cache miss
        
    # def _ensure_capacity(self):
    #     total = len(self.A1) + len(self.Am)
    #     if total > self.capacity:
    #         if self.A1:
    #             self.A1.popitem(last=False)
    #             self.evicted += 1
    #         elif self.Am:
    #             self.Am.popitem(last=False)
    #             self.evicted += 1
        
    def put(self, key, value):
        with self.lock:
            self.A1[key] = value # cold data, put to A1 first, if become hot promote to Am

            if len(self.A1) > self.a1_capacity:
                self.A1.popitem(last=False)
                self.evicted += 1

            # self._ensure_capacity()

    def stats(self):
        with self.lock:
            return {
                "hits": self.hits,
                "misses": self.misses,
                "evicted": self.evicted 
            }
    


class twoQCache_vol:
    def __init__(self, capacity_vol):
        self.capacity = capacity_vol
        self.a1_capacity = capacity_vol // 4
        self.am_capacity = capacity_vol - self.a1_capacity

        self.A1 = OrderedDict()
        self.Am = OrderedDict()
        self.lock = threading.Lock()

        self.a1_size = 0
        self.am_size = 0

        self.hits = 0
        self.misses = 0
        self.evicted = 0

    def _evict(self):
        while self.a1_size > self.a1_capacity:
            k, (v, ds) = self.A1.popitem(last=False)
            self.a1_size -= ds
            self.evicted += 1

        while self.am_size > self.am_capacity:
            k, (v, ds) = self.Am.popitem(last=False)
            self.am_size -= ds
            self.evicted += 1

        while self.a1_size + self.am_size > self.capacity:
            if self.A1:
                k, (v, ds) = self.A1.popitem(last=False)
                self.a1_size -= ds
                self.evicted += 1
            elif self.Am:
                k, (v, ds) = self.Am.popitem(last=False)
                self.am_size -= ds
                self.evicted += 1


    def get(self, key):
        with self.lock:
            if key in self.Am:
                self.Am.move_to_end(key)
                self.hits += 1
                return self.Am[key][0]

            if key in self.A1:
                value, data_size = self.A1.pop(key) # promote A1 to Am
                self.a1_size -= data_size
                self.Am[key] = (value, data_size)
                self.am_size += data_size
                self.hits += 1
                self._evict()
                # self._add_to_Am(key, value, data_size)
                return value

            self.misses +=1
            return None # cache miss

    def put(self, key, value):
        size = len(value)

        with self.lock:
            self.A1[key] = (value, size)
            self.a1_size += size
            self._evict()

    # def _add_to_Am(self, key, value, size):
    #     self.Am[key] = (value, size)
    #     self.am_size += size
    #     self._evict()

    def stats(self):
        with self.lock:
            return {
                "hits": self.hits,
                "misses": self.misses,
                "evicted": self.evicted 
            }
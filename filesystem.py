import os
import sys
import struct
import json
import stat
from collections import OrderedDict
from fuse import FUSE, Operations, FuseOSError
from errno import ENOENT
import time
import LRUCache
import twoQCache

# ==========================
# Hyperparams
# ==========================

## 2Q LRU Cache Size
# cache_size = 1656177836
cache_size = 18572720764 * 0.25

## Backend Selection (set at most one to True; setting all to False will activate the Basic backend)
is_twoQPackedMM = False # 06.
is_PackedMM = False # 05.
is_twoQPacked = False    # 04.
is_Packed = False   # 03.
is_twoQ = False # 02.

## Print cache latency?
print_lat = False





# ==============================
# 01. FUSE Only (AFS)
# ==============================

class BasicBackend:
    def __init__(self, root):
        self.root = root
        self.cache_misses = 0
        self.total_backend_time = 0.0   # accumulated latency

    def _real_path(self, path):
        return os.path.join(self.root, path.lstrip("/"))

    def getattr(self, path):
        real_path = self._real_path(path)
        try:
            st = os.lstat(real_path)
            return {
                'st_atime': st.st_atime,
                'st_ctime': st.st_ctime,
                'st_gid': st.st_gid,
                'st_mode': st.st_mode,
                'st_mtime': st.st_mtime,
                'st_nlink': st.st_nlink,
                'st_size': st.st_size,
                'st_uid': st.st_uid,
            }
        except FileNotFoundError:
            raise FuseOSError(ENOENT)

    def readdir(self, path):
        real_path = self._real_path(path)
        return ['.', '..'] + os.listdir(real_path)
    
    def read(self, path, size, offset):
        start_backend = time.perf_counter()

        ## data read
        real_path = self._real_path(path)
        with open(real_path, "rb") as f:
            data = f.read() # read disk
        self.cache_misses += 1
        end_backend = time.perf_counter()

        backend_latency = end_backend - start_backend   # latency for current read
        self.total_backend_time += backend_latency

        if print_lat is True:
            print(f"Latency: {backend_latency*1000:.4f} ms | "
                #   f"Avg backend latency: "
                #   f"{(self.total_backend_time/(self.cache_hits+self.cache_misses))*1000:.3f} ms | "
                f"Accum. Latency: {self.total_backend_time*1000:.3f} ms | "
                f"Miss={self.cache_misses} | "
                f"File: {path}")

        return data[offset:offset+size]


# ==============================
# 02. FUSE + 2Q LRU Cache
# ==============================

class CachedBackend:
    def __init__(self, root):
        self.root = root
        # self.cache = LRUCache.LRUCache_vol(cache_size)
        self.cache = twoQCache.twoQCache_vol(cache_size)
        self.cache_hits = 0
        self.cache_misses = 0
        self.cache_evicted = 0  # num of data evicted under LRU policy
        self.total_backend_time = 0.0   # accumulated latency
        self.cache_miss_latency = 0.0   # for checking the average latency during a cache miss

    def _real_path(self, path):
        return os.path.join(self.root, path.lstrip("/"))

    def getattr(self, path):
        real_path = self._real_path(path)
        try:
            st = os.lstat(real_path)
            return {
                'st_atime': st.st_atime,
                'st_ctime': st.st_ctime,
                'st_gid': st.st_gid,
                'st_mode': st.st_mode,
                'st_mtime': st.st_mtime,
                'st_nlink': st.st_nlink,
                'st_size': st.st_size,
                'st_uid': st.st_uid,
            }
        except FileNotFoundError:
            raise FuseOSError(ENOENT)


    def readdir(self, path):
        real_path = self._real_path(path)
        # print(f"readdir triggered: {real_path}")
        return ['.', '..'] + os.listdir(real_path)
    
    def read(self, path, size, offset):
        start_backend = time.perf_counter()

        ## data read
        data = self.cache.get(path)
        if data is None:    # cache miss
            real_path = self._real_path(path)
            cache_status = "MISS"
            source = "DISK"
            with open(real_path, "rb") as f:
                data = f.read() # read disk
            self.cache.put(path, data)  # insert data into cache
        else:
            cache_status = "HIT"
            source = "CACHE"

        end_backend = time.perf_counter()

        if print_lat is True:
        ## collect results
            
            stats = self.cache.stats()
            self.cache_hits = stats["hits"]
            self.cache_misses = stats["misses"]
            self.cache_evicted = stats["evicted"]
            total = self.cache_hits + self.cache_misses
            hit_rate = self.cache_hits / total if total > 0 else 0
            backend_latency = end_backend - start_backend   # latency for current read
            self.total_backend_time += backend_latency
            if cache_status == "MISS":
                self.cache_miss_latency += backend_latency
            print(f"CACHE <{cache_status}>, source={source} | "
                f"Latency: {backend_latency*1000:.4f} ms | "
                #   f"Avg backend latency: "
                #   f"{(self.total_backend_time/(self.cache_hits+self.cache_misses))*1000:.3f} ms | "
                f"Accum. Latency: {self.total_backend_time*1000:.3f} ms | "
                f"Accum. Latency on Miss: {1000*self.cache_miss_latency:.3f} ms | "
                f"Hit={self.cache_hits}, Miss={self.cache_misses}, Evicted={self.cache_evicted}, Hit Ratio={hit_rate:.3f} | "
                f"File: {path}")

        return data[offset:offset+size] 

# ===================================
# 03. FUSE + Single Type Packed
# ===================================

class PackedBackend:
    MAGIC = b'VLMFS001'

    def __init__(self, pack_path):
        self.pack_path = pack_path
        self.pack_file = open(pack_path, "rb")
        self.pack_fd = os.open(pack_path, os.O_RDONLY)
        self.index = {} # each file index is 180-220 bytes, 15k images is at most 3.3 MB, so no need to worry about memory space

        self._load_index()

    def _load_index(self):
        self.pack_file.seek(0)

        magic = self.pack_file.read(8)  # MAGIC is a user-defined string when packing all images to one pack file
                                        # see tools/udacity_preprocess.py
        if magic != self.MAGIC:
            raise RuntimeError("Invalid pack format")
        
        version = struct.unpack("I", self.pack_file.read(4))[0]
        file_count = struct.unpack("I", self.pack_file.read(4))[0]
        index_offset = struct.unpack("Q", self.pack_file.read(8))[0]

        self.pack_file.seek(index_offset)

        ## load all indices
        for _ in range(file_count):
            name_len = struct.unpack("H", self.pack_file.read(2))[0]
            name = self.pack_file.read(name_len).decode()
            offset = struct.unpack("Q", self.pack_file.read(8))[0]
            size = struct.unpack("Q", self.pack_file.read(8))[0]

            self.index["/" + name] = (offset, size)
        #     print(f'loading file {_+1}/{file_count}')
        # print('load complete')

    def getattr(self, path):
        if path == "/":
            return dict(st_mode=(0o755 | 0o040000), st_nlink=2)

        if path in self.index:
            _, size = self.index[path]
            return dict(
                st_mode=(0o444 | 0o100000),
                st_nlink=1,
                st_size=size
            )

        raise FuseOSError(ENOENT)

    def readdir(self, path):
        yield "."
        yield ".."

        if path == "/":
            for filename in self.index.keys():
                yield filename[1:]

    def read(self, path, size, offset):
        if path not in self.index:
            raise FuseOSError(ENOENT)

        file_offset, file_size = self.index[path]
        # print(file_offset, file_size, path)
        
        # If offset beyond file size
        if offset >= file_size:
            return b''

        # Clamp read size
        read_size = min(size, file_size - offset)

        # Compute absolute position in pack file
        absolute_pos = file_offset + offset

        return os.pread(self.pack_fd, read_size, absolute_pos)
        # Single seek + single read
        # self.pack_file.seek(absolute_pos)
        # return self.pack_file.read(read_size)

# ===================================
# 04. FUSE + Single Type Packed + 2Q
# ===================================
class twoQPackedBackend:
    MAGIC = b'VLMFS001'

    def __init__(self, pack_path):
        self.pack_path = pack_path
        self.pack_file = open(pack_path, "rb")
        self.pack_fd = os.open(pack_path, os.O_RDONLY)
        self.index = {} # each file index is 180-220 bytes, 15k images is at most 3.3 MB, so no need to worry about memory space

        self._load_index()
        self.cache = twoQCache.twoQCache_vol(cache_size)
        self.cache_hits = 0
        self.cache_misses = 0
        self.cache_evicted = 0
        self.total_backend_time = 0.0
        self.cache_miss_latency = 0.0   # for checking the average latency during a cache miss


    def _load_index(self):
        self.pack_file.seek(0)

        magic = self.pack_file.read(8)  # MAGIC is a user-defined string when packing all images to one pack file
                                        # see tools/udacity_preprocess.py
        if magic != self.MAGIC:
            raise RuntimeError("Invalid pack format")
        
        version = struct.unpack("I", self.pack_file.read(4))[0]
        file_count = struct.unpack("I", self.pack_file.read(4))[0]
        index_offset = struct.unpack("Q", self.pack_file.read(8))[0]

        self.pack_file.seek(index_offset)

        ## load all indices
        for _ in range(file_count):
            name_len = struct.unpack("H", self.pack_file.read(2))[0]
            name = self.pack_file.read(name_len).decode()
            offset = struct.unpack("Q", self.pack_file.read(8))[0]
            size = struct.unpack("Q", self.pack_file.read(8))[0]

            self.index["/" + name] = (offset, size)
        #     print(f'loading file {_+1}/{file_count}')
        # print('load complete')

    def getattr(self, path):
        if path == "/":
            return dict(st_mode=(0o755 | 0o040000), st_nlink=2)

        if path in self.index:
            _, size = self.index[path]
            return dict(
                st_mode=(0o444 | 0o100000),
                st_nlink=1,
                st_size=size
            )

        raise FuseOSError(ENOENT)

    def readdir(self, path):
        yield "."
        yield ".."

        if path == "/":
            for filename in self.index.keys():
                yield filename[1:]

    def read(self, path, size, offset):
        start_backend = time.perf_counter()

        if path not in self.index:
            raise FuseOSError(ENOENT)

        file_offset, file_size = self.index[path]
        # print(file_offset, file_size, path)
        
        # If offset beyond file size
        if offset >= file_size:
            return b''
        
        data = self.cache.get(path)
        if data is None:
            cache_status = "MISS"
            source = "PACK"

            # Clamp read size
            read_size = min(size, file_size - offset)

            # Compute absolute position in pack file
            absolute_pos = file_offset + offset
            data = os.pread(self.pack_fd, read_size, absolute_pos)
            self.cache.put(path, data)

        else:
            cache_status = "HIT"
            source = "CACHE"
            
        end_backend = time.perf_counter()
        backend_latency = end_backend - start_backend
        self.total_backend_time += backend_latency
        if cache_status == "MISS":
            self.cache_miss_latency += backend_latency

        if print_lat is True:
            stats = self.cache.stats()
            self.cache_hits = stats["hits"]
            self.cache_misses = stats["misses"]
            self.cache_evicted = stats["evicted"]
            total = self.cache_hits + self.cache_misses
            hit_rate = self.cache_hits / total if total > 0 else 0.0

            print(
                f"PACKED-2Q <{cache_status}>, source={source} | "
                f"Latency: {backend_latency*1000:.4f} ms | "
                f"Accum. Latency: {self.total_backend_time*1000:.3f} ms | "
                f"Accum. Latency on Miss: {1000*self.cache_miss_latency:.3f} ms | "
                f"Hit={self.cache_hits}, Miss={self.cache_misses}, "
                f"Evicted={self.cache_evicted}, Hit Ratio={hit_rate:.3f} | "
                f"File: {path}"
            )

        return data[offset:offset + size]




# ===================================
# 05. FUSE + MultiModal Packed
# ===================================
class PackedMMBackend:
    def __init__(self, pack_path, index_path):
        # self.pack_file = open(pack_path, "rb")
        self.pack_fd = os.open(pack_path, os.O_RDONLY)
        
        with open(index_path, "r", encoding="utf-8") as f:
            self.index = json.load(f)

        self.samples = sorted(self.index.keys(), key=lambda x: int(x))

        # Map visible filesystem filenames -> sample id
        self.image_files = {}
        self.audio_files = {}

        for s in self.samples:
            entry = self.index[s]

            # img_name = entry["image_file"]
            # aud_name = entry["audio_file"]

            self.image_files[entry["image_file"]] = s
            self.audio_files[entry["audio_file"]] = s

    def getattr(self, path):
        if path in [
            "/",
            "/dataset",
            "/dataset/coco",
            "/dataset/coco/images",
            "/dataset/audiocaps",
            "/dataset/audiocaps/audio",
        ]:
            return dict(
                st_mode=(stat.S_IFDIR | 0o755),
                st_nlink=2
            )

        # image file
        if path.startswith("/dataset/coco/images/"):
            name = os.path.basename(path)

            if name not in self.image_files:
                raise FuseOSError(ENOENT)

            sample = self.image_files[name]
            size = self.index[sample]["image_size"]

            return dict(
                st_mode=(stat.S_IFREG | 0o444),
                st_nlink=1,
                st_size=size
            )

        # audio file
        if path.startswith("/dataset/audiocaps/audio/"):
            name = os.path.basename(path)

            if name not in self.audio_files:
                raise FuseOSError(ENOENT)

            sample = self.audio_files[name]
            size = self.index[sample]["audio_size"]

            return dict(
                st_mode=(stat.S_IFREG | 0o444),
                st_nlink=1,
                st_size=size
            )

        raise FuseOSError(ENOENT)

    def readdir(self, path):
        if path == "/":
            return [".", "..", "dataset"]

        if path == "/dataset":
            return [".", "..", "coco", "audiocaps"]

        if path == "/dataset/coco":
            return [".", "..", "images"]

        if path == "/dataset/audiocaps":
            return [".", "..", "audio"]

        if path == "/dataset/coco/images":
            return [".", ".."] + list(self.image_files.keys())

        if path == "/dataset/audiocaps/audio":
            return [".", ".."] + list(self.audio_files.keys())

        return []

    def read(self, path, size, offset):
        if path.startswith("/dataset/coco/images/"):
            name = os.path.basename(path)

            if name not in self.image_files:
                raise FuseOSError(ENOENT)

            sample = self.image_files[name]
            entry = self.index[sample]
            data_offset = entry["image_offset"]
            data_size = entry["image_size"]

        elif path.startswith("/dataset/audiocaps/audio/"):
            name = os.path.basename(path)

            if name not in self.audio_files:
                raise FuseOSError(ENOENT)

            sample = self.audio_files[name]
            entry = self.index[sample]
            data_offset = entry["audio_offset"]
            data_size = entry["audio_size"]

        else:
            raise FuseOSError(ENOENT)

        if offset >= data_size:
            return b""

        read_size = min(size, data_size - offset)
        absolute_offset = data_offset + offset
        return os.pread(self.pack_fd, read_size, absolute_offset)
    
        # self.pack_file.seek(data_offset + offset)
        # return self.pack_file.read(read_size)




# ===================================
# 06. FUSE + MultiModal Packed + 2Q
# ===================================
class twoQPackedMMBackend:
    def __init__(self, pack_path, index_path):
        self.pack_fd = os.open(pack_path, os.O_RDONLY)

        with open(index_path, "r", encoding="utf-8") as f:
            self.index = json.load(f)

        self.samples = sorted(self.index.keys(), key=lambda x: int(x))

        # visible filesystem filename -> sample id
        self.image_files = {}
        self.audio_files = {}

        for s in self.samples:
            entry = self.index[s]
            self.image_files[entry["image_file"]] = s
            self.audio_files[entry["audio_file"]] = s

        # 2Q cache stores full file payloads keyed by visible path
        self.cache = twoQCache.twoQCache_vol(cache_size)
        self.cache_hits = 0
        self.cache_misses = 0
        self.cache_evicted = 0
        self.total_backend_time = 0.0

    def _lookup_entry(self, path):
        if path.startswith("/dataset/coco/images/"):
            name = os.path.basename(path)
            if name not in self.image_files:
                raise FuseOSError(ENOENT)

            sample = self.image_files[name]
            entry = self.index[sample]
            return {
                "offset": entry["image_offset"],
                "size": entry["image_size"],
            }

        elif path.startswith("/dataset/audiocaps/audio/"):
            name = os.path.basename(path)
            if name not in self.audio_files:
                raise FuseOSError(ENOENT)

            sample = self.audio_files[name]
            entry = self.index[sample]
            return {
                "offset": entry["audio_offset"],
                "size": entry["audio_size"],
            }

        raise FuseOSError(ENOENT)

    def getattr(self, path):
        if path in [
            "/",
            "/dataset",
            "/dataset/coco",
            "/dataset/coco/images",
            "/dataset/audiocaps",
            "/dataset/audiocaps/audio",
        ]:
            return dict(
                st_mode=(stat.S_IFDIR | 0o755),
                st_nlink=2
            )

        if path.startswith("/dataset/coco/images/"):
            name = os.path.basename(path)
            if name not in self.image_files:
                raise FuseOSError(ENOENT)

            sample = self.image_files[name]
            size = self.index[sample]["image_size"]
            return dict(
                st_mode=(stat.S_IFREG | 0o444),
                st_nlink=1,
                st_size=size
            )

        if path.startswith("/dataset/audiocaps/audio/"):
            name = os.path.basename(path)
            if name not in self.audio_files:
                raise FuseOSError(ENOENT)

            sample = self.audio_files[name]
            size = self.index[sample]["audio_size"]
            return dict(
                st_mode=(stat.S_IFREG | 0o444),
                st_nlink=1,
                st_size=size
            )

        raise FuseOSError(ENOENT)

    def readdir(self, path):
        if path == "/":
            return [".", "..", "dataset"]

        if path == "/dataset":
            return [".", "..", "coco", "audiocaps"]

        if path == "/dataset/coco":
            return [".", "..", "images"]

        if path == "/dataset/audiocaps":
            return [".", "..", "audio"]

        if path == "/dataset/coco/images":
            return [".", ".."] + list(self.image_files.keys())

        if path == "/dataset/audiocaps/audio":
            return [".", ".."] + list(self.audio_files.keys())

        return []

    def read(self, path, size, offset):
        start_backend = time.perf_counter()

        meta = self._lookup_entry(path)
        data_size = meta["size"]

        if offset >= data_size:
            return b""

        # Try cache first; cache stores the whole file by visible path
        data = self.cache.get(path)
        if data is None:
            cache_status = "MISS"
            source = "PACK"

            # Read the full object once from the packed file using pread()
            data = os.pread(self.pack_fd, data_size, meta["offset"])
            self.cache.put(path, data)
        else:
            cache_status = "HIT"
            source = "CACHE"

        end_backend = time.perf_counter()
        backend_latency = end_backend - start_backend
        self.total_backend_time += backend_latency

        if print_lat is True:
            stats = self.cache.stats()
            self.cache_hits = stats["hits"]
            self.cache_misses = stats["misses"]
            self.cache_evicted = stats["evicted"]
            total = self.cache_hits + self.cache_misses
            hit_rate = self.cache_hits / total if total > 0 else 0.0

            print(
                f"PACKED-2Q <{cache_status}>, source={source} | "
                f"Latency: {backend_latency*1000:.4f} ms | "
                f"Accum. Latency: {self.total_backend_time*1000:.3f} ms | "
                f"Hit={self.cache_hits}, Miss={self.cache_misses}, "
                f"Evicted={self.cache_evicted}, Hit Ratio={hit_rate:.3f} | "
                f"File: {path}"
            )

        return data[offset:offset + size]


# ==============================
# Front-End (FUSE Adapter)
# ==============================
class LLMFS(Operations):
    # front end operation class that takes a backend logic
    # (easier to try different backend strategy)
    def __init__(self, backend):
        self.backend = backend

    def getattr(self, path, fh=None):
        return self.backend.getattr(path)

    def readdir(self, path, fh):
        return self.backend.readdir(path)

    def read(self, path, size, offset, fh):
        return self.backend.read(path, size, offset)


# ==============================
# Mount
# ==============================

if __name__ == "__main__":
    backend_selection = [is_twoQPackedMM, is_PackedMM, is_twoQPacked, is_Packed, is_twoQ]
    if sum(backend_selection) > 1:
        raise ValueError(f"Error: more than one backend is selected.")

    # 2Q + Packed (Multimodal) >> 06.
    if is_twoQPackedMM is True:
        print('multi 2Q + packed activated')
        print(f'cache size: {cache_size/1024/1024/1024:.1f}GB')
        pack_path = "./packed_file/multimodal.pack"
        index_path = "./packed_file/multimodal_index.json"
        backend = twoQPackedMMBackend(pack_path, index_path)
        mount_point = "mountpoint"

    # Packed (Multimodal) >> 05. 
    elif is_PackedMM is True:
        print('multi packed activated')
        pack_path = "./packed_file/multimodal.pack"
        index_path = "./packed_file/multimodal_index.json"
        backend = PackedMMBackend(pack_path, index_path)
        mount_point = "mountpoint"

    # 2Q + Packed (Single Modal) >> 04.
    elif is_twoQPacked is True:
        print('single 2Q + packed activated')
        print(f'cache size: {cache_size/1024/1024/1024:.1f}GB')
        pack_path = "./packed_file/dataset.pack"
        backend = twoQPackedBackend(pack_path)
        mount_point = "./mountpoint/dataset/object-detection-crowdai/images"

    # Packed (Single Modal) >> 03.
    elif is_Packed is True:
        print('single packed activated')
        pack_path = "./packed_file/dataset.pack"
        backend = PackedBackend(pack_path)
        mount_point = "./mountpoint/dataset/object-detection-crowdai/images"

    # 2Q LRU (Multimodal & Single Modal) >> 02.
    elif is_twoQ is True:
        print('2Q activated')
        print(f'cache size: {cache_size/1024/1024/1024:.1f}GB')
        storage_root = "./storage"
        backend = CachedBackend(storage_root)
        mount_point = "mountpoint"

    # FUSE + Basic Backend (AFS in macos) >> 01.
    else:
        print('basic activated')
        storage_root = "./storage"
        backend = BasicBackend(storage_root)
        mount_point = "mountpoint"       
    
    FUSE(LLMFS(backend), mount_point, foreground=True, direct_io=True)
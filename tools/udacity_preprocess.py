import os
import struct
import subprocess

MAGIC = b'VLMFS001' # magic word (user-defined) that is used for matching pack file

def pack_dataset(input_dir, output_file):
    files = sorted(os.listdir(input_dir))
    index = {}
    
    with open(output_file, "wb") as out:
        out.write(b'\x00' * 32)  # reserve header
        
        for fname in files:
            path = os.path.join(input_dir, fname)
            with open(path, "rb") as f:
                data = f.read()
            
            offset = out.tell()
            
            name_bytes = fname.encode()
            out.write(struct.pack("H", len(name_bytes)))
            out.write(name_bytes)
            out.write(struct.pack("Q", len(data)))
            out.write(data)
            
            index[fname] = (offset, len(data))
            
            # 4KB align
            pad = (4096 - (out.tell() % 4096)) % 4096
            out.write(b'\x00' * pad)
        
        index_offset = out.tell()
        
        for fname, (offset, size) in index.items():
            name_bytes = fname.encode()
            out.write(struct.pack("H", len(name_bytes)))
            out.write(name_bytes)
            out.write(struct.pack("Q", offset))
            out.write(struct.pack("Q", size))
        
        # Write header
        out.seek(0)
        out.write(MAGIC)
        out.write(struct.pack("I", 1))
        out.write(struct.pack("I", len(index)))
        out.write(struct.pack("Q", index_offset))
        out.write(b'\x00' * 8)

if __name__ == "__main__":
    INPUT_DIR = "./storage/dataset/object-detection-crowdai/images"
    OUTPUT_FILE = "./storage/dataset/object-detection-crowdai/dataset.pack"
    pack_dataset(INPUT_DIR, OUTPUT_FILE)

    # !wget https://s3.amazonaws.com/udacity-sdc/annotations/object-detection-crowdai.tar.gz  # download the image data
    # !tar xf object-detection-crowdai.tar.gz
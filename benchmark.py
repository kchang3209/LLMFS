import os
import time
import json
import random
import multiprocessing as mp
import pandas as pd

# ==========================
# Config
# ==========================

DATASET_PATH = "./mountpoint/dataset/object-detection-crowdai/images"
RESULT_PATH = "./benchmark_result.jsonl"

NUM_WORKERS = 4
BATCH_SIZE = 32
EPOCHS = 100
SHUFFLE = True

random.seed(237)

# ==========================
# Worker Function
# ==========================

def worker_process(worker_id, file_list, batch_size):

    for i in range(0, len(file_list), batch_size):
        batch = file_list[i:i+batch_size]

        for img_path in batch:
            with open(img_path, "rb") as f:
                f.read()

# ==========================
# Main Benchmark
# ==========================

def main():
    
    image_files = [
        os.path.join(DATASET_PATH, f)
        for f in os.listdir(DATASET_PATH)
        if f.lower().endswith(".jpg")
    ]
    dataset_size = len(image_files)
    print("\n============= Config =============")
    print("Workers:", NUM_WORKERS)
    print("Batch size:", BATCH_SIZE)
    print("Epochs:", EPOCHS)
    print("Shuffle:", SHUFFLE)
    # print("="*34,'\n')
    total_time = 0

    _ = pd.read_csv("./storage/dataset/object-detection-crowdai/labels.csv")   # simulating csv file read
    with open(RESULT_PATH, 'w') as outfile:
        for epoch in range(EPOCHS):
            start = time.perf_counter()

            if SHUFFLE:
                random.shuffle(image_files)

            # Split dataset across workers
            chunks = [[] for _ in range(NUM_WORKERS)]
            for i, img_path in enumerate(image_files):
                chunks[i % NUM_WORKERS].append(img_path)

            processes = []

            for wid in range(NUM_WORKERS):
                p = mp.Process(
                    target=worker_process,
                    args=(
                        wid,
                        chunks[wid],
                        BATCH_SIZE,
                    )
                )
                p.start()
                processes.append(p)
            for p in processes:
                p.join()

            end = time.perf_counter()
            epoch_time = end - start
            total_time += epoch_time
            print(f"--- Epoch {epoch+1}/{EPOCHS}: {epoch_time:.3f} secs ---")
            entry = {
                f"total_time": total_time,
                "throughput": dataset_size*(epoch+1) / total_time,
                "processed" : dataset_size*(epoch+1)
            }
            json_record = json.dumps(entry)  # write total time at the end of each epoch
            outfile.write(json_record + '\n')

    print(f"Total wall time: {total_time:.3f} sec")
    print(f"Total samples processed: {len(image_files)*EPOCHS}")
    print(f"Throughput: {len(image_files)*EPOCHS / total_time:.2f} samples/sec")
    # print(f"Avg worker time: {total_worker_time / NUM_WORKERS:.3f} sec")    
    print("="*34)

    


if __name__ == "__main__":
    main()
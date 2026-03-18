import os
import time
import json
import random
import multiprocessing as mp
import pandas as pd

# =====================================
# Config
# =====================================

# Mounted through your filesystem
COCO_IMAGE_DIR = "./mountpoint/dataset/coco/images"
AUDIO_DIR = "./mountpoint/dataset/audiocaps/audio"

# TSV stays outside the mounted FS and is loaded once into memory
AUDIOCAPS_TSV = "./storage/dataset/audiocaps/audiocaps_train.tsv"

RESULT_PATH = "./benchmark_result.jsonl"

NUM_WORKERS = 4
BATCH_SIZE = 32
EPOCHS = 10
SHUFFLE = True
SEED = 237

random.seed(SEED)

# =====================================
# Worker Function
# =====================================

def worker_process(worker_id, sample_list, batch_size, return_dict):
    bytes_read = 0
    samples_processed = 0

    try:
        for i in range(0, len(sample_list), batch_size):
            batch = sample_list[i:i + batch_size]

            # Realistic access pattern:
            # process one sample at a time
            for image_path, audio_path, caption in batch:
                with open(image_path, "rb") as f:
                    img_data = f.read()
                bytes_read += len(img_data)

                with open(audio_path, "rb") as f:
                    audio_data = f.read()
                bytes_read += len(audio_data)

                # caption already in memory
                _ = caption

                samples_processed += 1

        return_dict[worker_id] = {
            "samples_processed": samples_processed,
            "bytes_read": bytes_read,
            "status": "ok",
        }

    except Exception as e:
        return_dict[worker_id] = {
            "samples_processed": samples_processed,
            "bytes_read": bytes_read,
            "status": "error",
            "error": repr(e),
        }


# =====================================
# Dataset Preparation
# =====================================

def load_dataset():
    image_files = sorted(
        os.path.join(COCO_IMAGE_DIR, f)
        for f in os.listdir(COCO_IMAGE_DIR)
        if f.lower().endswith((".jpg", ".jpeg", ".png"))
    )

    audio_files = sorted(
        os.path.join(AUDIO_DIR, f)
        for f in os.listdir(AUDIO_DIR)
        if f.lower().endswith((".flac", ".wav", ".mp3"))
    )

    df = pd.read_csv(AUDIOCAPS_TSV, sep="\t")
    captions = df["text"].tolist()

    dataset_size = min(len(image_files), len(audio_files), len(captions))

    image_files = image_files[:dataset_size]
    audio_files = audio_files[:dataset_size]
    captions = captions[:dataset_size]

    samples = list(zip(image_files, audio_files, captions))
    return samples


# =====================================
# Worker Split
# =====================================

def split_for_workers(samples, num_workers):
    chunks = [[] for _ in range(num_workers)]
    for i, sample in enumerate(samples):
        chunks[i % num_workers].append(sample)
    return chunks


# =====================================
# Main Benchmark
# =====================================

def main():
    dataset = load_dataset()
    dataset_size = len(dataset)

    print("\n============= Config =============")
    print("Workers:", NUM_WORKERS)
    print("Batch size:", BATCH_SIZE)
    print("Epochs:", EPOCHS)
    print("Shuffle:", SHUFFLE)
    print("Dataset size:", dataset_size)

    total_time = 0.0
    total_samples = 0
    total_bytes = 0

    with open(RESULT_PATH, "w") as outfile:
        for epoch in range(EPOCHS):
            epoch_dataset = list(dataset)

            if SHUFFLE:
                rng = random.Random(SEED + epoch)
                rng.shuffle(epoch_dataset)

            chunks = split_for_workers(epoch_dataset, NUM_WORKERS)

            manager = mp.Manager()
            return_dict = manager.dict()
            processes = []

            start = time.perf_counter()

            for wid in range(NUM_WORKERS):
                p = mp.Process(
                    target=worker_process,
                    args=(wid, chunks[wid], BATCH_SIZE, return_dict)
                )
                p.start()
                processes.append(p)

            for p in processes:
                p.join()

            end = time.perf_counter()
            epoch_time = end - start

            # aggregate worker results
            epoch_samples = 0
            epoch_bytes = 0
            worker_errors = []

            for wid in range(NUM_WORKERS):
                result = return_dict.get(wid)
                if result is None:
                    worker_errors.append(f"worker {wid}: no result returned")
                    continue

                epoch_samples += result["samples_processed"]
                epoch_bytes += result["bytes_read"]

                if result["status"] != "ok":
                    worker_errors.append(f"worker {wid}: {result.get('error', 'unknown error')}")

            total_time += epoch_time
            total_samples += epoch_samples
            total_bytes += epoch_bytes

            throughput_samples = epoch_samples / epoch_time if epoch_time > 0 else 0.0
            throughput_mib = (epoch_bytes / (1024 * 1024)) / epoch_time if epoch_time > 0 else 0.0
            avg_sample_size = (epoch_bytes / epoch_samples) if epoch_samples > 0 else 0.0

            print(f"--- Epoch {epoch + 1}/{EPOCHS}: {epoch_time:.3f} sec ---")
            print(f"    Samples: {epoch_samples}")
            print(f"    Bytes read: {epoch_bytes}")
            print(f"    Throughput: {throughput_samples:.2f} samples/sec")
            print(f"    Throughput: {throughput_mib:.2f} MiB/sec")

            if worker_errors:
                print("    Worker errors:")
                for err in worker_errors:
                    print("     ", err)

            entry = {
                "epoch": epoch + 1,
                "epoch_time_sec": epoch_time,
                "epoch_samples": epoch_samples,
                "epoch_bytes_read": epoch_bytes,
                "epoch_throughput_samples_per_sec": throughput_samples,
                "epoch_throughput_mib_per_sec": throughput_mib,
                "avg_sample_size_bytes": avg_sample_size,
                "cumulative_time_sec": total_time,
                "cumulative_samples": total_samples,
                "cumulative_bytes_read": total_bytes,
                "worker_errors": worker_errors,
            }

            outfile.write(json.dumps(entry) + "\n")

    final_samples_per_sec = total_samples / total_time if total_time > 0 else 0.0
    final_mib_per_sec = (total_bytes / (1024 * 1024)) / total_time if total_time > 0 else 0.0

    print("\n============= Result =============")
    print(f"Total wall time: {total_time:.3f} sec")
    print(f"Total samples processed: {total_samples}")
    print(f"Total bytes read: {total_bytes}")
    print(f"Overall throughput: {final_samples_per_sec:.2f} samples/sec")
    print(f"Overall throughput: {final_mib_per_sec:.2f} MiB/sec")
    print("=" * 34)


# =====================================
# Entry
# =====================================

if __name__ == "__main__":
    main()
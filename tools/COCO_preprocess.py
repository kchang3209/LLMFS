import os
import json
import struct
import argparse
import csv
from tqdm import tqdm

MAGIC = b"MMFS"
VERSION = 1
HEADER_SIZE = 4096
ALIGNMENT = 4096


def align_up(x: int, alignment: int) -> int:
    return (x + alignment - 1) // alignment * alignment


def pad_to_alignment(f, current_offset: int, alignment: int) -> int:
    aligned_offset = align_up(current_offset, alignment)
    padding = aligned_offset - current_offset
    if padding > 0:
        f.write(b"\x00" * padding)
    return aligned_offset


def write_header(f, num_samples: int, index_offset: int, index_size: int):
    f.seek(0)
    header = struct.pack(
        "<4sIQQQ",
        MAGIC,         # 4s
        VERSION,       # I
        num_samples,   # Q
        index_offset,  # Q
        index_size     # Q
    )
    f.write(header)
    remaining = HEADER_SIZE - len(header)
    if remaining < 0:
        raise ValueError("Header metadata exceeds HEADER_SIZE")
    f.write(b"\x00" * remaining)


def load_tsv_captions(tsv_path: str):
    captions = []
    with open(tsv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            captions.append(row["text"])
    return captions


def list_files_with_ext(directory: str, exts):
    exts = tuple(ext.lower() for ext in exts)
    files = [
        f for f in os.listdir(directory)
        if os.path.isfile(os.path.join(directory, f)) and f.lower().endswith(exts)
    ]
    return sorted(files)


def pack_dataset(image_dir, audio_dir, caption_tsv, pack_path, index_path):
    captions = load_tsv_captions(caption_tsv)

    images = list_files_with_ext(image_dir, [".jpg", ".jpeg", ".png"])
    audios = list_files_with_ext(audio_dir, [".flac", ".wav", ".mp3"])

    num_samples = min(len(images), len(audios), len(captions))

    print("Images:", len(images))
    print("Audio:", len(audios))
    print("Captions:", len(captions))
    print("Samples to pack:", num_samples)
    print("Sample alignment:", ALIGNMENT, "bytes")

    index = {}
    offset = HEADER_SIZE

    with open(pack_path, "wb") as pack:
        # Reserve fixed-size header region
        pack.write(b"\x00" * HEADER_SIZE)

        for i in tqdm(range(num_samples), desc="Packing samples"):
            img_file = images[i]
            audio_file = audios[i]
            caption_text = captions[i]

            img_path = os.path.join(image_dir, img_file)
            audio_path = os.path.join(audio_dir, audio_file)

            with open(img_path, "rb") as f:
                img_bytes = f.read()

            with open(audio_path, "rb") as f:
                audio_bytes = f.read()

            text_bytes = caption_text.encode("utf-8")

            # Align the START of each sample, not each object
            aligned_sample_offset = align_up(offset, ALIGNMENT)
            if aligned_sample_offset > offset:
                pack.write(b"\x00" * (aligned_sample_offset - offset))
                offset = aligned_sample_offset

            sample_offset = offset
            entry = {
                "sample_offset": sample_offset,
                "image_file": img_file,
                "audio_file": audio_file,
            }

            # Pack one sample contiguously: image -> audio -> text
            entry["image_offset"] = offset
            entry["image_size"] = len(img_bytes)
            pack.write(img_bytes)
            offset += len(img_bytes)

            entry["audio_offset"] = offset
            entry["audio_size"] = len(audio_bytes)
            pack.write(audio_bytes)
            offset += len(audio_bytes)

            entry["text_offset"] = offset
            entry["text_size"] = len(text_bytes)
            pack.write(text_bytes)
            offset += len(text_bytes)

            entry["sample_size"] = offset - sample_offset
            index[str(i)] = entry

        index_offset = offset
        index_bytes = json.dumps(index, ensure_ascii=False).encode("utf-8")
        index_size = len(index_bytes)
        pack.write(index_bytes)

        write_header(pack, num_samples, index_offset, index_size)

    with open(index_path, "w", encoding="utf-8") as f:
        json.dump(index, f, indent=2, ensure_ascii=False)

    print("\nPacking complete")
    print("Samples packed:", num_samples)
    print("Pack file:", pack_path)
    print("Index file:", index_path)
    print("Index offset:", index_offset)
    print("Index size:", index_size)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--image_dir", default="../storage/dataset/coco/images/")
    parser.add_argument("--audio_dir", default="../storage/dataset/audiocaps/audio/")
    parser.add_argument("--caption_tsv", default="../storage/dataset/audiocaps/audiocaps_train.tsv")

    parser.add_argument("--pack", default="multimodal.pack")
    parser.add_argument("--index", default="multimodal_index.json")

    args = parser.parse_args()

    pack_dataset(
        args.image_dir,
        args.audio_dir,
        args.caption_tsv,
        args.pack,
        args.index
    )
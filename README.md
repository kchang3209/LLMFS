# Filesystem for Multimodal LLM

### Requirement: 
1. FUSE package installation
2. Udacity Dataset [[image download]](https://s3.amazonaws.com/udacity-sdc/annotations/object-detection-crowdai.tar.gz) [[annotation download]](https://raw.githubusercontent.com/udacity/self-driving-car/master/annotations/labels_crowdai.csv)
3. MSCOCO Dataset [[download]](https://www.kaggle.com/datasets/awsaf49/coco-2017-dataset/data)
4. Audiocaps Dataset [[download]](https://www.kaggle.com/datasets/nickkar30/audiocaps/data)

### Usage
1. Download datasets and preprocess them by using the packing scripts in `./tools`.
2. Store the pack files in a `./packed_file` folder.
3. In `filesystem.py`, select a backend under the Hyperparams section and set the cache size (if applicable). Also, adjust the `pack_path`, `index_path`, and `storage_root` as needed. The cache latency result can be printed by setting the `print_lat` to `True`.
4. Running `python filesystem.py` will activate the backend and mount the backend data in the `./storage` folder to `./mountpoint`.
5. Open another terminal and run either `benchmark.py` or `benchmark_mm.py`, depending on which file packing script was used to generate the pack file. In the course project, `./tools/COCO_preprocess.py` corresponds to the `benchmark_mm.py`; `./tools/udacity_preprocess.py` to the `benchmark.py`.
 

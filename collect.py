import kagglehub
from kagglehub import KaggleDatasetAdapter
from tqdm import tqdm
from pathlib import Path

files_path = [
    "yellow_tripdata_2015-01.csv",
    "yellow_tripdata_2016-01.csv",
    "yellow_tripdata_2016-02.csv",
    "yellow_tripdata_2016-03.csv"
]

raw_folder = Path("data/raw/")
raw_folder.mkdir(parents=True, exist_ok=True)

for file in tqdm(files_path, desc="Baixando arquivos"):
    
    df = kagglehub.dataset_load(
        KaggleDatasetAdapter.PANDAS,
        "elemento/nyc-yellow-taxi-trip-data",
        file,
    )

    output_file = raw_folder / file.replace('.csv', '.parquet')
    df.to_parquet(output_file)

    del df
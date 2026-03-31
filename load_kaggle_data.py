import os
import argparse
import pandas as pd
import kaggle
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from pathlib import Path

load_dotenv()

def get_engine():
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    db = os.getenv("POSTGRES_DB")
    return create_engine(f"postgresql://{user}:{password}@localhost:5432/{db}")

def download_dataset():
    download_path = Path.home() / "Desktop" / "taxi_data"
    download_path.mkdir(exist_ok=True)

    if list(download_path.glob("*.csv")):
        print('Файлы уже скачаны, пропускаем загрузку')
        return download_path

    print('Скачиваем датасет с Kaggle')
    kaggle.api.authenticate()
    kaggle.api.dataset_download_files(
        "elemento/nyc-yellow-taxi-trip-data",
        path=str(download_path),
        unzip=True)
    print('Датасет скачан')
    return download_path

def load_file(filepath, engine, append=False, limit=None, skiprows=None):
    print(f"\n📂 Читаем: {filepath.name}" + (f" (лимит: {limit:,})" if limit else ""))
    df = pd.read_csv(filepath, nrows=limit, low_memory=False)
    df.columns = [col if col != "RateCodeID" else "RatecodeID" for col in df.columns]
    df["source_file"] = filepath.name

    df["tpep_pickup_datetime"] = pd.to_datetime(
        df["tpep_pickup_datetime"], format="%Y-%m-%d %H:%M:%S", errors="coerce")
    df["tpep_dropoff_datetime"] = pd.to_datetime(
        df["tpep_dropoff_datetime"], format="%Y-%m-%d %H:%M:%S", errors="coerce")
    
    mode = "append" if append else "replace"
    df.to_sql("nyc_taxi_raw", engine, if_exists=mode, index=False, chunksize=10000)

    print(f"Загружено {len(df):,} строк  (режим: {mode})")
    return len(df)


def main(append=False, limit=None, only2015=False, skip2015=False):
    engine = get_engine()
    folder = download_dataset()

    csv_files = list(folder.glob("*.csv"))
    print(f"\nНайдено файлов: {len(csv_files)}")
    total = 0
    first_file = True
    
    if only2015:
        csv_files = [f for f in csv_files if "2015" in f.name]
        print("📅 Режим: только файлы 2015 года")
    elif skip2015:
        csv_files = [f for f in csv_files if "2015" not in f.name]
        print("📅 Режим: только файлы 2016 года (пропускаем 2015)")
 
    print(f"\nНайдено файлов: {len(csv_files)}")
    total = 0
    first_file = True
    for filepath in sorted(csv_files):
        file_mode = append or not first_file
        total += load_file(filepath, engine, append=file_mode, limit=limit)
        first_file = False

    print(f"\n Загружено: {total:,} строк в таблицу nyc_taxi_raw")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--append",
        action="store_true",
        help="добавить данные к существующей таблице")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="ограничить количество строк на файл")
    parser.add_argument(
        "--only2015",
        action="store_true",
        help="загрузить только файл 2015 года")
    parser.add_argument(
        "--skip2015",
        action="store_true",
        help="загрузить только файлы 2016 года (пропустить 2015)")
    args = parser.parse_args()
    main(append=args.append, limit=args.limit, only2015=args.only2015, skip2015=args.skip2015)
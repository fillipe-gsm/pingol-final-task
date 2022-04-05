"""Entrypoint to upload all desired data files"""
from pathlib import Path

from data_ingestion.upload_data import upload_blob


for file_name in Path("./data/").glob("*.csv"):
    print(f"Uploading file {file_name}")
    upload_blob(file_name, f"assinaturas/{file_name}")

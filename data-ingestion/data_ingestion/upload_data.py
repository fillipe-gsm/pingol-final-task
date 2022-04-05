"""Main module to upload the data here to a datalake"""
from azure.storage.blob import BlobClient

from config import settings


def upload_blob(file_name: str, blob_name: str) -> None:
    """Upload a blob to the container specified in settings"""
    blob = BlobClient.from_connection_string(
        conn_str=settings.connection_string,
        container_name=settings.container_name,
        blob_name=blob_name,
    )

    with open(file_name, "rb") as data:
        blob.upload_blob(data)

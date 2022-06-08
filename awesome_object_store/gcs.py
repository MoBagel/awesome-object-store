import glob
import json
from io import BytesIO
from logging import Logger
from os import path
from pathlib import Path
from typing import IO, List, Optional

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud.storage import Blob, Bucket, Client

from awesome_object_store.base import BaseObjectStore


class GoogleCloudStore(BaseObjectStore[Bucket, Blob]):
    client: Client

    def __init__(
        self,
        bucket: str,
        logger: Optional[Logger] = None,
    ):
        self.bucket = bucket
        self.client = Client()
        self.logger = logger if logger is not None else Logger("minio")
        found = self.client.bucket(self.bucket).exists()
        if not found:
            self.logger.warning("bucket not exist, creating it")
            self.create_bucket(self.bucket)
        else:
            self.logger.info("bucket '%s' exists", self.bucket)

    def create_bucket(self, bucket_name: str):
        self.client.create_bucket(bucket_name)

    def bucket_exists(self, bucket_name: str) -> bool:
        return self.client.bucket(bucket_name).exists()

    def list_buckets(self):
        """List information of all accessible buckets with text."""
        return [x.name for x in self.client.list_buckets()]

    def list_objects(self, prefix: str = None, recursive: bool = False):
        """Lists object information of a bucket with text."""
        delimiter = None if recursive else "/"
        include_trailing_delimiter = True if delimiter else False
        blobs = self.client.list_blobs(
            self.bucket,
            prefix=prefix,
            delimiter=delimiter,
            include_trailing_delimiter=include_trailing_delimiter,
        )
        return [b.name for b in blobs if b.name != prefix]

    def fput(self, name: str, file_path: str, exclude_files: List[str] = []):
        """Uploads data from a file/folder to an object in a bucket."""
        if path.isdir(file_path):
            for local_file in glob.glob(file_path + "/**"):
                file_name = Path(local_file).name
                remote_path = path.join(name, file_name)

                if file_name in exclude_files:
                    self.logger.info(f"exclude: {local_file}")
                    continue

                if not path.isfile(local_file):
                    self.fput(remote_path, local_file, exclude_files)
                else:
                    remote_path = path.join(name, local_file[1 + len(file_path) :])
                    blob: Blob = self.client.bucket(self.bucket).blob(remote_path)
                    blob.upload_from_filename(local_file)
        else:
            blob = self.client.bucket(self.bucket).blob(name)
            blob.upload_from_filename(file_path)

    def put(
        self,
        name: str,
        data: IO,
        length: Optional[int] = None,
        content_type: str = "application/octet-stream",
    ):
        """Uploads data from a stream to an object in a bucket."""
        if not length:
            data.seek(0)

        blob: Blob = self.client.bucket(self.bucket).blob(name)
        blob.upload_from_file(data)

    def get(self, name: str):
        """Gets data of an object."""
        file_obj = BytesIO()
        blob = self.client.bucket(self.bucket).blob(name)
        blob.download_to_file(file_obj)
        file_obj.seek(0)
        return file_obj

    def get_json(self, name: str) -> dict:
        """Gets data of an object and return a json."""
        try:
            file_obj = self.get(name)
        except NotFound as e:
            self.logger.warning(e)
            return {}
        result = json.load(file_obj)
        return result

    def get_df(
        self,
        name: str,
        column_types: dict = {},
        date_columns: List[str] = [],
    ) -> Optional[pd.DataFrame]:
        """Gets data of an object and return a dataframe."""
        try:
            file_obj = self.get(name)
        except NotFound as e:
            self.logger.warning(e)
            return None
        if not date_columns:
            df = pd.read_csv(file_obj, dtype=column_types)
        else:
            df = pd.read_csv(file_obj, parse_dates=date_columns, dtype=column_types)
        return df

    def exists(self, name: str) -> bool:
        """Check if object or bucket exist."""
        blob: Blob = self.client.bucket(self.bucket).get_blob(name)
        return False if blob is None else True

    def remove_object(self, name: str):
        """Remove an object."""
        blob: Blob = self.client.bucket(self.bucket).blob(name)
        blob.delete()

    def download(self, name: str, file_path: str):
        """Downloads data of an object to file."""
        blob: Blob = self.client.bucket(self.bucket).blob(name)
        blob.download_to_filename(file_path)

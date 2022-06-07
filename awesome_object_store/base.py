import csv
import json
from abc import ABC, abstractmethod
from io import BytesIO, StringIO
from logging import Logger
from typing import IO, List, Optional

import pandas as pd
from minio import S3Error
from starlette.datastructures import UploadFile


class BaseObjectStorage(ABC):
    bucket: str
    logger: Logger

    @abstractmethod
    def create_bucket(self, bucket_name: str):
        pass

    @abstractmethod
    def bucket_exists(self, bucket_name: str) -> bool:
        pass

    @abstractmethod
    def list_buckets(self):
        pass

    @abstractmethod
    def list_objects(self, prefix: str = None, recursive: bool = False):
        pass

    @abstractmethod
    def fput(self, name: str, file_path: str, exclude_files: List[str] = []):
        pass

    @abstractmethod
    def put(
        self,
        name: str,
        data: IO,
        length: Optional[int] = None,
        content_type: str = "application/octet-stream",
    ):
        pass

    @abstractmethod
    def get(self, name: str):
        pass

    @abstractmethod
    def exists(self, name: str) -> bool:
        pass

    @abstractmethod
    def remove_object(self, name: str):
        pass

    @abstractmethod
    def download(self, name: str, file_path: str):
        pass

    def remove_dir(self, folder: str):
        """Remove folder."""
        self.logger.warning("removing %s", folder)
        objects_to_delete = self.list_objects(prefix=folder, recursive=True)
        self.logger.warning("Removing: %s", objects_to_delete)
        self.remove_objects(objects_to_delete)

    def upload_df(
        self, name: str, data: pd.DataFrame, index=False, quoting=csv.QUOTE_MINIMAL
    ):
        """Uploads data from a pandas dataframe to an object in a bucket."""
        data_bytes = data.to_csv(index=index, quoting=quoting).encode("utf-8")
        data_byte_stream = BytesIO(data_bytes)

        self.put(name, data_byte_stream, content_type="application/csv")

    def put_as_json(self, name: str, data: dict):
        """Uploads data from a json to an object in a bucket."""
        data_bytes = json.dumps(data).encode("utf-8")
        data_byte_stream = BytesIO(data_bytes)

        self.put(name, data_byte_stream, content_type="application/json")

    def fget_df(
        self,
        file: UploadFile,
        column_types: dict = {},
        date_columns: List[str] = [],
    ) -> Optional[pd.DataFrame]:
        try:
            file_io = StringIO(str(file.file.read(), "utf-8"))
            df = pd.read_csv(file_io, dtype=column_types, parse_dates=date_columns)
            file_io.close()
        except Exception as e:
            self.logger.warning("unable to read csv %s" % str(e))
            return None
        return df

    def get_json(self, name: str) -> dict:
        """Gets data of an object and return a json."""
        try:
            file_obj = self.get(name)
        except S3Error as e:
            self.logger.warning(e)
            return {}
        result = json.load(file_obj)
        file_obj.close()
        file_obj.release_conn()
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
        except S3Error as e:
            self.logger.warning(e)
            return None

        if not date_columns:
            df = pd.read_csv(file_obj, dtype=column_types)
        else:
            df = pd.read_csv(file_obj, parse_dates=date_columns, dtype=column_types)
        file_obj.close()
        file_obj.release_conn()
        return df

    def remove_objects(self, names: list):
        """Remove objects."""
        for name in names:
            try:
                self.remove_object(name)
            except Exception as e:
                self.logger.warning("%s Deletion Error: %s", name, e)

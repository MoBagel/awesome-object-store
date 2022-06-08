import csv
import json
from abc import ABC, abstractmethod
from io import BytesIO, StringIO
from logging import Logger
from typing import IO, Generic, List, Optional, TypeVar

import pandas as pd
from starlette.datastructures import UploadFile

BlobType = TypeVar("BlobType")
BucketType = TypeVar("BucketType")


class BaseObjectStore(Generic[BucketType, BlobType], ABC):
    bucket: str
    logger: Logger

    @abstractmethod
    def create_bucket(self, bucket_name: str) -> None:
        pass

    @abstractmethod
    def bucket_exists(self, bucket_name: str) -> bool:
        pass

    @abstractmethod
    def list_buckets(self) -> List[BucketType]:
        pass

    @abstractmethod
    def list_objects(
        self, prefix: str = None, recursive: bool = False
    ) -> List[BlobType]:
        pass

    @abstractmethod
    def fput(self, name: str, file_path: str, exclude_files: List[str] = []) -> None:
        pass

    @abstractmethod
    def put(
        self,
        name: str,
        data: IO,
        length: Optional[int] = None,
        content_type: str = "application/octet-stream",
    ) -> None:
        pass

    @abstractmethod
    def get(self, name: str) -> BlobType:
        pass

    @abstractmethod
    def exists(self, name: str) -> bool:
        pass

    @abstractmethod
    def remove_object(self, name: str) -> None:
        pass

    @abstractmethod
    def download(self, name: str, file_path: str) -> None:
        pass

    @abstractmethod
    def get_df(
        self,
        name: str,
        column_types: dict = {},
        date_columns: List[str] = [],
    ) -> Optional[pd.DataFrame]:
        pass

    @abstractmethod
    def get_json(self, name: str) -> dict:
        pass

    def remove_dir(self, folder: str) -> None:
        """Remove folder."""
        self.logger.warning("removing %s", folder)
        objects_to_delete = self.list_objects(prefix=folder, recursive=True)
        self.logger.warning("Removing: %s", objects_to_delete)
        self.remove_objects(objects_to_delete)

    def upload_df(
        self, name: str, data: pd.DataFrame, index=False, quoting=csv.QUOTE_MINIMAL
    ) -> None:
        """Uploads data from a pandas dataframe to an object in a bucket."""
        data_bytes = data.to_csv(index=index, quoting=quoting).encode("utf-8")
        data_byte_stream = BytesIO(data_bytes)

        self.put(name, data_byte_stream, content_type="application/csv")

    def put_as_json(self, name: str, data: dict) -> None:
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

    def remove_objects(self, names: list) -> None:
        """Remove objects."""
        for name in names:
            try:
                self.remove_object(name)
            except Exception as e:
                self.logger.warning("%s Deletion Error: %s", name, e)

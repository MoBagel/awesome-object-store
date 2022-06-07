import csv
import json
from abc import ABC, abstractmethod
from io import StringIO
from logging import Logger
from typing import IO, List, Optional

import pandas as pd
from minio import S3Error
from starlette.datastructures import UploadFile


class BaseObjectStorage(ABC):
    bucket: str
    logger: Logger

    @abstractmethod
    def list_buckets(self):
        pass

    @abstractmethod
    def list_objects(self, prefix: str = None, recursive: bool = False):
        """Lists object information of a bucket with text."""

    @abstractmethod
    def fput(self, name: str, file_path: str, exclude_files: List[str] = []):
        """Uploads data from a file/folder to an object in a bucket."""

    @abstractmethod
    def put(
        self,
        name: str,
        data: IO,
        length: Optional[int] = None,
        content_type: str = "application/octet-stream",
    ):
        """Uploads data from a stream to an object in a bucket."""

    @abstractmethod
    def put_as_json(self, name: str, data: dict):
        """Uploads data from a json to an object in a bucket."""

    @abstractmethod
    def get(self, name: str):
        """Gets data of an object."""

    @abstractmethod
    def exists(self, name: str) -> bool:
        """Check if object or bucket exist."""

    @abstractmethod
    def remove_dir(self, folder: str):
        """Remove folder."""

    @abstractmethod
    def remove_object(self, name: str):
        """Remove an object."""

    @abstractmethod
    def upload_df(
        self, name: str, data: pd.DataFrame, index=False, quoting=csv.QUOTE_MINIMAL
    ):
        """Uploads data from a pandas dataframe to an object in a bucket."""

    @abstractmethod
    def download(self, name: str, file_path: str):
        """Downloads data of an object to file."""

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

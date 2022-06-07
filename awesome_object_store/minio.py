import glob
from logging import Logger
from os import path
from pathlib import Path
from typing import IO, List, Optional

from minio import Minio

from awesome_object_store.base import BaseObjectStorage


class MinioStore(BaseObjectStorage):
    client: Minio

    def __init__(
        self,
        host: str,
        bucket: str,
        access_key: str,
        secret_key: str,
        secure: bool = False,
        region: Optional[str] = None,
        logger: Optional[Logger] = None,
    ):
        self.bucket = bucket
        self.client = Minio(
            host,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
            region=region,
        )
        self.logger = logger if logger is not None else Logger("minio")
        found = self.bucket_exists(self.bucket)
        if not found:
            self.logger.warning("bucket not exist, creating it")
            self.create_bucket(self.bucket)
        else:
            self.logger.info("bucket '%s' exists", self.bucket)

    def create_bucket(self, bucket_name: str):
        self.client.make_bucket(bucket_name)

    def bucket_exists(self, bucket_name: str) -> bool:
        return self.client.bucket_exists(bucket_name)

    def list_buckets(self):
        """List information of all accessible buckets with text."""
        return [x.name for x in self.client.list_buckets()]

    def list_objects(self, prefix: str = None, recursive: bool = False):
        """Lists object information of a bucket with text."""
        return [
            x.object_name
            for x in self.client.list_objects(
                self.bucket, prefix=prefix, recursive=recursive
            )
        ]

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
                    self.client.fput_object(self.bucket, remote_path, local_file)
        else:
            self.client.fput_object(self.bucket, name, file_path)

    def put(
        self,
        name: str,
        data: IO,
        length: Optional[int] = None,
        content_type: str = "application/octet-stream",
    ):
        """Uploads data from a stream to an object in a bucket."""
        if not length:
            length = len(data.read())
            data.seek(0)

        self.client.put_object(
            self.bucket, name, data, length, content_type=content_type
        )

    def get(self, name: str):
        """Gets data of an object."""
        return self.client.get_object(self.bucket, name)

    def exists(self, name: str) -> bool:
        """Check if object or bucket exist."""
        try:
            self.client.stat_object(self.bucket, name)
            return True
        except Exception:
            return False

    def remove_object(self, name: str):
        """Remove an object."""
        self.client.remove_object(self.bucket, name)

    def download(self, name: str, file_path: str):
        """Downloads data of an object to file."""
        self.client.fget_object(self.bucket, name, file_path)

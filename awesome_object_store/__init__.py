from logging import Logger
from typing import Optional

from awesome_object_store.base import BaseObjectStore
from awesome_object_store.gcs import GoogleCloudStore
from awesome_object_store.minio import MinioStore


def init_object_store(
    bucket: str,
    host: Optional[str] = None,
    access_key: str = None,
    secret_key: str = None,
    secure: bool = False,
    region: str = None,
    logger: Optional[Logger] = None,
    protocol: Optional[str] = "gcs",
) -> BaseObjectStore:
    if protocol == "gcs":
        return GoogleCloudStore(bucket, logger)
    else:
        return MinioStore(
            bucket,
            host,
            access_key,
            secret_key,
            secure,
            region,
            logger,
        )

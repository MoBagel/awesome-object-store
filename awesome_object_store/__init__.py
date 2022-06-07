import os
from logging import Logger
from typing import Optional

from awesome_object_store.base import BaseObjectStorage
from awesome_object_store.gcs import GoogleCloudStore
from awesome_object_store.minio import MinioStore


def init_object_store(
    bucket: str,
    host: Optional[str] = None,
    access_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    secure: Optional[bool] = None,
    region: Optional[str] = None,
    logger: Optional[Logger] = None,
) -> BaseObjectStorage:
    if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", None) is not None:
        return GoogleCloudStore(bucket, logger)
    else:
        return MinioStore(
            host,
            bucket,
            access_key,
            secret_key,
            secure,
            region,
            logger,
        )

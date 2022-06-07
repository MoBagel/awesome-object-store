from awesome_object_store.base import BaseObjectStorage
from awesome_object_store.minio import MinioStore


def init_object_store() -> BaseObjectStorage:
    if "some_condition":
        return MinioStore()
    else:
        pass

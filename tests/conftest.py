import pytest

from awesome_minio.minio import MinioStore
from tests import generate_fake_dataframe
from pydantic import BaseSettings, Field


class TestSettings(BaseSettings):
    mongodb_dsn: str = Field(
        default="mongodb://localhost:27017/beanie_db", env="MONGODB_DNS"
    )
    mongodb_db_name: str = Field(default="beanie_db", env="MONGODB_DB_NAME")
    minio_access_key: str = Field(default="minioadmin", env="MINIO_ACCESS_KEY")
    minio_secret_key: str = Field(default="minioadmin", env="MINIO_SECRET_KEY")
    minio_bucket: str = Field(default="test", env="MINIO_BUCKET")
    minio_host: str = Field(default="0.0.0.0:9000", env="MINIO_ADDRESS")


@pytest.fixture
def settings():
    return TestSettings()


@pytest.fixture()
def test_string():
    return b"to grasp how wide and long and high and deep is the love of Christ"


@pytest.fixture()
def test_dataframe():
    return generate_fake_dataframe(size=100, cols="cicid")


@pytest.fixture()
def test_dict(test_string):
    return {"test_string": test_string.decode("utf-8")}


@pytest.fixture
def minio_store(settings):
    return MinioStore(
        host=settings.minio_host,
        bucket=settings.minio_bucket,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
    )

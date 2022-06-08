from typing import Optional

import pytest

from awesome_object_store import GoogleCloudStore
from awesome_object_store.minio import MinioStore
from tests import generate_fake_dataframe
from pydantic import BaseSettings, Field


class TestSettings(BaseSettings):
    mongodb_dsn: str = Field(
        default="mongodb://localhost:27017/beanie_db", env="MONGODB_DNS"
    )
    mongodb_db_name: str = Field(default="beanie_db", env="MONGODB_DB_NAME")
    minio_access_key: str = Field(default="minioadmin", env="MINIO_ACCESS_KEY")
    minio_secret_key: str = Field(default="minioadmin", env="MINIO_SECRET_KEY")
    minio_bucket: str = Field(default="8ndpoint-test-dev", env="MINIO_BUCKET")
    minio_host: str = Field(default="0.0.0.0:9000", env="MINIO_ADDRESS")
    minio_secure: bool = Field(default=False, env="MINIO_SECURE")
    minio_region: Optional[str] = Field(default=None, env="MINIO_REGION")


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
        secure=settings.minio_secure,
        region=settings.minio_region,
    )


@pytest.fixture
def google_application_credentials():
    return "./tests/service-account.json"

@pytest.fixture
def google_cloud_store(monkeypatch, google_application_credentials, settings):
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", google_application_credentials)
    return GoogleCloudStore(bucket=settings.minio_bucket)

@pytest.fixture
def test_file_name():
    return "test.txt"
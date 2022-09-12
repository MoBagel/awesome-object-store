import pytest

from awesome_object_store import GoogleCloudStore, MinioStore, init_object_store


def test_init_object_store(monkeypatch, settings, google_application_credentials):
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", google_application_credentials)
    google_cloud_store = init_object_store(
        bucket=settings.minio_bucket,
    )
    assert isinstance(google_cloud_store, GoogleCloudStore)

    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS")
    minio_store = init_object_store(
        bucket=settings.minio_bucket,
        host=settings.minio_host,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=settings.minio_secure,
        region=settings.minio_region,
        protocol="minio",
    )
    assert isinstance(minio_store, MinioStore)

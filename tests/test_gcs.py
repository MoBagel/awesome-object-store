import os
import tempfile

from starlette.datastructures import UploadFile


def test_bucket_exists(google_cloud_store, settings):
    existed = google_cloud_store.bucket_exists(settings.minio_bucket)
    assert existed is True


def test_fput(google_cloud_store, test_string, test_file_name):
    with tempfile.NamedTemporaryFile(suffix=".txt") as file:
        file.write(test_string)
        file.flush()
        google_cloud_store.fput(test_file_name, file.name)
    begotten = google_cloud_store.get(test_file_name)
    assert google_cloud_store.exists(test_file_name)
    assert begotten.read(len(test_string)) == test_string
    # clean up test_file_name 
    google_cloud_store.remove_object(test_file_name)

    with tempfile.TemporaryDirectory() as dir:
        with tempfile.NamedTemporaryFile(dir=dir) as file1:
            file1.write(test_string)
            file1.flush()
            with tempfile.NamedTemporaryFile(dir=dir) as file2:
                file_name2 = file2.name[1 + len(dir) :]
                file2.write(test_string)
                file2.flush()
                with tempfile.TemporaryDirectory(dir=dir) as tmp_dir:
                    google_cloud_store.fput(f"test_dir{dir}",dir, exclude_files=[file_name2])
    assert google_cloud_store.exists(f"test_dir{file1.name}")
    assert google_cloud_store.exists(f"test_dir{file2.name}") is False

    # clean up file1
    google_cloud_store.remove_object(f"test_dir{file1.name}")

def test_put(google_cloud_store, test_string):
    with tempfile.NamedTemporaryFile() as file:
        file.write(test_string)
        file.flush()
        file.seek(0)
        google_cloud_store.put(file.name, file)
    begotten = google_cloud_store.get(file.name)
    assert google_cloud_store.exists(file.name)
    assert begotten.read(len(test_string)) == test_string
    # clean up file
    google_cloud_store.remove_object(file.name)


def test_put_and_get_json(google_cloud_store, test_dict):
    google_cloud_store.put_as_json("dict.json", test_dict)
    assert google_cloud_store.exists("dict.json")
    begotten = google_cloud_store.get_json("dict.json")

    # clean up dict.json
    google_cloud_store.remove_object("dict.json")

    assert begotten["test_string"] == test_dict["test_string"]
    begotten = google_cloud_store.get_json("non_exist.json")
    assert len(begotten) == 0


def test_put_get_and_download_df(google_cloud_store, test_dataframe):
    google_cloud_store.upload_df("test.csv", test_dataframe)
    df = google_cloud_store.get_df("test.csv")
    assert df.shape[0] == 100
    df = google_cloud_store.get_df("test.csv", date_columns=["column_4_date"])
    assert df.shape[0] == 100
    df = google_cloud_store.get_df("not_exist.csv", date_columns=["column_4_date"])
    assert df is None

    google_cloud_store.download("test.csv", "test.csv")
    assert os.path.exists("test.csv")
    os.remove("test.csv")

    # clean up test.csv
    google_cloud_store.remove_object("test.csv")
    assert google_cloud_store.exists("test.csv") is False

def test_remove_object_objects_and_dir(google_cloud_store, test_dict):
    for i in range(3):
        google_cloud_store.put_as_json(f"dict{i}.json", test_dict)
    google_cloud_store.put_as_json("tmp/dict.json", test_dict)

    for i in range(3):
        assert google_cloud_store.exists(f"dict{i}.json")
    assert google_cloud_store.exists("tmp/dict.json")

    # test remove object
    google_cloud_store.remove_object("dict0.json")
    assert google_cloud_store.exists("dict0.json") is False

    # test remove objects
    google_cloud_store.remove_objects(["dict1.json", "dict2.json"])

    assert google_cloud_store.exists("dict1.json") is False
    assert google_cloud_store.exists("dict2.json") is False

    # # test remove directory
    google_cloud_store.remove_dir("tmp/")
    assert google_cloud_store.exists("tmp/dict.json") is False


async def test_fget_df(google_cloud_store, test_dataframe):
    with tempfile.NamedTemporaryFile() as temp:
        test_dataframe.to_csv(temp.name, index=False)
        upload_file = UploadFile(temp.name, temp)
        df = google_cloud_store.fget_df(upload_file)
        assert df.shape[0] == 100
        await upload_file.seek(0)
        df = google_cloud_store.fget_df(upload_file, date_columns=["column_4_date"])
        assert df.shape[0] == 100
        upload_file = UploadFile("non_exist")
        df = google_cloud_store.fget_df(upload_file, date_columns=["column_4_date"])
        assert df is None


async def test_list_objects_with_invalid_args(google_cloud_store,test_dict):
    for i in range(6):
        google_cloud_store.put_as_json(f"XD/{i}.json", test_dict)
    blobs = google_cloud_store.list_objects("XD/", start_offset="XD/1", end_offset="XD/4")
    for blob in blobs:
        assert blob in ['XD/1.json', 'XD/2.json', 'XD/3.json']
    google_cloud_store.remove_dir("XD/")

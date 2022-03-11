[![Stable Version](https://img.shields.io/pypi/v/awesome-minio?label=stable)](https://pypi.org/project/awesome-minio/)
[![tests](https://github.com/MoBagel/awesome-minio/workflows/develop/badge.svg)](https://github.com/MoBagel/awesome-minio)
[![Coverage Status](https://coveralls.io/repos/github/MoBagel/awesome-minio/badge.svg?branch=develop)](https://coveralls.io/github/MoBagel/awesome-minio)

# Awesome Minio

A library that extends minio python client to perform more complex task like read/write pandas DataFrame, json file, ...etc

# Feature
* list_buckets: list all buckets.
* list_objects: list object under a prefix.
* put_as_json: put a dict as json file on s3.
* exists: check if an object exist on s3.
* remove_dir: remove a directory on s3.
* upload_df: Upload df as csv to s3.
* get_json: Get as dict from a json file on s3.
* get_df: Get a dataframe from a csv object on s3.
* remove_objects: Remove objects.
* download: Downloads data of an object to file.

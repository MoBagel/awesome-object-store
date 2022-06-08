[![Stable Version](https://badge.fury.io/py/awesome_object_store.svg)](https://pypi.org/project/awesome_object_store/)
[![tests](https://github.com/MoBagel/awesome_object_store/workflows/develop/badge.svg)](https://github.com/MoBagel/awesome_object_store)
[![Coverage Status](https://coveralls.io/repos/github/MoBagel/awesome_object_store/badge.svg?branch=develop)](https://coveralls.io/github/MoBagel/awesome_object_store)

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

# Development
## run unit test
1. getting service account credential:
   1.visit google cloud console
   2.go to project 8ndpoint-datalake-dev -> Security -> Secret Manager -> awesome-object-store-unit-test
   3.action -> view secret value
   4.store the value in tests/service-account.json
2. run ./run_test.sh
# -*- coding: utf-8 -*-

import boto3
from moto import mock_s3

import json

import zarr
from zarr.storage import FSStore

class TestS3(object):

    @mock_s3
    def test_fsstore(self):
        bucket = "test_fsstore"
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket=bucket)

        store = FSStore(f"s3://{bucket}/test.zarr")
        group = zarr.group(store=store)
        array = group.array("array", data=[[1,2,3], [4,5,6]])

        array_json = conn.Object(bucket, 'test.zarr/array/.zarray').get()
        assert [2,3] == json.loads(array_json["Body"].read())["chunks"]
        chunk_bytes = conn.Object(bucket, 'test.zarr/array/0.0').get()

    @mock_s3
    def test_fsstore_nested(self):
        bucket = "test_fsstore"
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket=bucket)

        store = FSStore(f"s3://{bucket}/test.zarr", key_separator="/")
        group = zarr.group(store=store)
        array = group.array("array", data=[[1,2,3], [4,5,6]])

        array_json = conn.Object(bucket, 'test.zarr/array/.zarray').get()
        assert [2,3] == json.loads(array_json["Body"].read())["chunks"]
        chunk_bytes = conn.Object(bucket, 'test.zarr/array/0/0').get()

#!/usr/bin/env python3

import os
import boto3
import json
import io
import re

from boto3.s3.transfer import TransferConfig
from botocore.config import Config
import indexed_gzip # pip3 install indexed_gzip

from seekable_s3_stream import SeekableS3Stream

# Load S3 Config
with open("polygon.json", "r") as f:
    config = json.load(f)

session = boto3.Session(
   aws_access_key_id=config["s3_access_key_id"],
   aws_secret_access_key=config["s3_secret_access_key"],
)

def build_index_for_file(in_s3_client, in_bucket_name, in_object_key):
    assert(in_object_key.endswith(".gz"))

    host = "__unknown_host__"
    try:
        host = s3._endpoint.host
        match = re.match(r"^(?:https?:\/\/)?([^\/]+)", host)
        host = match.group(1)
    except:
        print("Warning: failed to extract hostname from s3 client, maybe they changed the internal class structure")

    # Create seekable stream
    seekable_stream = SeekableS3Stream(in_s3_client, in_bucket_name, in_object_key)

    # Use with indexed_gzip
    with indexed_gzip.IndexedGzipFile(fileobj=seekable_stream) as fobj:
        fobj.build_full_index()
        destination_path = os.path.join(host, bucket_name, object_key.replace(".gz", ".gzidx"))
        destination_dir = os.path.split(destination_path)[0]
        os.makedirs(destination_dir, exist_ok = True)
        fobj.export_index(destination_path)

def build_index_for_prefix(in_s3_client, in_bucket_name, in_prefix):
    paginator = in_s3_client.get_paginator('list_objects_v2')
    indexables = []
    for page in paginator.paginate(Bucket=in_bucket_name, Prefix=in_prefix):
        for obj in page['Contents']:
            if obj['Key'].endswith('.gz'):
                indexables.append(obj['Key'])

    for indexable in indexables:
        print(f"Building index for {indexable}")
        build_index_for_file(in_s3_client, in_bucket_name, in_prefix)

s3 = session.client("s3", endpoint_url=config["s3_endpoint"], config=Config(signature_version='s3v4'))
bucket_name = "flatfiles"
object_key = "us_options_opra/quotes_v1/2025/01/2025-01-28.csv.gz"

build_index_for_file(s3, bucket_name, object_key)

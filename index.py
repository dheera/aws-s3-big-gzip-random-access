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

KB = 2 ** 10
MB = 2 ** 20
GB = 2 ** 30

def build_index(s3_client, bucket_name, object_key, rebuild_existing = False, spacing = 32*MB, readbuf_size = 16*MB, s3_destination = None):
    assert(object_key.endswith(".gz"))

    host = "__unknown_host__"
    try:
        host = s3_client._endpoint.host
        match = re.match(r"^(?:https?:\/\/)?([^\/]+)", host)
        host = match.group(1)
    except:
        print("Warning: failed to extract hostname from s3 client, maybe they changed the internal class structure")

    # Create seekable stream
    seekable_stream = SeekableS3Stream(s3_client, bucket_name, object_key)

    destination_path = os.path.join(".gzidx", host, bucket_name, object_key.replace(".gz", ".gzidx"))
    destination_dir = os.path.split(destination_path)[0]

    if not rebuild_existing and os.path.exists(destination_path):
        return

    # Use with indexed_gzip
    with indexed_gzip.IndexedGzipFile(fileobj=seekable_stream, spacing = spacing, readbuf_size = readbuf_size) as fobj:
        fobj.build_full_index()
        os.makedirs(destination_dir, exist_ok = True)
        fobj.export_index(destination_path)

    if s3_destination is not None:
        dest_client, dest_bucketname, dest_prefix = s3_destination
        dest_client.upload_file(destination_path, dest_bucketname, os.path.join(dest_prefix, host, bucket_name, object_key.replace(".gz", ".gzidx")))
        os.remove(destination_path)

def build_index_for_prefix(s3_client, bucket_name, in_prefix, spacing = 32*MB, readbuf_size = 16*MB, s3_destination = None):
    paginator = s3_client.get_paginator('list_objects_v2')
    indexables = []
    for page in paginator.paginate(Bucket=bucket_name, Prefix=in_prefix):
        for obj in page['Contents']:
            if obj['Key'].endswith('.gz'):
                indexables.append(obj['Key'])

    for indexable in indexables:
        print(f"Building index for {indexable}")
        build_index(s3_client, bucket_name, indexable, spacing = spacing, readbuf_size = readbuf_size, s3_destination = s3_destination)

def get_stream(s3_client, bucket_name, object_key, index_file = None):
    assert(object_key.endswith(".gz"))

    host = "__unknown_host__"
    try:
        host = s3_client._endpoint.host
        match = re.match(r"^(?:https?:\/\/)?([^\/]+)", host)
        host = match.group(1)
    except:
        print("Warning: failed to extract hostname from s3 client, maybe they changed the internal class structure")

    # Create seekable stream
    seekable_stream = SeekableS3Stream(s3_client, bucket_name, object_key)
    
    if index_file is None: # assume it is where we would have created it with the build_index() function
        index_file = os.path.join(".gzidx", host, bucket_name, object_key.replace(".gz", ".gzidx"))
    
    assert(os.path.exists(index_file))

    return indexed_gzip.IndexedGzipFile(fileobj=seekable_stream, index_file=index_file)


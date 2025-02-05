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
from index import build_index, get_stream,build_index_for_prefix

KB = 2 ** 10
MB = 2 ** 20
GB = 2 ** 30

# Step 0: Build the index

# This example is for building an index for a 100GB+ .csv.gz file from Polygon
# This example requires a paid subscription to the "Options Advanced" credentials
# but you can replace it with whatever .gz dataset you have access to

# You can build the indexes on an EC2 instance which should have free downlink bandwidth
# The indexes will be output to .gzidx/

with open("polygon.json", "r") as f:
    config = json.load(f)

with open("local.json", "r") as f:
    local_config = json.load(f)

session = boto3.Session(
   aws_access_key_id=config["s3_access_key_id"],
   aws_secret_access_key=config["s3_secret_access_key"],
)

session_local = boto3.Session(
   aws_access_key_id=local_config["s3_access_key_id"],
   aws_secret_access_key=local_config["s3_secret_access_key"],
)

s3 = session.client("s3", endpoint_url=config["s3_endpoint"], config=Config(signature_version='s3v4'))
bucket_name = "flatfiles"

local_s3 = session_local.client("s3")
local_bucketname = "fin.dheera.net"
local_prefix = "gzidx/files.polygon.io/"

import sys
year = sys.argv[1]
month = sys.argv[2]

# this will take a while
build_index_for_prefix(s3, bucket_name, f"us_options_opra/quotes_v1/{year}/{month}", spacing = 256*MB, readbuf_size = 64*MB, s3_destination = (local_s3, local_bucketname, local_prefix))



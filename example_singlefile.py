#!/usr/bin/env python3

import os
import boto3
import json

from boto3.s3.transfer import TransferConfig
from botocore.config import Config
import indexed_gzip # pip3 install indexed_gzip

from index import build_index, get_stream

KB = 2 ** 10
MB = 2 ** 20
GB = 2 ** 30

# Step 0: Build the index
# This example is for building an index for a 100GB+ .csv.gz file from Polygon
# This example requires a paid subscription to the "Options Advanced" credentials
# but you can replace it with whatever .gz dataset you have access to

with open("polygon.json", "r") as f:
    config = json.load(f)

session = boto3.Session(
   aws_access_key_id=config["s3_access_key_id"],
   aws_secret_access_key=config["s3_secret_access_key"],
)

s3 = session.client("s3", endpoint_url=config["s3_endpoint"], config=Config(signature_version='s3v4'))
bucket_name = "flatfiles"

object_key = "us_options_opra/quotes_v1/2025/01/2025-01-28.csv.gz" # 100GB+ large example

# this will take a while
build_index(s3, bucket_name, object_key)

# the resulting index that enables random access will be in .gzidx/
# you can copy this to wherever else you want before doing Step 1 below

# Step 1: Random access
# This can be done from anywhere even with a low bandwidth connection.

stream = get_stream(s3, bucket_name, object_key)

stream.seek(53*GB) # seek to a location 53GB into the file to demonstrate random access
stream.readline() # throwaway the first partial CSV line

for i in range(20): # read 20 more lines
    print(stream.readline())



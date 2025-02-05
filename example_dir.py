#!/usr/bin/env python3

import os
import boto3
import json

from boto3.s3.transfer import TransferConfig
from botocore.config import Config

from index import build_index_for_prefix

with open("polygon.json", "r") as f:
    config = json.load(f)

session = boto3.Session(
   aws_access_key_id=config["s3_access_key_id"],
   aws_secret_access_key=config["s3_secret_access_key"],
)

s3 = session.client("s3", endpoint_url=config["s3_endpoint"], config=Config(signature_version='s3v4'))
bucket_name = "flatfiles"

# this will take a while
build_index_for_prefix(s3, bucket_name, "us_options_opra/quotes_v1/2024/")


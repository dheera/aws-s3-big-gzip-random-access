#!/usr/bin/env python3

import boto3
import json
import io
from boto3.s3.transfer import TransferConfig
from botocore.config import Config

class SeekableS3Stream(io.RawIOBase):
    def __init__(self, s3_client, bucket, key):
        self.s3_client = s3_client
        self.bucket = bucket
        self.key = key
        self.position = 0
        self.file_size = self._get_file_size()
        self.stream = self._open_stream()

    def _get_file_size(self):
        response = self.s3_client.head_object(Bucket=self.bucket, Key=self.key)
        return response['ContentLength']

    def _open_stream(self):
        range_header = f'bytes={self.position}-'
        response = self.s3_client.get_object(Bucket=self.bucket, Key=self.key, Range=range_header)
        return response['Body']

    def seek(self, offset, whence=io.SEEK_SET):
        print(f"seeking to {offset}")
        if whence == io.SEEK_SET:
            self.position = offset
        elif whence == io.SEEK_CUR:
            self.position += offset
        elif whence == io.SEEK_END:
            self.position = self.file_size + offset
        self.position = max(0, min(self.position, self.file_size))
        self.stream = self._open_stream()
        return self.position

    def tell(self):
        return self.position

    def read(self, size=-1):
        print(f"reading {size}")
        if size == -1:
            size = self.file_size - self.position
        size = min(size, self.file_size - self.position)
        data = self.stream.read(size)
        self.position += len(data)
        return data


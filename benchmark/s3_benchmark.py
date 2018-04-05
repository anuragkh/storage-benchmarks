import logging

import boto3

from benchmark.latency import benchmark_latency
from benchmark.throughput import benchmark_throughput

logging.getLogger('boto3').setLevel(logging.FATAL)


class S3Client:
    def __init__(self, bucket_name):
        self.client = boto3.client('s3')
        self.bucket_name = bucket_name

    def get(self, key):
        return self.client.get_object(Bucket=self.bucket_name, Key=key)['Body'].read()

    def put(self, key, value):
        return self.client.put_object(Bucket=self.bucket_name, Key=key, Body=value)['VersionId']

    def update(self, key, value):
        return self.put(key, value)

    def remove(self, key):
        return self.client.delete_object(Bucket=self.bucket_name, Key=key)['VersionId']

    def remove_all(self):
        paginator = self.client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.bucket_name)

        delete_us = dict(Objects=[])
        for item in pages.search('Contents'):
            delete_us['Objects'].append(dict(Key=item['Key']))

            # flush once aws limit reached
            if len(delete_us['Objects']) >= 1000:
                self.client.delete_objects(Bucket=self.bucket_name, Delete=delete_us)
                delete_us = dict(Objects=[])

        # flush rest
        if len(delete_us['Objects']):
            self.client.delete_objects(Bucket=self.bucket_name, Delete=delete_us)


class S3ClientBuilder:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name

    def __call__(self):
        return S3Client(self.bucket_name)


def s3_bench_throughput(bucket, workload_path, workload_off=0, n_ops=100000, n_procs=1):
    client_builder = S3ClientBuilder(bucket)
    benchmark_throughput(workload_path, workload_off, client_builder, n_ops, n_procs)


def s3_bench_latency(bucket, workload_path, workload_off=0, n_ops=100000):
    client_builder = S3ClientBuilder(bucket)
    benchmark_latency(client_builder, workload_path, workload_off, n_ops)


def s3_clear(bucket):
    S3Client(bucket).remove_all()

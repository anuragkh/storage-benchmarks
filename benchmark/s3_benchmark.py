import boto3

from benchmark.latency import benchmark_latency
from benchmark.throughput import benchmark_throughput


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


class S3ClientBuilder:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name

    def __call__(self):
        return S3Client(self.bucket_name)


def s3_bench_throughput(bucket, workload_path, workload_off, n_ops=100000, n_procs=1):
    client_builder = S3ClientBuilder(bucket)
    benchmark_throughput(workload_path, workload_off, client_builder, n_ops, n_procs)


def s3_bench_latency(bucket, workload_path, workload_off, n_ops):
    client_builder = S3ClientBuilder(bucket)
    benchmark_latency(client_builder, workload_path, workload_off, n_ops)

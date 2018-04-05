#!/usr/bin/env python

import argparse
import logging
import sys

from benchmark.s3_benchmark import *

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s",
                    datefmt="%Y-%m-%d %X")


def run_benchmark(bucket):
    workload_paths = ['load', 'workloada', 'workloadb', 'workloadc', 'workloadd']
    tot_ops = 1000000
    n_ops = 100000
    for workload_path in workload_paths:
        workload_off = 0
        for n_proc in [1, 2, 4, 8, 16, 32]:
            logging.info(
                "Benchmarking sync throughput for workload_path=%s and num_thread=%d" % (workload_path, n_proc))
            output_file = workload_path + "-throughput-" + str(n_proc) + ".txt"
            stdout_backup = sys.stdout
            with open(output_file, 'w') as f:
                sys.stdout = f
                s3_bench_throughput(bucket, workload_path, workload_off, n_ops, n_proc)
            sys.stdout = stdout_backup
            workload_off += n_ops
        logging.info("Benchmarking sync latency for workload path %s" % workload_path)
        latency_ops = tot_ops - workload_off if workload_path.endswith('/load') else n_ops
        output_file = workload_path + "-latency.txt"
        stdout_backup = sys.stdout
        with open(output_file, 'w') as f:
            sys.stdout = f
            s3_bench_latency(bucket, workload_path, workload_off, latency_ops)
        sys.stdout = stdout_backup

    s3_clear(bucket)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Benchmark S3 operations.')
    parser.add_argument('--bucket', type=str, nargs=1, required=True, help='S3 bucket name')
    args = parser.parse_args()
    run_benchmark(args.bucket)

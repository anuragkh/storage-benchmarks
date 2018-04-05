#!/usr/bin/env python

import argparse
import logging
import sys

from benchmark.redis_benchmark import *

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s",
                    datefmt="%Y-%m-%d %X")


def run_benchmark(host, port):
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
                redis_bench_throughput(host, port, workload_path, workload_off, n_ops, n_proc)
            sys.stdout = stdout_backup
            workload_off += n_ops
        logging.info("Benchmarking sync latency for workload path %s" % workload_path)
        latency_ops = tot_ops - workload_off if workload_path.endswith('/load') else n_ops
        output_file = workload_path + "-latency.txt"
        stdout_backup = sys.stdout
        with open(output_file, 'w') as f:
            sys.stdout = f
            redis_bench_latency(host, port, workload_path, workload_off, latency_ops)
        sys.stdout = stdout_backup

    redis_clear(host, port)

    n_proc = 1
    for workload_path in workload_paths:
        workload_off = 0
        for max_async in [2, 4, 8, 16, 32, 64, 128, 256, 512, 1024]:
            logging.info(
                "Benchmarking async throughput for workload_path=%s and max_async=%d" % (workload_path, max_async))
            output_file = workload_path + "_async-throughput-" + str(max_async) + ".txt"
            stdout_backup = sys.stdout
            with open(output_file, 'w') as f:
                sys.stdout = f
                redis_bench_pipelined_throughput(host, port, workload_path, workload_off, n_ops, n_proc, max_async)
            sys.stdout = stdout_backup
            workload_off += n_ops
    redis_clear(host, port)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Benchmark Redis operations.')
    parser.add_argument('--host', type=str, default='127.0.0.1', help='Redis host')
    parser.add_argument('--port', type=int, default=6379, help='Redis port')
    args = parser.parse_args()
    run_benchmark(args.host, args.port)

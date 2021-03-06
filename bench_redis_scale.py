#!/usr/bin/env python

import argparse
import logging
import sys
import time
from multiprocessing import Process

import boto3

from benchmark.redis_benchmark import *

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s", datefmt="%Y-%m-%d %X")


def run_scale_up(cluster_name, from_n, to_n):
    client = boto3.client('elasticache')
    with open('scale_nodes', 'w') as f:
        f.write('{} {}\n'.format(time.time() * 1e6, from_n))
        for i in range(from_n + 1, to_n + 1):
            time.sleep(60)
            f.write('{} {}\n'.format(time.time() * 1e6, i - 1))
            logging.info('Scaling cluster={} to {} nodes'.format(cluster_name, i))
            response = client.modify_replication_group_shard_configuration(
                ReplicationGroupId=cluster_name,
                NodeGroupCount=i,
                ApplyImmediately=True,
            )
            logging.info('Response: {}'.format(response))
            waiter = client.get_waiter('cache_cluster_available')
            logging.info('Waiting for scaling to complete')
            start = time.time()
            waiter.wait(
                WaiterConfig={
                    'Delay': 1,
                    'MaxAttempts': 86400
                }
            )
            end = time.time()
            f.write('{} {}\n'.format(end * 1e6, i))
            logging.info('Scaling to {} nodes complete in {}s'.format(i, end - start))


def run_benchmark(host, port, num_ops, num_clients):
    logging.info("Benchmarking scale for redis")
    client_builder = RedisClusterClientBuilder(host, port)
    benchmark_scale(client_builder, num_ops, num_clients)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Benchmark ElastiCache scaling.')
    parser.add_argument('--host', type=str, default='127.0.0.1', help='ElastiCache host')
    parser.add_argument('--port', type=int, default=6379, help='ElastiCache port')
    parser.add_argument('--cluster', type=str, default='mycluster', help='ElastiCache cluster name')
    parser.add_argument('--count', type=int, default=sys.maxsize, help='Number items in Redis')
    parser.add_argument('--num-clients', type=int, default=1, help='Number of ElastiCache clients')
    parser.add_argument('--initial-scale', type=int, default=1, help='Initial #nodes in ElastiCache cluster')
    parser.add_argument('--final-scale', type=int, default=5, help='Final #nodes in ElastiCache cluster')

    args = parser.parse_args()
    p = Process(target=run_scale_up, args=(args.cluster, args.initial_scale, args.final_scale))
    p.start()
    run_benchmark(args.host, args.port, args.count, args.num_clients)
    p.join()

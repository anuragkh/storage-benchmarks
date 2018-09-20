#!/usr/bin/env python

import argparse
import logging
import sys
import time

from benchmark.redis_benchmark import *

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s", datefmt="%Y-%m-%d %X")


def load_data(host, port, num_ops, value_size=128):
    logging.info("Loading data to {}:{}".format(host, port))
    client_builder = RedisClusterClientBuilder(host, port)
    client = client_builder()
    value = 'x' * value_size
    logging.info("Starting load...")
    begin = time.time()
    for i in range(num_ops):
        client.put(str(i), value)
    tot = time.time() - begin
    logging.info("Completed scale benchmark in {}s".format(tot))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Load data into Redis cluster.')
    parser.add_argument('--host', type=str, default='127.0.0.1', help='Redis host')
    parser.add_argument('--port', type=int, default=6379, help='Redis port')
    parser.add_argument('--count', type=int, default=sys.maxsize, help='Number of data items to load')
    parser.add_argument('--value-size', type=int, default=128, help='Value size')

    args = parser.parse_args()
    load_data(args.host, args.port, args.count, args.value_size)

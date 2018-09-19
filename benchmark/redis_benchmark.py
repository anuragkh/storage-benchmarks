from redis import StrictRedis
from rediscluster import StrictRedisCluster

from benchmark.latency import benchmark_latency
from benchmark.scale import benchmark_scale
from benchmark.throughput import benchmark_throughput


class RedisClient:
    def __init__(self, host='127.0.0.1', port=6379):
        self.r = StrictRedis(host=host, port=port, db=0)

    def get(self, key):
        return self.r.get(key)

    def put(self, key, value):
        return self.r.set(key, value, nx=True)

    def update(self, key, value):
        return self.r.set(key, value, xx=True)

    def remove(self, key):
        return self.r.delete(key)

    def remove_all(self):
        return self.r.flushdb()


class RedisClusterClient:
    def __init__(self, host='127.0.0.1', port=6379):
        self.r = StrictRedisCluster(startup_nodes=[{"host": host, "port": port}], decode_responses=True)

    def get(self, key):
        return self.r.get(key)

    def put(self, key, value):
        return self.r.set(key, value, nx=True)

    def update(self, key, value):
        return self.r.set(key, value, xx=True)

    def remove(self, key):
        return self.r.delete(key)

    def remove_all(self):
        return self.r.flushdb()


class RedisPipelinedClient:
    def __init__(self, host='127.0.0.1', port=6379, max_async=2):
        self.r = StrictRedis(host=host, port=port, db=0).pipeline(transaction=False)
        self.max_async = max_async
        self.count = 0

    def _pipe_check(self):
        self.count += 1
        if self.count % self.max_async == 0:
            self.r.execute()

    def get(self, key):
        self.r.get(key)
        self._pipe_check()

    def put(self, key, value):
        self.r.set(key, value, nx=True)
        self._pipe_check()

    def update(self, key, value):
        self.r.set(key, value, xx=True)
        self._pipe_check()

    def remove(self, key):
        self.r.delete(key)
        self._pipe_check()


class RedisClientBuilder:
    def __init__(self, host='127.0.0.1', port=6379):
        self.host = host
        self.port = port

    def __call__(self):
        return RedisClient(self.host, self.port)


class RedisClusterClientBuilder:
    def __init__(self, host='127.0.0.1', port=6379):
        self.host = host
        self.port = port

    def __call__(self):
        return RedisClusterClient(self.host, self.port)


class RedisPipelinedClientBuilder:
    def __init__(self, host='127.0.0.1', port=6379, max_async=2):
        self.host = host
        self.port = port
        self.max_async = max_async

    def __call__(self):
        return RedisPipelinedClient(self.host, self.port, self.max_async)


def redis_bench_throughput(host, port, workload_path, workload_off=0, n_ops=100000, n_procs=1):
    client_builder = RedisClientBuilder(host, port)
    benchmark_throughput(workload_path, workload_off, client_builder, n_ops, n_procs)


def redis_bench_latency(host, port, workload_path, workload_off=0, n_ops=100000):
    client_builder = RedisClientBuilder(host, port)
    benchmark_latency(client_builder, workload_path, workload_off, n_ops)


def redis_bench_pipelined_throughput(host, port, workload_path, workload_off, n_ops=100000, n_procs=1, max_async=2):
    client_builder = RedisPipelinedClientBuilder(host, port, max_async)
    benchmark_throughput(workload_path, workload_off, client_builder, n_ops, n_procs)


def redis_bench_scale(host, port, n_ops=5000000, n_procs=1, value_size=1024):
    client_builder = RedisClusterClientBuilder(host, port)
    benchmark_scale(client_builder, n_ops, n_procs, value_size)


def redis_clear(host, port):
    RedisClient(host, port).remove_all()

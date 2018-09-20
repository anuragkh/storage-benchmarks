import logging
import sys
import time
from multiprocessing import Process, Barrier

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s",
                    datefmt="%Y-%m-%d %X")


def _run_workload(barrier, client_id, client_builder, count):
    client = client_builder()
    barrier.wait()
    ops = 0
    logging.info("[Process] Starting scale benchmark...")
    with open('scale_latency_%d' % client_id, 'w') as f:
        while True:
            begin = time.time()
            client.get(str(ops % count))
            elapsed = time.time() - begin
            f.write("%f %f\n" % (begin * 1e6, elapsed * 1e6))
            ops += 1


def benchmark_scale(client_builder, count=sys.maxsize, n_procs=1):
    barrier = Barrier(n_procs)
    logging.info("[Master] Creating processes with n_ops=%d, n_procs=%d..." % (count, n_procs))
    benchmark = [Process(target=_run_workload, args=(barrier, i, client_builder, count,)) for i in range(n_procs)]

    for b in benchmark:
        b.start()

    for b in benchmark:
        b.join()
    logging.info("[Master] Benchmark complete.")

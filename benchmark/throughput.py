import logging

import time

from multiprocessing import Condition, Value, Process, Barrier

from benchmark.workload import make_workload

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s",
                    datefmt="%Y-%m-%d %X")


def _load_and_run_workload(load_barrier, workload_path, workload_off, client_builder, n_ops, n_procs,
                           log_interval):
    client = client_builder()
    workload = make_workload(workload_path, workload_off, n_ops, client)

    logging.info("[Process] Loaded data for process.")
    load_barrier.wait()
    logging.info("[Process] Starting benchmark...")

    ops = 0
    begin = time.time()
    while ops < len(workload):
        workload[ops][0](*workload[ops][1])
        ops += 1
        if ops % log_interval == 0:
            logging.info("[Process] Completed %d ops" % ops)
    end = time.time()

    logging.info("[Process] Benchmark completed: %d ops in %f seconds" % (ops, (end - begin)))
    print float(ops) / (end - begin)


def benchmark_throughput(workload_path, workload_off, client_builder, n_ops, n_procs, log_interval=100000):
    load_barrier = Barrier(n_procs)
    logging.info("[Master] Creating processes with workload_path=%s, workload_off=%d, n_ops=%d, n_procs=%d..." %
                 (workload_path, workload_off, n_ops, n_procs))
    benchmark = [Process(target=_load_and_run_workload,
                         args=(load_barrier, workload_path, workload_off + i * (n_ops / n_procs),
                               client_builder, int(n_ops / n_procs), n_procs, log_interval,))
                 for i in range(n_procs)]

    for b in benchmark:
        b.start()

    for b in benchmark:
        b.join()
    logging.info("[Master] Benchmark complete.")

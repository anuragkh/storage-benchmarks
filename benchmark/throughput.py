import logging

import time

from multiprocessing import Condition, Value, Process

from benchmark.workload import make_workload

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s",
                    datefmt="%Y-%m-%d %X")


def _load_and_run_workload(n_load, load_cv, start_cv, workload_path, workload_off, client_builder, n_ops, n_procs):
    client = client_builder()
    workload = make_workload(workload_path, workload_off, n_ops, client)

    with load_cv:
        n_load.value += 1
        logging.info("[Process] Loaded data for process.")
        if n_load.value == n_procs:
            logging.info("[Process] All processes completed loading, notifying master...")
            load_cv.notify()

    with start_cv:
        logging.info("[Process] Waiting for master to start...")
        start_cv.wait()

    logging.info("[Process] Starting benchmark...")

    ops = 0
    begin = time.clock()
    while ops < len(workload):
        workload[ops][0](*workload[ops][1])
        ops += 1
    end = time.clock()

    print float(ops) / (end - begin)


def benchmark_throughput(workload_path, workload_off, client_builder, n_ops, n_procs):
    load_cv = Condition()
    start_cv = Condition()
    n_load = Value('i', 0)
    logging.info("[Master] Creating processes with workload_path=%s, workload_off=%d, n_ops=%d, n_procs=%d..." %
                 (workload_path, workload_off, n_ops, n_procs))
    benchmark = [Process(target=_load_and_run_workload,
                         args=(n_load, load_cv, start_cv, workload_path, workload_off + i * (n_ops / n_procs),
                               client_builder, int(n_ops / n_procs), n_procs,))
                 for i in range(n_procs)]

    for b in benchmark:
        b.start()

    logging.info("[Master] Waiting for processes to load data...")
    with load_cv:
        load_cv.wait()

    logging.info("[Master] Notifying processes to start...")
    with start_cv:
        start_cv.notify_all()

    for b in benchmark:
        b.join()
    logging.info("[Master] Benchmark complete.")

import logging
import time
from multiprocessing import Process, Barrier

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s",
                    datefmt="%Y-%m-%d %X")


def _run_workload(barrier, client_id, client_builder, n_ops, value_size):
    client = client_builder()
    logging.info("[Process] Loaded data for process.")

    barrier.wait()
    logging.info("[Process] Starting benchmark...")

    ops = 0
    tot_time = 0.0
    key_off = client_id * n_ops
    value = 'x' * value_size
    logging.info("[Process] Starting scale benchmark...")
    with open('scale_latency_%d' % client_id, 'w') as f:
        while ops < n_ops:
            begin = time.time()
            client.put(str(key_off + ops), value)
            tot = time.time() - begin
            f.write("%f %f\n" % (begin * 1e6, tot * 1e6))
            ops += 1
            tot_time += (tot * 1e6)
        logging.info("[Process] Completed scale benchmark; average latency = %f us" % (tot_time / ops))


def benchmark_scale(client_builder, n_ops=5000000, n_procs=1, value_size=1024):
    barrier = Barrier(n_procs)
    logging.info("[Master] Creating processes with n_ops=%d, n_procs=%d..." % (n_ops, n_procs))
    benchmark = [Process(target=_run_workload, args=(barrier, i, client_builder, int(n_ops / n_procs), value_size,))
                 for i in range(n_procs)]

    for b in benchmark:
        b.start()

    for b in benchmark:
        b.join()
    logging.info("[Master] Benchmark complete.")

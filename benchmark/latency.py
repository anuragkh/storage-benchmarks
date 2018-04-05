import logging
import time

from benchmark.workload import make_workload


def _load_and_run_latency_benchmark(client_builder, workload_path, workload_off=0, n_ops=100000):
    client = client_builder()
    workload = make_workload(workload_path, workload_off, n_ops, client)

    ops = 0
    tot_time = 0.0
    logging.info("Starting latency benchmark...")
    while ops < len(workload):
        begin = time.clock()
        workload[ops][0](*workload[ops][1])
        tot = time.clock() - begin
        print "%f" % (tot * 1e6)
        ops += 1
        tot_time += (tot * 1e6)
    logging.info("Completed latency benchmark; average latency = %f us" % tot_time / ops)


def benchmark_latency(client_builder, workload_path, workload_off=0, n_ops=100000):
    _load_and_run_latency_benchmark(client_builder, workload_path, workload_off, n_ops)

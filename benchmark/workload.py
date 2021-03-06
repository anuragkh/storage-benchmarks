import logging

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s",
                    datefmt="%Y-%m-%d %X")


def make_workload(path, off, count, client):
    logging.info("Loading %d entries from %s from offset %d" % (count, path, off))
    with open(path) as f:
        ops = [x.strip().split() for x in f.readlines()[int(off):int(off + count)]]
        workload = [[getattr(client, x[0]), x[1:]] for x in ops]

    logging.info("Loaded %d entries from %s from offset %d" % (len(workload), path, off))
    return workload

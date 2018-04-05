def make_workload(path, off, count, client):
    with open(path) as f:
        ops = [x.strip().split() for x in f.readlines()[off:(off + count)]]
        workload = [[getattr(client, x[0]), x[1:]] for x in ops]

    return workload

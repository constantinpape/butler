import sys
import time
from concurrent import futures
import numpy as np

# sys.path.append('/home/papec/Work/my_projects/z5/bld/python')
sys.path.append('/home/papec/Work/software/bld/z5/python')
sys.path.append('../..')


def dummy_worker(worker_id):
    import z5py
    from butler.block_service import BlockClient
    host, port = "localhost", 9999
    client = BlockClient(host, port)
    out_file = './output.n5'
    block_shape = (100, 100, 100)
    ds = z5py.File(out_file)['out']

    x = np.ones(block_shape, dtype='uint8')

    print("Starting inference, worker", worker_id)
    while True:
        block_offset = client.request()
        # print("Processing block", block_offset)
        if block_offset is None:
            break

        roi = tuple(slice(bo, bo + bs) for bo, bs in zip(block_offset, block_shape))
        ds[roi] += x

    print("Done inference, worker", worker_id)


if __name__ == '__main__':
    t0 = time.time()
    n_workers = int(sys.argv[1])
    with futures.ProcessPoolExecutor(n_workers) as pp:
        tasks = [pp.submit(dummy_worker, worker) for worker in range(n_workers)]
    print("Processing with", n_workers, "workers took", time.time() - t0)

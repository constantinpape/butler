import os
import sys
import json
import logging
from shutil import rmtree

sys.path.append('/home/papec/Work/my_projects/z5/bld/python')
# sys.path.append('/home/papec/Work/software/bld/z5/python')
sys.path.append('../..')


# make a dummy block list
# and output file
def setup():
    import z5py
    shape = (1000, 1000, 1000)
    chunks = (100, 100, 100)

    if os.path.exists('./output.n5'):
        rmtree('./output.n5')

    f = z5py.File('./output.n5')
    f.create_dataset('out', shape=shape, chunks=chunks, dtype='uint8', compression='gzip')

    block_list = []
    for z in range(10):
        for y in range(10):
            for x in range(10):
                block_list.append([z*100, y*100, x*100])

    with open('block_list.json', 'w') as f:
        json.dump(block_list, f)


def start_service():
    from butler import start_service
    from butler.block_service import BlockService, BlockRequestHandler
    setup()
    host, port = "localhost", 9999
    block_list = './block_list.json'
    # TODO properly set up logger
    logging.basicConfig(level=logging.INFO)
    service = BlockService(block_list, 20, 10, out_prefix='./service_status_')
    start_service(host, port, service, BlockRequestHandler)


if __name__ == '__main__':
    start_service()

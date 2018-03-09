import os
import json
import threading
import time
from collections import deque

from ..base import BaseRequestHandler, BaseService, BaseClient


class BlockRequestHandler(BaseRequestHandler):
    """
    BlockRequestHandler
    """
    def format_request(self, request):
        """
        Format the request: single word will result in requesting a new block,
        3 words will confirm this block
        """
        request = request.split()
        # if we have a length of 1, a new block is requested, otherwise a block is confirmed
        if len(request) == 1:
            return None
        elif len(request) == 3:
            if not all(reg.isdigit() for reg in request):
                raise RuntimeError("Invalid block request")
            return [int(req) for req in request]
        else:
            raise RuntimeError("Invalid block request")

    def format_response(self, response):
        """
        Format the response: return 0 or 1 for a confirmation request,
        return the block offsets for a block request,
        return "stop" if all requests are processed (None)
        """
        if isinstance(response, bool):
            response = "0" if response else "1"
        elif isinstance(response, list):
            assert len(response) == 3
            response = " ".join(map(str, response))
        elif response is None:
            response = "stop"
        else:
            raise RuntimeError("Invalid response")
        return response


class BlockService(BaseService):
    """
    Provide workers with block offsets from requests.
    Blocks need to be confirmed and the service periodically checks for
    blocks that are over the time limit.
    """

    def __init__(self, block_file, logger, time_limit,
                 check_interval=60, num_retries=2, out_path=None):

        # initialize the base class
        super(BlockService, self).__init__(logger)
        self.logger.info("Init BlockService:")

        # time limit and check interval;
        assert time_limit > check_interval
        self.time_limit = time_limit
        self.check_interval = check_interval
        self.logger.info("with time_limit: %i and check_interval: %i" % (time_limit, check_interval))

        # number of retries for failed blocks
        self.num_retries = num_retries
        self.try_counter = 0
        self.logger.info("with num_retries: %i" % num_retries)

        # the outpath to serialize failed blocks
        self.out_path = out_path
        if self.out_path is not None:
            self.logger.info("will serialize failed blocks at: %s" % self.out_path)
        else:
            self.logger.warn("will not serialize failed blocks")

        # load the coordinates of the blocks that will be processed
        # make a queue containing all block offsets
        assert os.path.exists(block_file), block_file
        with open(block_file, 'r') as f:
            self.block_queue = deque(reversed(json.load(f)))
        self.logger.info("loaded block list from: %s" % block_file)

        # list to keep track of ids that are currently processed
        self.in_progress = []
        self.time_stamps = []
        # list of offsets that have been processed
        self.processed_list = []
        # list of failed blocks
        self.failed_blocks = []
        self.lock = threading.Lock()

        # start the background thread that checks for failed jobs
        bg_thread = threading.Thread(target=self.check_progress_list, args=())
        bg_thread.daemon = True
        bg_thread.start()

    def process_request(self, request):
        self.logger.debug("process incomig request: %s" % str(request))
        # request a new block
        if request is None:
            return self.request_block()
        # confirm a block
        else:
            return self.confirm_block(request)

    # check the progress list for blocks that have exceeded the time limit
    def check_progress_list(self):
        while True:
            time.sleep(self.check_interval)
            with self.lock:
                now = time.time()
                self.logger.debug("checking progress list for %i blocks" % len(self.time_stamps))
                # find blocks that have exceeded the time limit
                failed_block_ids = [ii for ii, time_stamp in enumerate(self.time_stamps)
                                    if now - time_stamp > self.time_limit]
                self.logger.debug("found %i blocks over the time limit" % len(failed_block_ids))
                # remove failed blocks and time stamps from in progress and
                # append failed blocks to the failed list
                # NOTE: we need to iterate in reverse order to delete the correct elements
                for ii in sorted(failed_block_ids, reverse=True):
                    del self.time_stamps[ii]
                    self.failed_blocks.append(self.in_progress.pop(ii))

    # request the next block to be processed
    # if no more blocks are present, return None
    def request_block(self):
        with self.lock:
            if len(self.block_queue) > 0:
                block_offsets = self.block_queue.pop()
                self.in_progress.append(block_offsets)
                self.time_stamps.append(time.time())
                self.logger.debug("returning block offsets: %s" % str(block_offsets))
            else:
                # check if we still repopulate, otherwise
                # return `None` and initiate shutdowns
                if self.try_counter < self.num_retries and self.failed_blocks:
                    self.logger.info("exhausted block queue, repopulating for %i time" % self.try_counter)
                    block_offsets = self.repopulate_queue()
                    self.try_counter += 1
                else:
                    self.logger.info("exhausted block queue, shutting down service")
                    block_offsets = None
                    self.serialize_failed_blocks()
                    self.shutdown_server()
        return block_offsets

    # confirm that a block has been processed
    def confirm_block(self, block_offset):
        # see of the offset is still in the in-progress
        # list and remove it.
        # if not, the time limit was exceeded and something is most likely wrong
        # with the block and the block was put on the failed block list
        try:
            index = self.in_progress.index(block_offset)
            with self.lock:
                del self.in_progress[index]
                del self.time_stamps[index]
                self.processed_list.append(block_offset)
            success = True
            self.logger.debug("block offsets %s were confirmed" % str(block_offset))
        except ValueError:
            success = False
            self.logger.debug("block offsets %s were not confirmed" % str(block_offset))
        return success

    def repopulate_queue(self):
        self.block_queue.extendleft(self.failed_blocks)
        block_offsets = self.block_queue.pop()
        self.in_progress.append(block_offsets)
        self.time_stamps.append(time.time())
        self.logger.debug("returning block offsets: %s" % str(block_offsets))
        return block_offsets

    def serialize_failed_blocks(self):
        if self.out_path is not None:
            with open(self.out_path, 'w') as f:
                json.dump(self.failed_blocks, f)


class BlockClient(BaseClient):
    """
    """
    def format_request(self, request):
        """
        Format incoming request.
        Must return string.
        """
        return "1" if request is None else " ".join(map(str, request))

    def format_response(self, response):
        """
        Format incoming response.
        """
        response = response.split()
        # if the response has length 3, it consists of
        # block coordinates
        if len(response) == 3:
            return list(map(int, response))
        else:
            return None if response is 'stop' else bool(int(response))

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

    def __init__(self, block_file, time_limit,
                 check_interval=60, num_retries=2, out_prefix=None):

        # initialize the base class
        super(BlockService, self).__init__()
        self.logger.info(" Init BlockService:")

        # time limit and check interval;
        assert time_limit > check_interval
        self.time_limit = time_limit
        self.check_interval = check_interval
        self.logger.info(" time_limit: %i and check_interval: %i" % (time_limit, check_interval))

        # number of retries for failed blocks
        self.num_retries = num_retries
        self.try_counter = 0
        self.logger.info(" num_retries: %i" % num_retries)

        # the outpath to serialize failed blocks
        self.out_prefix = out_prefix
        if self.out_prefix is not None:
            self.logger.info(" Will serialize failed blocks at: %s" % self.out_prefix)
        else:
            self.logger.warn(" Will not serialize failed blocks, you can serialize them by passing argument `out_prefix`")

        # load the coordinates of the blocks that will be processed
        # make a queue containing all block offsets
        assert os.path.exists(block_file), block_file
        with open(block_file, 'r') as f:
            self.block_queue = deque(reversed(json.load(f)))
        self.logger.info(" Loaded block list from: %s" % block_file)
        self.logger.info(" Added %i blocks to queue" % len(self.block_queue))

        # list to keep track of ids that are currently processed
        self.in_progress = []
        self.time_stamps = []
        # list of offsets that have been processed
        self.processed_list = []
        # list of failed blocks
        self.failed_blocks = []
        self.lock = threading.Lock()

        # start the background thread that checks for failed jobs
        self.bg_thread = threading.Thread(target=self.check_progress_list, args=())
        self.bg_thread.daemon = True
        self.bg_thread.start()

    def process_request(self, request):
        self.logger.debug(" Process incomig request: %s" % str(request))
        # request a new block
        if request is None:
            return self.request_block()
        # confirm a block
        else:
            return self.confirm_block(request)

    # check the progress list for blocks that have exceeded the time limit
    def check_progress_list(self):
        while self.server_is_running:
            time.sleep(self.check_interval)
            with self.lock:
                now = time.time()
                self.logger.debug(" Checking progress list for %i blocks" % len(self.time_stamps))
                # find blocks that have exceeded the time limit
                failed_block_ids = [ii for ii, time_stamp in enumerate(self.time_stamps)
                                    if now - time_stamp > self.time_limit]
                self.logger.info(" Found %i blocks over the time limit" % len(failed_block_ids))
                # remove failed blocks and time stamps from in progress and
                # append failed blocks to the failed list
                # NOTE: we need to iterate in reverse order to delete the correct elements
                for ii in sorted(failed_block_ids, reverse=True):
                    del self.time_stamps[ii]
                    self.failed_blocks.append(self.in_progress.pop(ii))

    # request the next block to be processed
    # if no more blocks are present, return None
    def request_block(self):

        # return a block offset if we still have blocks in the quee
        if len(self.block_queue) > 0:
            with self.lock:
                block_offsets = self.block_queue.pop()
                self.in_progress.append(block_offsets)
                self.time_stamps.append(time.time())
                self.logger.debug(" Returning block offsets: %s" % str(block_offsets))

        # otherwise, wait for the ones in progress to finish (or be cancelled)
        # then either repopulate, or exit
        else:

            # NOTE this must not be locked, otherwise
            # we end up with a deadlock with the lock in `check_progress_list`
            while self.in_progress:
                time.sleep(self.check_interval)
                continue

            with self.lock:
                # we need to check again inf the block queue is empty, because it might have been repopulated
                # in the meantime already
                if len(self.block_queue) > 0:
                    block_offsets = self.block_queue.pop()
                    self.in_progress.append(block_offsets)
                    self.time_stamps.append(time.time())
                    self.logger.debug(" Returning block offsets: %s" % str(block_offsets))
                elif self.try_counter < self.num_retries and self.failed_blocks:
                    self.logger.info(" Exhausted block queue, repopulating for %i time" % self.try_counter)
                    block_offsets = self.repopulate_queue()
                    self.try_counter += 1
                else:
                    block_offsets = None
                    if self.server_is_running:
                        self.logger.info(" Exhausted block queue, shutting down service")
                        self.serialize_status()
                        self.shutdown_server()
        return block_offsets

    # confirm that a block has been processed
    def confirm_block(self, block_offset):
        # see of the offset is still in the in-progress
        # list and remove it.
        # if not, the time limit was exceeded and something is most likely wrong
        # with the block and the block was put on the failed block list
        self.logger.debug(" Confirming block %s" % str(block_offset))
        try:
            with self.lock:
                index = self.in_progress.index(block_offset)
                del self.in_progress[index]
                del self.time_stamps[index]
                self.processed_list.append(block_offset)
            success = True
            self.logger.debug(" Block %s was processed properly." % str(block_offset))
        except ValueError:
            success = False
            self.logger.debug(" Block %s is over time limit and was added to failed blocks" % str(block_offset))
        return success

    def repopulate_queue(self):
        self.block_queue.extendleft(self.failed_blocks)
        self.failed_blocks = []
        block_offsets = self.block_queue.pop()
        self.in_progress.append(block_offsets)
        self.time_stamps.append(time.time())
        self.logger.debug(" Returning block offsets: %s" % str(block_offsets))
        return block_offsets

    def serialize_status(self, from_interrupt=False):
        """
        Serialize the status (failed blocks, processed blocks, in-progress blocks)
        """
        if from_interrupt:
            self.logger.info(" serialize_status called after interrupt")
        else:
            self.logger.info(" serialize_status called after regular shutdown")

        if self.out_prefix is not None:
            if self.failed_blocks:
                out_failed_blocks = self.out_prefix + "failed_blocks.json"
                self.logger.info(" Serialized list of failed blocks with %i entries to %s" %
                                 (len(self.failed_blocks), out_failed_blocks))
                with open(out_failed_blocks, 'w') as f:
                    json.dump(self.failed_blocks, f)

            if self.processed_list:
                out_processed_blocks = self.out_prefix + "processed_blocks.json"
                self.logger.info(" Serialized list of processed blocks with %i entries to %s" %
                                 (len(self.processed_list), out_processed_blocks))
                with open(out_processed_blocks, 'w') as f:
                    json.dump(self.processed_list, f)

            if self.in_progress:
                out_in_progress = self.out_prefix + "inprogress_blocks.json"
                self.logger.info(" Serialized list of in-progress blocks with %i entries to %s" %
                                 (len(self.in_progress), out_in_progress))
                with open(self.out_prefix + "inprogress_blocks.json", 'w') as f:
                    json.dump(self.in_progress, f)


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
            response = response[0]
            return None if response is 'stop' else bool(int(response))

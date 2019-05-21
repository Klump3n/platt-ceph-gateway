#!/usr/bin/env python3
"""
Test the integration of the client and server.

"""
import unittest

try:
    import modules.backend_manager as backend_manager
except ImportError:
    import sys
    sys.path.append('../../..')
    import modules.backend_manager as backend_manager

from util.loggers import CoreLog as cl, BackendLog as bl, SimulationLog as sl

import modules.tests.unittests.backend_manager_client as client

import multiprocessing
import asyncio
import time
import os
import hashlib
import queue

class Test_BackendManager_ClientOrServerDown(unittest.TestCase):

    def setUp(self):

        cl("debug")
        bl("debug")
        sl("debug")

        # queues for pushing information about new files over the socket
        # send from server
        self.new_file_server_queue = multiprocessing.Queue()
        # receive at client
        self.new_file_client_queue = multiprocessing.Queue()

        #
        # a queue for returning the requested index
        self.queue_datacopy_backend_index_data = multiprocessing.Queue()
        #
        # queue for index data on client side
        self.queue_client_index_data = multiprocessing.Queue()

        # server and client side of index exchange
        # index request events
        self.get_index_server_event = multiprocessing.Event()
        self.get_index_client_event = multiprocessing.Event()
        # index avail events
        self.index_avail_server_event = multiprocessing.Event()
        self.index_avail_client_event = multiprocessing.Event()
        # index pipes
        self.server_index_pipe = multiprocessing.Pipe()
        self.client_index_pipe = multiprocessing.Pipe()
        (
            self.server_index_pipe_local,
            self.server_index_pipe_remote
        ) = self.server_index_pipe
        (
            self.client_index_pipe_local,
            self.client_index_pipe_remote
        ) = self.client_index_pipe

        # queues for getting files from the ceph cluster
        # request file at server
        self.file_name_request_server_queue = multiprocessing.Queue()
        # send file contents, name and hash from server
        self.file_contents_name_hash_server_queue = multiprocessing.Queue()
        # request file name at client
        self.file_name_request_client_queue = multiprocessing.Queue()
        # receive file contents, name and hash on client
        self.file_contents_name_hash_client_queue = multiprocessing.Queue()

        self.shutdown_backend_manager_event = multiprocessing.Event()
        self.shutdown_client_event = multiprocessing.Event()

        print()

    def tearDown(self):
        self.shutdown_backend_manager_event.set()
        self.shutdown_client_event.set()
        time.sleep(1)
        self.shutdown_backend_manager_event.clear()
        self.shutdown_client_event.clear()
        time.sleep(1)

    def test_server_not_up(self):
        """Server has not been started

        """
        self.client = multiprocessing.Process(
            target=client.Client,
            args=(
                "localhost", 9001,
                self.new_file_client_queue,
                self.get_index_client_event,
                self.queue_client_index_data,
                # self.index_avail_client_event,
                # self.client_index_pipe_remote,
                self.file_name_request_client_queue,
                self.file_contents_name_hash_client_queue,
                self.shutdown_client_event,
            )
        )
        self.client.start()
        time.sleep(.01)

        time.sleep(1)

        self.server = multiprocessing.Process(
            target=backend_manager.BackendManager,
            args=(
                "localhost", 9001,
                self.new_file_server_queue,
                self.get_index_server_event,
                self.queue_datacopy_backend_index_data,
                # self.index_avail_server_event,
                # self.server_index_pipe_remote,
                self.file_name_request_server_queue,
                self.file_contents_name_hash_server_queue,
                self.shutdown_backend_manager_event,
            )
        )
        self.server.start()

        time.sleep(5)
        self.shutdown_backend_manager_event.set()
        time.sleep(.1)
        self.server.terminate()

        try:
            time.sleep(5)
            self.shutdown_client_event.set()
            time.sleep(.1)
            self.client.terminate()
        except Exception as e:
            print(e)

    def test_client_not_up(self):
        """Client has not been started (is never connecting)

        """
        self.server = multiprocessing.Process(
            target=backend_manager.BackendManager,
            args=(
                "localhost", 9001,
                self.new_file_server_queue,
                self.get_index_server_event,
                self.queue_datacopy_backend_index_data,
                # self.index_avail_server_event,
                # self.server_index_pipe_remote,
                self.file_name_request_server_queue,
                self.file_contents_name_hash_server_queue,
                self.shutdown_backend_manager_event,
            )
        )
        self.server.start()

        # race condition
        self.new_file_server_queue.put("test")
        self.assertEqual(self.new_file_server_queue.get(), "test")

        time.sleep(.5 + .1)      # init in backend mgr is .5 seconds
        # "test" should have been cleared out of the queue
        self.new_file_server_queue.put("test")
        time.sleep(.1)
        with self.assertRaises(queue.Empty):
            self.new_file_server_queue.get(False)

        # too lazy to test the rest; having a bad day

        try:
            time.sleep(1)
            self.shutdown_backend_manager_event.set()
            time.sleep(.1)
            self.server.terminate()
        except:
            pass

    def test_client_shut_down_mid_process(self):
        """Client has not been started (is never connecting)

        """
        self.server = multiprocessing.Process(
            target=backend_manager.BackendManager,
            args=(
                "localhost", 9001,
                self.new_file_server_queue,
                self.get_index_server_event,
                self.queue_datacopy_backend_index_data,
                # self.index_avail_server_event,
                # self.server_index_pipe_remote,
                self.file_name_request_server_queue,
                self.file_contents_name_hash_server_queue,
                self.shutdown_backend_manager_event,
            )
        )
        self.server.start()

        self.client = multiprocessing.Process(
            target=client.Client,
            args=(
                "localhost", 9001,
                self.new_file_client_queue,
                self.get_index_client_event,
                self.queue_client_index_data,
                # self.index_avail_client_event,
                # self.client_index_pipe_remote,
                self.file_name_request_client_queue,
                self.file_contents_name_hash_client_queue,
                self.shutdown_client_event,
            )
        )
        self.client.start()

        time.sleep(.01)

        # # race condition
        # self.new_file_server_queue.put("test")
        # # self.assertEqual(self.new_file_server_queue.get(), "test")

        # # "test" should have been cleared out of the queue
        # self.new_file_server_queue.put("test")
        # time.sleep(.1)
        # with self.assertRaises(queue.Empty):
        #     self.new_file_server_queue.get(False)

        time.sleep(.3)
        self.shutdown_client_event.set()
        time.sleep(.1)
        self.client.terminate()

        time.sleep(.3)
        self.client = multiprocessing.Process(
            target=client.Client,
            args=(
                "localhost", 9001,
                self.new_file_client_queue,
                self.get_index_client_event,
                self.queue_client_index_data,
                # self.index_avail_client_event,
                # self.client_index_pipe_remote,
                self.file_name_request_client_queue,
                self.file_contents_name_hash_client_queue,
                self.shutdown_client_event,
            )
        )
        self.client.start()

        time.sleep(.3)
        self.shutdown_client_event.set()
        time.sleep(.1)
        self.client.terminate()

        try:
            time.sleep(1)
            self.shutdown_backend_manager_event.set()
            time.sleep(.1)
            self.server.terminate()
        except:
            pass


if __name__ == '__main__':
    unittest.main(verbosity=2)

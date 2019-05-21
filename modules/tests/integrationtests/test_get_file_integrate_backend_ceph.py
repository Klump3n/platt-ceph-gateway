#!/usr/bin/env python3
"""
Test integration of ceph interface and local_data_instance

"""
import unittest

try:
    from modules.local_data_manager import LocalDataManager
    from modules.backend_manager import BackendManager
    from modules.simulation_manager import SimulationManager
    from modules.ceph_manager import CephManager
except ImportError:
    import sys
    sys.path.append('../../..')
    from modules.local_data_manager import LocalDataManager
    from modules.backend_manager import BackendManager
    from modules.simulation_manager import SimulationManager
    from modules.ceph_manager import CephManager

from util.loggers import CoreLog as cl, BackendLog as bl, SimulationLog as sl

import modules.tests.unittests.backend_manager_client as client

import multiprocessing
import asyncio
import time
import os
import hashlib
import queue
import pathlib

class Test_Integrate_GET_FILE_Backend_Ceph(unittest.TestCase):
    def setUp(self):

        sl("debug")
        bl("debug")
        cl("debug")


        ceph_conf = pathlib.Path.home() / ".ccphi/simuser.ceph.conf"
        ceph_pool = "simdata"
        ceph_user = "simuser"

        host = "localhost"
        backend_port = 9009
        simulation_port = 9010


        # create all necessary queues, pipes and events for inter process
        # communication
        #
        # inter process communication for registering new files
        #
        # a queue for sending information about new files from the simulation to the
        # data copy process
        queue_sim_datacopy_new_file = multiprocessing.Queue()
        #
        # a queue for requesting the hash for a new file from the ceph cluster
        queue_datacopy_ceph_request_hash_for_new_file = multiprocessing.Queue()
        #
        # a queue for answering the request for a hash for a new file from the ceph
        # cluster. contains the name and the hash
        queue_datacopy_ceph_answer_hash_for_new_file = multiprocessing.Queue()
        #
        # a queue for sending the name and hash of a new file to the backend manager
        queue_datacopy_backend_new_file_and_hash = multiprocessing.Queue()


        # inter process communication for requesting files from the ceph cluster
        #
        # a queue for sending a request for a file to the ceph manager
        queue_backend_ceph_request_file = multiprocessing.Queue()
        #
        # a queue for answering the request for a file with the file name, contents
        # and hash
        queue_backend_ceph_answer_file_name_contents_hash = multiprocessing.Queue()


        # inter process communication for requesting the index for the backend
        # manager from the data copy
        #
        # an event for requesting the index for the backend from the data copy
        event_datacopy_backend_get_index = multiprocessing.Event()
        #
        # a queue for returning the requested index
        queue_datacopy_backend_index_data = multiprocessing.Queue()
        # #
        # # an event for telling the backend that the index from the data copy is
        # # ready for pickup
        # self.event_datacopy_backend_index_ready = multiprocessing.Event()
        # #
        # # a pipe that connects the datacopy mgr and the backend class, for
        # # transferring the requested index
        # (
        #     self.pipe_this_end_datacopy_backend_index,
        #     self.pipe_that_end_datacopy_backend_index
        # ) = multiprocessing.Pipe()


        # inter process communication for requesting the index for the data manager
        # from the ceph cluster
        #
        # an event for requesting the index for the data copy from the ceph cluster
        event_datacopy_ceph_update_index = multiprocessing.Event()
        #
        # a queue for updating the local datacopy with these names and hashes
        queue_datacopy_ceph_filename_and_hash = multiprocessing.Queue()


        # inter process communication for shutting down processes
        #
        # an event for shutting down the backend manager
        self.event_backend_manager_shutdown = multiprocessing.Event()
        #
        # an event for shutting down the ceph manager
        self.event_ceph_shutdown = multiprocessing.Event()


        localdata_manager = multiprocessing.Process(
            target=LocalDataManager,
            args=(
                queue_sim_datacopy_new_file,
                queue_datacopy_ceph_request_hash_for_new_file,
                queue_datacopy_ceph_answer_hash_for_new_file,
                queue_datacopy_backend_new_file_and_hash,
                event_datacopy_backend_get_index,
                queue_datacopy_backend_index_data,
                # event_datacopy_backend_index_ready,
                # pipe_this_end_datacopy_backend_index,
                event_datacopy_ceph_update_index,
                queue_datacopy_ceph_filename_and_hash
            )
        )
        simulation_manager = multiprocessing.Process(
            target=SimulationManager,
            args=(
                host,
                simulation_port,
                queue_sim_datacopy_new_file,
            )
        )
        self.backend_manager = multiprocessing.Process(
            target=BackendManager,
            args=(
                host,
                backend_port,
                queue_datacopy_backend_new_file_and_hash,
                event_datacopy_backend_get_index,
                queue_datacopy_backend_index_data,
                # event_datacopy_backend_index_ready,
                # pipe_that_end_datacopy_backend_index,
                queue_backend_ceph_request_file,
                queue_backend_ceph_answer_file_name_contents_hash,
                self.event_backend_manager_shutdown
            )
        )
        self.ceph_manager = multiprocessing.Process(
            target=CephManager,
            args=(
                ceph_conf,
                ceph_pool,
                ceph_user,
                self.event_ceph_shutdown,
                queue_datacopy_ceph_request_hash_for_new_file,
                queue_datacopy_ceph_answer_hash_for_new_file,
                queue_backend_ceph_request_file,
                queue_backend_ceph_answer_file_name_contents_hash,
                event_datacopy_ceph_update_index,
                queue_datacopy_ceph_filename_and_hash
            )
        )

        # queues for pushing information about new files over the socket
        # receive at client
        self.new_file_client_queue = multiprocessing.Queue()

        # server and client side of index exchange
        # index request events
        self.get_index_client_event = multiprocessing.Event()
        # index data queues
        self.client_index_data_queue = multiprocessing.Queue()
        # # index avail events
        # self.index_avail_client_event = multiprocessing.Event()
        # # index pipes
        # self.client_index_pipe = multiprocessing.Pipe()
        # (
        #     self.client_index_pipe_local,
        #     self.client_index_pipe_remote
        # ) = self.client_index_pipe

        # request file name at client
        self.file_name_request_client_queue = multiprocessing.Queue()
        # receive file contents, name and hash on client
        self.file_contents_name_hash_client_queue = multiprocessing.Queue()

        self.shutdown_client_event = multiprocessing.Event()

        self.client = multiprocessing.Process(
            target=client.Client,
            args=(
                host, backend_port,
                self.new_file_client_queue,
                self.get_index_client_event,
                self.client_index_data_queue,
                # self.index_avail_client_event,
                # self.client_index_pipe_remote,
                self.file_name_request_client_queue,
                self.file_contents_name_hash_client_queue,
                self.shutdown_client_event,
            )
        )

        try:
            self.backend_manager.start()
            time.sleep(.1)
            self.ceph_manager.start()
            time.sleep(.1)
            self.client.start()
            time.sleep(.1)
        except KeyboardInterrupt:
            self.event_backend_manager_shutdown.set()
            self.event_ceph_shutdown.set()
            self.shutdown_client_event.set()
            time.sleep(1)
            self.ceph_manager.terminate()
            self.client.terminate()
            self.backend_manager.terminate()


    def tearDown(self):
        time.sleep(2)

        self.event_backend_manager_shutdown.set()
        self.event_ceph_shutdown.set()
        self.shutdown_client_event.set()
        time.sleep(1)
        self.ceph_manager.terminate()
        self.client.terminate()
        self.backend_manager.terminate()

    def test_get_file(self):
        """get file from ceph and send it via socket

        """
        expected_list = []
        namespace = "numsim_napf_tiefziehversuch"
        filenames = [
            "universe.fo.nodal.PE2@000000000.035000"
        ]
        expected_namespace = namespace
        expected_object = filenames[0]
        expected_sha1sum = "a27d7607165e65dba5b02de677a9fd4b1bfb5411"

        for filename in filenames:
            new_file = {"namespace": namespace, "key": filename}
            expected_list.append({'todo': 'new_file', 'new_file': new_file})
            self.file_name_request_client_queue.put(new_file)
            time.sleep(.06)     # some time between new files
        time.sleep(.005)          # wait for queue

        for filename in filenames:
            res = self.file_contents_name_hash_client_queue.get(True, 1)
            ns = res["file_request"]["namespace"]
            o = res["file_request"]["object"]
            s = res["file_request"]["tags"]["sha1sum"]
            self.assertEqual(ns, expected_namespace)
            self.assertEqual(o, expected_object)
            self.assertEqual(s, expected_sha1sum)



if __name__ == '__main__':
    unittest.main(verbosity=2)


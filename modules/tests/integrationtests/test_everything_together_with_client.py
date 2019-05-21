#!/usr/bin/env python3
"""
Test the integration of all the processes together.

Client as only interface to the system.

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
import socket


def send_data(namespace, filename, arbitrary_hash):
    entry = "{}\t{}\t{}".format(namespace, filename, arbitrary_hash)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(("", 9010))
        s.send(entry.encode())


class Test_Integrate_EVERYTHING(unittest.TestCase):
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


        # queues for pushing information about new files over the socket
        # receive at client
        self.new_file_client_queue = multiprocessing.Queue()

        # server and client side of index exchange
        # index request events
        self.get_index_client_event = multiprocessing.Event()
        # index data queue (client)
        self.client_index_data_queue = multiprocessing.Queue()
        # # index avail events
        # self.index_avail_client_event = multiprocessing.Event()
        # # index pipes
        # self.client_index_pipe = multiprocessing.Pipe()
        # (
        #     self.client_index_pipe_local,
        #     self.client_index_pipe_remote
        # ) = self.client_index_pipe

        # queues for getting files from the ceph cluster
        # request file name at client
        self.file_name_request_client_queue = multiprocessing.Queue()
        # receive file contents, name and hash on client
        self.file_contents_name_hash_client_queue = multiprocessing.Queue()


        # inter process communication for shutting down processes
        #
        # an event for shutting down the backend manager
        self.event_backend_manager_shutdown = multiprocessing.Event()
        #
        # an event for shutting down the ceph manager
        self.event_ceph_shutdown = multiprocessing.Event()
        #
        # an event for shutting down the client
        self.shutdown_client_event = multiprocessing.Event()


        self.localdata_manager = multiprocessing.Process(
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
        self.simulation_manager = multiprocessing.Process(
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

        self.client = multiprocessing.Process(
            target=client.Client,
            args=(
                "localhost", backend_port,
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

        print()
        try:
            self.ceph_manager.start()
            time.sleep(.1)
            self.localdata_manager.start()
            time.sleep(.1)
            self.simulation_manager.start()
            time.sleep(.1)
            self.backend_manager.start()
            time.sleep(.1)
            self.client.start()
            time.sleep(.1)

        except KeyboardInterrupt:
            self.event_backend_manager_shutdown.set()
            self.event_ceph_shutdown.set()
            self.shutdown_client_event.set()

            time.sleep(1)

            self.ceph_manager.terminate()
            time.sleep(.1)
            self.localdata_manager.terminate()
            time.sleep(.1)
            self.simulation_manager.terminate()
            time.sleep(.1)
            self.backend_manager.terminate()
            time.sleep(.1)
            self.client.terminate()
            time.sleep(.1)


    def tearDown(self):
        time.sleep(2)

        self.event_backend_manager_shutdown.set()
        self.event_ceph_shutdown.set()
        self.shutdown_client_event.set()

        time.sleep(1)

        self.ceph_manager.terminate()
        time.sleep(.1)
        self.localdata_manager.terminate()
        time.sleep(.1)
        self.simulation_manager.terminate()
        time.sleep(.1)
        self.backend_manager.terminate()
        time.sleep(.1)
        self.client.terminate()
        time.sleep(.1)

    def test_client_tell_file(self):
        """inform the client about a new file

        """
        time.sleep(50)          # wait for the index to start up. might have to adjust this if the cluster grows... better mock that shit T_T

        pkg = "eo_mesh\tuniverse.fo.nodal.z1.eo@000000000.000000\t"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(("", 9010))
            s.send(pkg.encode())
            time.sleep(.1)

        res = self.new_file_client_queue.get(True, 1)
        exp_res = {
            "todo": "new_file",
            "new_file": {'sha1sum': '1cf0ae170f866d5caf5b0695f2f10a9d0972b8a6', 'namespace': 'eo_mesh', 'key': 'universe.fo.nodal.z1.eo@000000000.000000'}
        }
        self.assertEqual(res, exp_res)

        # make this run in parallel too

    def test_client_get_index(self):
        """make the client get the index

        """
        time.sleep(50)          # wait for the index to start up. might have to adjust this if the cluster grows... better mock that shit T_T

        self.get_index_client_event.set()

        try:
            index = self.client_index_data_queue.get(True, 10)
        except queue.Empty:
            index = None

        self.assertIsNotNone(index)
        print(index.keys())

if __name__ == '__main__':
    unittest.main(verbosity=2)


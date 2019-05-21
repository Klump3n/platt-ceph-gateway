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

class Test_Integrate_GET_INDEX_Backend_DataCopy(unittest.TestCase):
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
        self.queue_datacopy_ceph_filename_and_hash = multiprocessing.Queue()


        # inter process communication for shutting down processes
        #
        # an event for shutting down the backend manager
        self.event_backend_manager_shutdown = multiprocessing.Event()
        #
        # an event for shutting down the ceph manager
        self.event_ceph_shutdown = multiprocessing.Event()


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
                self.queue_datacopy_ceph_filename_and_hash
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
        ceph_manager = multiprocessing.Process(
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
                self.queue_datacopy_ceph_filename_and_hash
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
            self.localdata_manager.start()
            time.sleep(.1)
            self.client.start()
            time.sleep(.1)
        except KeyboardInterrupt:
            self.event_backend_manager_shutdown.set()
            self.event_ceph_shutdown.set()
            self.shutdown_client_event.set()
            time.sleep(1)
            self.localdata_manager.terminate()
            self.client.terminate()
            self.backend_manager.terminate()


    def tearDown(self):
        time.sleep(2)

        self.event_backend_manager_shutdown.set()
        self.event_ceph_shutdown.set()
        self.shutdown_client_event.set()
        time.sleep(1)
        self.localdata_manager.terminate()
        self.client.terminate()
        self.backend_manager.terminate()

    def test_get_index(self):
        """get index from datacopy and send it via socket

        """
        indexfiles = [
            {'sha1sum': '', 'key': 'universe.fo.elemental.c3d6.hard0@000000415.050000', 'namespace': 'ccphiam.s355.singlepass.coarse'},
            {'sha1sum': '', 'key': 'universe.fo.elemental.c3d8.grainsize@000000240.050000', 'namespace': 'ccphiam.s355.singlepass.coarse'},
            {'sha1sum': '', 'key': 'universe.fo.elemental.c3d6.kappa@000000380.000000', 'namespace': 'ccphiam.s355.singlepass.coarse'},
            {'sha1sum': '', 'key': 'universe.fo.elemental.c3d6.z3@000000125.000000', 'namespace': 'ccphiam.s355.singlepass.coarse'},
            {'sha1sum': '', 'key': 'universe.fo.elemental.c3d6.hard2@000000230.000000', 'namespace': 'ccphiam.s355.singlepass.coarse'},
            {'sha1sum': '', 'key': 'universe.fo.nodal.T@000000105.050000', 'namespace': 'ccphiam.s355.singlepass.coarse'}
        ]

        for i in indexfiles:
            ns = i["namespace"]
            key = i["key"]
            sha1sum = i["sha1sum"]
            self.queue_datacopy_ceph_filename_and_hash.put({"namespace": ns, "key": key, "sha1sum": sha1sum})

        expected_index = {'todo': 'index', 'index': {'ccphiam.s355.singlepass.coarse': {'000000240.050000': {'elemental': {'grainsize': {'c3d8': {'object_key': 'universe.fo.elemental.c3d8.grainsize@000000240.050000', 'sha1sum': ''}}}}, '000000125.000000': {'elemental': {'z3': {'c3d6': {'object_key': 'universe.fo.elemental.c3d6.z3@000000125.000000', 'sha1sum': ''}}}}, '000000230.000000': {'elemental': {'hard2': {'c3d6': {'object_key': 'universe.fo.elemental.c3d6.hard2@000000230.000000', 'sha1sum': ''}}}}, '000000415.050000': {'elemental': {'hard0': {'c3d6': {'object_key': 'universe.fo.elemental.c3d6.hard0@000000415.050000', 'sha1sum': ''}}}}, '000000380.000000': {'elemental': {'kappa': {'c3d6': {'object_key': 'universe.fo.elemental.c3d6.kappa@000000380.000000', 'sha1sum': ''}}}}, '000000105.050000': {'nodal': {'T': {'object_key': 'universe.fo.nodal.T@000000105.050000', 'sha1sum': ''}}}}}}

        self.get_index_client_event.set()

        index = self.client_index_data_queue.get(True, 10)

        self.assertEqual(index, expected_index)


if __name__ == '__main__':
    unittest.main(verbosity=2)


#!/usr/bin/env python3
"""
Test the simulation backend.

That is the part that gets the updates about new files that are checked into
ceph.

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

import pathlib
import multiprocessing
import asyncio
import socket
import time
import struct
from itertools import repeat

def send_data(namespace, filename, arbitrary_hash):
    entry = "{}\t{}\t{}".format(namespace, filename, arbitrary_hash)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(("", 9010))
        s.send(entry.encode())


class Test_Integrate_TELL_NEW_FILE_Simulation_DataCopy(unittest.TestCase):

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
        self.queue_datacopy_ceph_request_hash_for_new_file = multiprocessing.Queue()
        #
        # a queue for answering the request for a hash for a new file from the ceph
        # cluster. contains the name and the hash
        self.queue_datacopy_ceph_answer_hash_for_new_file = multiprocessing.Queue()
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


        self.localdata_manager = multiprocessing.Process(
            target=LocalDataManager,
            args=(
                queue_sim_datacopy_new_file,
                self.queue_datacopy_ceph_request_hash_for_new_file,
                self.queue_datacopy_ceph_answer_hash_for_new_file,
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
        backend_manager = multiprocessing.Process(
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
                self.queue_datacopy_ceph_request_hash_for_new_file,
                self.queue_datacopy_ceph_answer_hash_for_new_file,
                queue_backend_ceph_request_file,
                queue_backend_ceph_answer_file_name_contents_hash,
                event_datacopy_ceph_update_index,
                queue_datacopy_ceph_filename_and_hash
            )
        )

        print()
        try:
            self.simulation_manager.start()
            time.sleep(.1)
            self.localdata_manager.start()
            time.sleep(.1)
            # self.client.start()
            # time.sleep(.1)
        except KeyboardInterrupt:
            # self.event_backend_manager_shutdown.set()
            # self.event_ceph_shutdown.set()
            # self.shutdown_client_event.set()
            time.sleep(1)
            self.localdata_manager.terminate()
            # self.client.terminate()
            self.simulation_manager.terminate()


    def tearDown(self):

        time.sleep(2)
        # self.event_backend_manager_shutdown.set()
        # self.event_ceph_shutdown.set()
        # self.shutdown_client_event.set()
        time.sleep(1)
        self.localdata_manager.terminate()
        # self.client.terminate()
        self.simulation_manager.terminate()

    def test_drop_one_file(self):
        """registers a file in the local data copy after info came via socket

        """
        pkg = "some_namespace\tuniverse.fo.nodes@0000000001.000000\t"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(("", 9010))
            s.send(pkg.encode())
            time.sleep(.1)

        res = self.queue_datacopy_ceph_request_hash_for_new_file.get()
        self.assertEqual(res, {'sha1sum': '', 'namespace': 'some_namespace', 'key': 'universe.fo.nodes@0000000001.000000'})

    def test_drop_many_files_serial(self):
        """registers many files in the local data copy after info came via socket (serial)

        """
        namespace = "some_namespace"
        nodes = "universe.fo.nodes@0000000001.000000"
        elements_c3d6 = "universe.fo.elements.c3d6@0000000001.000000"
        elements_c3d8 = "universe.fo.elements.c3d8@0000000001.000000"
        nodal_field = "universe.fo.nodal.fieldname@0000000001.000000"
        elemental_field_c3d6 = "universe.fo.elemental.c3d6.fieldname@0000000001.000000"
        elemental_field_c3d8 = "universe.fo.elemental.c3d8.fieldname@0000000001.000000"
        surface_skin_c3d6 = "universe.fo.skin.c3d6@0000000001.000000"
        surface_skin_c3d8 = "universe.fo.skin.c3d8@0000000001.000000"
        elset_c3d6 = "universe.fo.elset.c3d6@0000000001.000000"
        elset_c3d8 = "universe.fo.elset.c3d8@0000000001.000000"
        arbitraty_hash = ""

        res_list = list()

        for filename in [
                nodes,
                elements_c3d6, elements_c3d8,
                nodal_field,
                    elemental_field_c3d6, elemental_field_c3d8,
                surface_skin_c3d6, surface_skin_c3d8,
                elset_c3d6, elset_c3d8
        ]:
            entry = "{}\t{}\t{}".format(namespace, filename, arbitraty_hash)
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(("", 9010))
                s.send(entry.encode())
            res_list.append({'sha1sum': '', 'namespace': namespace, 'key': filename})
        time.sleep(.01)

        while (len(res_list) > 0):
            res = self.queue_datacopy_ceph_request_hash_for_new_file.get()
            self.assertIn(res, res_list)
            res_list.remove(res)


    def test_drop_many_files_parallel(self):
        """registers many files in the local data copy after info came via socket (parallel)

        """
        namespace = "some_namespace"
        nodes = "universe.fo.nodes@0000000001.000000"
        elements_c3d6 = "universe.fo.elements.c3d6@0000000001.000000"
        elements_c3d8 = "universe.fo.elements.c3d8@0000000001.000000"
        nodal_field = "universe.fo.nodal.fieldname@0000000001.000000"
        elemental_field_c3d6 = "universe.fo.elemental.c3d6.fieldname@0000000001.000000"
        elemental_field_c3d8 = "universe.fo.elemental.c3d8.fieldname@0000000001.000000"
        surface_skin_c3d6 = "universe.fo.skin.c3d6@0000000001.000000"
        surface_skin_c3d8 = "universe.fo.skin.c3d8@0000000001.000000"
        elset_c3d6 = "universe.fo.elset.c3d6@0000000001.000000"
        elset_c3d8 = "universe.fo.elset.c3d8@0000000001.000000"
        arbitraty_hash = ""

        res_list = list()

        filenames = [
            nodes,
            elements_c3d6, elements_c3d8,
            nodal_field,
            elemental_field_c3d6, elemental_field_c3d8,
            surface_skin_c3d6, surface_skin_c3d8,
            elset_c3d6, elset_c3d8
        ]

        with multiprocessing.Pool(10) as mpool:
            mpool.starmap(send_data, zip(repeat(namespace), filenames, repeat(arbitraty_hash)))

        for filename in filenames:
            res_list.append({'sha1sum': '', 'namespace': namespace, 'key': filename})

        time.sleep(.01)

        while (len(res_list) > 0):
            res = self.queue_datacopy_ceph_request_hash_for_new_file.get()
            self.assertIn(res, res_list)
            res_list.remove(res)

if __name__ == '__main__':
    unittest.main(verbosity=2)

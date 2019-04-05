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

class Test_BackendManager(unittest.TestCase):

    def setUp(self):

        cl("debug")
        bl("debug")
        sl("debug")

        # queues for pushing information about new files over the socket
        # send from server
        self.new_file_server_queue = multiprocessing.Queue()
        # receive at client
        self.new_file_client_queue = multiprocessing.Queue()

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
        self.server = multiprocessing.Process(
            target=backend_manager.BackendManager,
            args=(
                "localhost", 9001,
                self.new_file_server_queue,
                self.get_index_server_event,
                self.index_avail_server_event,
                self.server_index_pipe_remote,
                self.file_name_request_server_queue,
                self.file_contents_name_hash_server_queue,
                self.shutdown_backend_manager_event,
            )
        )
        self.server.start()
        time.sleep(.1)
        self.client = multiprocessing.Process(
            target=client.Client,
            args=(
                "localhost", 9001,
                self.new_file_client_queue,
                self.get_index_client_event,
                self.index_avail_client_event,
                self.client_index_pipe_remote,
                self.file_name_request_client_queue,
                self.file_contents_name_hash_client_queue,
                self.shutdown_client_event,
            )
        )
        self.client.start()
        time.sleep(.1)


    def tearDown(self):
        time.sleep(2)
        self.shutdown_backend_manager_event.set()
        self.shutdown_client_event.set()
        time.sleep(1)
        try:
            self.client.terminate()
        except:
            pass
        try:
            self.server.terminate()
        except:
            pass

    def test_send_new_files(self):
        """init the server

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

        expected_list = []

        for filename in [
                nodes,
                elements_c3d6, elements_c3d8,
                nodal_field,
                elemental_field_c3d6, elemental_field_c3d8,
                surface_skin_c3d6, surface_skin_c3d8,
                elset_c3d6, elset_c3d8
        ]:
            new_file_name = "{}\t{}".format(namespace, filename)
            expected_list.append({'todo': 'new_file', 'new_file': new_file_name})
            self.new_file_server_queue.put(new_file_name)
            time.sleep(.06)     # some time between new files
        time.sleep(.005)          # wait for queue

        for filename in [
                nodes,
                elements_c3d6, elements_c3d8,
                nodal_field,
                elemental_field_c3d6, elemental_field_c3d8,
                surface_skin_c3d6, surface_skin_c3d8,
                elset_c3d6, elset_c3d8
        ]:
            res = self.new_file_client_queue.get()
            self.assertIn(res, expected_list)
            expected_list.remove(res)


        # files as a burst
        expected_list = []

        for filename in [
                nodes,
                elements_c3d6, elements_c3d8,
                nodal_field,
                elemental_field_c3d6, elemental_field_c3d8,
                surface_skin_c3d6, surface_skin_c3d8,
                elset_c3d6, elset_c3d8
        ]:
            new_file_name = "{}\t{}".format(namespace, filename)
            expected_list.append({'todo': 'new_file', 'new_file': new_file_name})
            self.new_file_server_queue.put(new_file_name)
        time.sleep(.005)          # wait for queue

        for filename in [
                nodes,
                elements_c3d6, elements_c3d8,
                nodal_field,
                elemental_field_c3d6, elemental_field_c3d8,
                surface_skin_c3d6, surface_skin_c3d8,
                elset_c3d6, elset_c3d8
        ]:
            res = self.new_file_client_queue.get()
            self.assertIn(res, expected_list)
            expected_list.remove(res)


    def test_request_index(self):
        """Request the index from the server.

        """
        mock_index = {
            "this": "is",
            "a": "mock",
            "index": True
        }
        expected_index = {'todo': 'index', 'index': mock_index}

        # tell the client that it should tell the server to get the index
        self.get_index_client_event.set()

        # wait for the server to have received a request for the index
        if self.get_index_server_event.wait(1):
            # give the server a mock index to send out
            self.server_index_pipe_local.send(mock_index)
            # tell the server to send the index
            self.index_avail_server_event.set()

        # wait until the index is ready on the client side
        if self.index_avail_client_event.wait(1):
            # clear the event
            self.index_avail_client_event.clear()
            # get the index from the pipe
            index = self.client_index_pipe_local.recv()

        # assert
        self.assertEqual(index, expected_index)

    def test_request_file_data(self):
        """Request the index from the server.

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


        # see if file requests show up on the server
        expected_list = []

        for filename in [
                nodes,
                elements_c3d6, elements_c3d8,
                nodal_field,
                elemental_field_c3d6, elemental_field_c3d8,
                surface_skin_c3d6, surface_skin_c3d8,
                elset_c3d6, elset_c3d8
        ]:
            file_name = "{}\t{}".format(namespace, filename)
            expected_list.append(file_name)
            self.file_name_request_client_queue.put(file_name)
            time.sleep(.06)     # some time between new files
        time.sleep(.005)          # wait for queue

        for filename in [
                nodes,
                elements_c3d6, elements_c3d8,
                nodal_field,
                elemental_field_c3d6, elemental_field_c3d8,
                surface_skin_c3d6, surface_skin_c3d8,
                elset_c3d6, elset_c3d8
        ]:
            res = self.file_name_request_server_queue.get(True, .1)
            self.assertIn(res, expected_list)
            expected_list.remove(res)

        # answer file requests with file data
        file_size = 1024        # 1kB
        expected_list = []
        transfer_this = {}

        for filename in [
                nodes,
                elements_c3d6, elements_c3d8,
                nodal_field,
                elemental_field_c3d6, elemental_field_c3d8,
                surface_skin_c3d6, surface_skin_c3d8,
                elset_c3d6, elset_c3d8
        ]:
            file_contents = os.urandom(file_size)
            file_name = "{}\t{}".format(namespace, filename)
            expected_list.append(file_name)
            file_hash = hashlib.sha1(file_contents).hexdigest()
            transfer_this[file_name] = {
                "name": file_name,
                "hash": file_hash,
                "contents": file_contents
            }
            # drop files in the queue
            self.file_name_request_client_queue.put(file_name)
            time.sleep(.06)     # some time between new files
        time.sleep(.005)          # wait for queue

        for filename in [
                nodes,
                elements_c3d6, elements_c3d8,
                nodal_field,
                elemental_field_c3d6, elemental_field_c3d8,
                surface_skin_c3d6, surface_skin_c3d8,
                elset_c3d6, elset_c3d8
        ]:
            res = self.file_name_request_server_queue.get(True, .1)
            self.assertIn(res, expected_list)
            transfer_me = transfer_this[res]
            self.file_contents_name_hash_server_queue.put(transfer_me)
        time.sleep(.05)          # wait for queue

        for filename in [
                nodes,
                elements_c3d6, elements_c3d8,
                nodal_field,
                elemental_field_c3d6, elemental_field_c3d8,
                surface_skin_c3d6, surface_skin_c3d8,
                elset_c3d6, elset_c3d8
        ]:
            res = self.file_contents_name_hash_client_queue.get(True, .1)
            self.assertIn(res["file_request"], list(transfer_this.values()))


if __name__ == '__main__':
    unittest.main(verbosity=2)

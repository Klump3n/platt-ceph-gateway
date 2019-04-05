#!/usr/bin/env python3
"""
Test the backend manager

"""
import time
import json
import struct
import socket
import unittest
import multiprocessing

try:
    from modules.backend_manager import BackendManager
except ImportError:
    import sys
    sys.path.append('../../..')
    from modules.backend_manager import BackendManager

from util.loggers import CoreLog as cl, BackendLog as bl, SimulationLog as sl


class NewFileRevcSocket(object):
    """
    Implements the other side of the backend.

    Receives messages about new files (to then be added to the other index).
    Requests the index and receives the requested index.
    Requests files and receives the requested files.

    """
    def __init__(self, host="", port=8009):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((host, port))

    def __del__(self):
        self.sock.close()

    def _rd_len(self):
        """
        Receives length of package.

        """
        pass

    def _rd_data(self, length):
        """
        Receives data of length.

        """
        pass

    def _snd_len(self, length):
        """
        Sends length of package.

        """
        pass

    def _snd_data(self, length):
        """
        Sends actual package.

        """
        pass

    def _snd_ack(self):
        """
        Sends an ACK.

        """
        pass

    def _snd_nack(self):
        """
        Sends a NACK.

        """
        pass

    def _decode(self, data):
        """
        Decodes data from binary to string.

        """
        pass

    def _encode(self, string):
        """
        Decodes data from string to binary.

        """
        pass



    def read(self, length):
        return self.sock.recv(length).decode()

    def read_len(self):
        return self.sock.recv(8)[0]

    def send(self, msg):
        return self.sock.send(msg.encode())

class SocketRead(object):
    def __init__(self, host="", port=8009):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((host, port))

    def __del__(self):
        self.sock.close()

    def read(self, length):
        return self.sock.recv(length).decode()

    def read_len(self):
        return self.sock.recv(8)[0]

    def send(self, msg):
        return self.sock.send(msg.encode())

class Test_BackendManager(unittest.TestCase):
    def setUp(self):

        bl("debug")

        # adding a file to the local data copy
        self.localdata_add_file_queue = multiprocessing.Queue()

        # # see if if file is in the local data copy
        # self.localdata_check_file_pipe = multiprocessing.Pipe()
        # (
        #     self.localdata_check_file_pipe_local,
        #     self.localdata_check_file_pipe_remote
        # ) = self.localdata_check_file_pipe
        # self.localdata_check_file_event = multiprocessing.Event()

        # # get the local data copy for a timestep
        # self.localdata_get_index_event = multiprocessing.Event()
        # self.localdata_index_avail_event = multiprocessing.Event()
        # self.localdata_get_index_pipe = multiprocessing.Pipe()
        # (
        #     self.localdata_get_index_pipe_local,
        #     self.localdata_get_index_pipe_remote
        # ) = self.localdata_get_index_pipe

        self.backend_manager = multiprocessing.Process(
            target=BackendManager,
            args=(
                "", 8009,
                self.localdata_add_file_queue,
                # self.localdata_check_file_event,
                # self.localdata_check_file_pipe_remote,
                # self.localdata_get_index_event,
                # self.localdata_index_avail_event,
                # self.localdata_get_index_pipe_remote
            )
        )
        self.backend_manager.start()
        time.sleep(.1)

    def tearDown(self):
        try:
            self.backend_manager.terminate()
        except:
            pass

    def test_push_new_file(self):
        """send out a new file via a socket

        """
        s = SocketRead()
        new_file = {"namespace": "some_namespace", "key": "universe.fo.nodes@0000000001.000000", "sha1sum": ""}
        self.localdata_add_file_queue.put(new_file)
        time.sleep(.01)
        len_b = s.read_len()
        s.send("ACK")
        res_b = s.read(len_b)
        res = json.loads(res_b)
        self.assertEqual(res, new_file)
        s.send("ACK")

    def test_push_many_new_files(self):
        """send out a new file via a socket

        """
        expected_vals_send = [
            {'namespace': 'some_namespace', 'key': 'universe.fo.nodes@0000000001.000000', 'sha1sum': ''},
            {'namespace': 'some_namespace', 'key': "universe.fo.elements.c3d6@0000000001.000000", 'sha1sum': ''},
            {'namespace': 'some_namespace', 'key': "universe.fo.elements.c3d8@0000000001.000000", 'sha1sum': ''},
            {'namespace': 'some_namespace', 'key': "universe.fo.nodal.fieldname@0000000001.000000", 'sha1sum': ''},
            {'namespace': 'some_namespace', 'key': "universe.fo.elemental.c3d6.fieldname@0000000001.000000", 'sha1sum': ''},
            {'namespace': 'some_namespace', 'key': "universe.fo.elemental.c3d8.fieldname@0000000001.000000", 'sha1sum': ''},
            {'namespace': 'some_namespace', 'key': "universe.fo.skin.c3d6@0000000001.000000", 'sha1sum': ''},
            {'namespace': 'some_namespace', 'key': "universe.fo.skin.c3d8@0000000001.000000", 'sha1sum': ''},
            {'namespace': 'some_namespace', 'key': "universe.fo.elset.c3d6@0000000001.000000", 'sha1sum': ''},
            {'namespace': 'some_namespace', 'key': "universe.fo.elset.c3d8@0000000001.000000", 'sha1sum': ''}
        ]
        expected_vals_recv = expected_vals_send.copy()

        for new_file in expected_vals_send:
            self.localdata_add_file_queue.put(new_file)

        s = SocketRead()
        time.sleep(.01)

        # check that elements are contained in the list
        while not expected_vals_recv == []:

            len_b = s.read_len()
            s.send("ACK")
            res_b = s.read(len_b)
            value = json.loads(res_b)
            self.assertIn(value, expected_vals_recv)
            expected_vals_recv.remove(value)
            s.send("ACK")

if __name__ == '__main__':
    unittest.main(verbosity=2)

#!/usr/bin/env python3
"""
Test the simulation backend.

That is the part that gets the updates about new files that are checked into
ceph.

"""
import unittest
try:
    from modules.simulation_manager import SimulationManager
except ImportError:
    import sys
    sys.path.append('../../..')
    from modules.simulation_manager import SimulationManager

from util.loggers import CoreLog as cl, BackendLog as bl, SimulationLog as sl

import queue
import multiprocessing
import asyncio
import socket
import time
import struct
from itertools import repeat

def send_data(namespace, filename, arbitrary_hash):
    entry = "{}\t{}\t{}".format(namespace, filename, arbitrary_hash)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(("", 8010))
        s.send(entry.encode())


class Test_SimulationManager(unittest.TestCase):

    def setUp(self):

        sl("debug")
        cl("debug")

        host = ""
        port = 8010

        # adding a file to the local data copy
        self.localdata_add_file_queue = multiprocessing.Queue()

        self.simulation_manager = multiprocessing.Process(
            target=SimulationManager,
            args=(
                host,
                port,
                self.localdata_add_file_queue
            )
        )

        self.simulation_manager.start()
        time.sleep(.1)

    def tearDown(self):
        self.simulation_manager.terminate()

    def test_drop_one_file(self):
        """send one file into the simulation and receive it in both queues

        """
        pkg = "some_namespace\tuniverse.fo.nodes@0000000001.000000\t"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(("", 8010))
            s.send(pkg.encode())
            time.sleep(.01)
        datacopy_queue_val = self.localdata_add_file_queue.get(.1)
        self.assertEqual(
            datacopy_queue_val,
            {'namespace': 'some_namespace', 'key': 'universe.fo.nodes@0000000001.000000', 'sha1sum': ''}
        )

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
                s.connect(("", 8010))
                s.send(entry.encode())
        time.sleep(.01)

        expected_vals_datacopy = [
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
        expected_vals_backend = expected_vals_datacopy.copy()

        # check that elements are contained in the list
        while not expected_vals_datacopy == []:
            datacopy_val = self.localdata_add_file_queue.get(.1)
            self.assertIn(datacopy_val, expected_vals_datacopy)
            expected_vals_datacopy.remove(datacopy_val)

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

        time.sleep(.01)

        expected_vals_datacopy = [
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
        expected_vals_backend = expected_vals_datacopy.copy()

        # check that elements are contained in the list
        while not expected_vals_datacopy == []:
            datacopy_val = self.localdata_add_file_queue.get(.1)
            self.assertIn(datacopy_val, expected_vals_datacopy)
            expected_vals_datacopy.remove(datacopy_val)

if __name__ == '__main__':
    unittest.main(verbosity=2)

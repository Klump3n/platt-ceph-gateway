#!/usr/bin/env python3
"""
Test the receiver routine.

"""
import unittest
try:
    from modules.simulation_manager import SimulationManager
except ImportError:
    import sys
    sys.path.append('../..')
    from modules.simulation_manager import SimulationManager

from modules.local_data_manager import LocalDataManager

from util.loggers import CoreLog as cl, BackendLog as bl, SimulationLog as sl

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

        # see if if file is in the local data copy
        self.localdata_check_file_pipe = multiprocessing.Pipe()
        (
            self.localdata_check_file_pipe_local,
            self.localdata_check_file_pipe_remote
        ) = self.localdata_check_file_pipe
        self.localdata_check_file_event = multiprocessing.Event()

        # get the local data copy for a timestep
        self.localdata_get_index_event = multiprocessing.Event()
        self.localdata_index_avail_event = multiprocessing.Event()
        self.localdata_get_index_pipe = multiprocessing.Pipe()
        (
            self.localdata_get_index_pipe_local,
            self.localdata_get_index_pipe_remote
        ) = self.localdata_get_index_pipe

        self.localdata_manager = multiprocessing.Process(
            target=LocalDataManager,
            args=(
                self.localdata_add_file_queue,
                self.localdata_check_file_event,
                self.localdata_check_file_pipe_remote,
                self.localdata_get_index_event,
                self.localdata_index_avail_event,
                self.localdata_get_index_pipe_remote
            )
        )

        self.simulation_manager = multiprocessing.Process(
            target=SimulationManager,
            args=(
                host,
                port,
                self.localdata_add_file_queue,
            )
        )

        self.localdata_manager.start()
        self.simulation_manager.start()
        time.sleep(.1)

    def tearDown(self):

        self.localdata_manager.terminate()
        self.simulation_manager.terminate()

    def test_drop_one_file(self):
        """registers a file in the local data copy after info came via socket

        """
        pkg = "some_namespace\tuniverse.fo.nodes@0000000001.000000\t"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(("", 8010))
            s.send(pkg.encode())
            time.sleep(.1)

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

        expected_res = {
            namespace: {
                '0000000001.000000': {
                    'nodes': {
                        'object_key': 'universe.fo.nodes@0000000001.000000', 'sha1sum': ''
                    },
                    'elements': {
                        'c3d6': {
                            'object_key': 'universe.fo.elements.c3d6@0000000001.000000', 'sha1sum': ''
                        },
                        'c3d8': {
                            'object_key': 'universe.fo.elements.c3d8@0000000001.000000', 'sha1sum': ''
                        }
                    },
                    'nodal': {
                        'fieldname': {
                            'object_key': 'universe.fo.nodal.fieldname@0000000001.000000', 'sha1sum': ''
                        }
                    },
                    'elemental': {
                        'fieldname': {
                            'c3d6': {
                                'object_key': 'universe.fo.elemental.c3d6.fieldname@0000000001.000000', 'sha1sum': ''
                            },
                            'c3d8': {
                                'object_key': 'universe.fo.elemental.c3d8.fieldname@0000000001.000000', 'sha1sum': ''
                            }
                        }
                    },
                    'skin': {
                        'c3d6': {
                            'object_key': 'universe.fo.skin.c3d6@0000000001.000000', 'sha1sum': ''
                        },
                        'c3d8': {
                            'object_key': 'universe.fo.skin.c3d8@0000000001.000000', 'sha1sum': ''
                        }
                    },
                    'elset': {
                        '': {
                            'c3d6': {
                                'object_key': 'universe.fo.elset.c3d6@0000000001.000000', 'sha1sum': ''
                            },
                            'c3d8': {
                                'object_key': 'universe.fo.elset.c3d8@0000000001.000000', 'sha1sum': ''
                            }
                        }
                    }
                }
            }
        }
        expected_ns_res = expected_res[namespace]

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

        for filename in [
                nodes,
                elements_c3d6, elements_c3d8,
                nodal_field,
                elemental_field_c3d6, elemental_field_c3d8,
                surface_skin_c3d6, surface_skin_c3d8,
                elset_c3d6, elset_c3d8
        ]:
            entry = {"namespace": namespace, "key": filename, "sha1sum": arbitraty_hash}

            self.localdata_check_file_event.clear()
            self.localdata_check_file_pipe_local.send(entry)
            while not self.localdata_check_file_event.wait(1):
                pass
            else:
                if self.localdata_check_file_pipe_local.poll():
                    self.assertTrue(self.localdata_check_file_pipe_local.recv())

        self.localdata_get_index_pipe_local.send(None)
        self.localdata_get_index_event.set()
        while not self.localdata_index_avail_event.wait(1):
            pass
        else:
            if self.localdata_get_index_pipe_local.poll():
                res = self.localdata_get_index_pipe_local.recv()
                self.localdata_index_avail_event.clear()
                self.assertEqual(res, expected_res)

        self.localdata_get_index_pipe_local.send(namespace)
        self.localdata_get_index_event.set()
        while not self.localdata_index_avail_event.wait(1):
            pass
        else:
            if self.localdata_get_index_pipe_local.poll():
                res = self.localdata_get_index_pipe_local.recv()
                self.localdata_index_avail_event.clear()
                self.assertEqual(res, expected_ns_res)

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

        expected_res = {
            namespace: {
                '0000000001.000000': {
                    'nodes': {
                        'object_key': 'universe.fo.nodes@0000000001.000000', 'sha1sum': ''
                    },
                    'elements': {
                        'c3d6': {
                            'object_key': 'universe.fo.elements.c3d6@0000000001.000000', 'sha1sum': ''
                        },
                        'c3d8': {
                            'object_key': 'universe.fo.elements.c3d8@0000000001.000000', 'sha1sum': ''
                        }
                    },
                    'nodal': {
                        'fieldname': {
                            'object_key': 'universe.fo.nodal.fieldname@0000000001.000000', 'sha1sum': ''
                        }
                    },
                    'elemental': {
                        'fieldname': {
                            'c3d6': {
                                'object_key': 'universe.fo.elemental.c3d6.fieldname@0000000001.000000', 'sha1sum': ''
                            },
                            'c3d8': {
                                'object_key': 'universe.fo.elemental.c3d8.fieldname@0000000001.000000', 'sha1sum': ''
                            }
                        }
                    },
                    'skin': {
                        'c3d6': {
                            'object_key': 'universe.fo.skin.c3d6@0000000001.000000', 'sha1sum': ''
                        },
                        'c3d8': {
                            'object_key': 'universe.fo.skin.c3d8@0000000001.000000', 'sha1sum': ''
                        }
                    },
                    'elset': {
                        '': {
                            'c3d6': {
                                'object_key': 'universe.fo.elset.c3d6@0000000001.000000', 'sha1sum': ''
                            },
                            'c3d8': {
                                'object_key': 'universe.fo.elset.c3d8@0000000001.000000', 'sha1sum': ''
                            }
                        }
                    }
                }
            }
        }
        expected_ns_res = expected_res[namespace]

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

        for filename in [
                nodes,
                elements_c3d6, elements_c3d8,
                nodal_field,
                elemental_field_c3d6, elemental_field_c3d8,
                surface_skin_c3d6, surface_skin_c3d8,
                elset_c3d6, elset_c3d8
        ]:
            entry = {"namespace": namespace, "key": filename, "sha1sum": arbitraty_hash}

            self.localdata_check_file_event.clear()
            self.localdata_check_file_pipe_local.send(entry)
            while not self.localdata_check_file_event.wait(1):
                pass
            else:
                if self.localdata_check_file_pipe_local.poll():
                    self.assertTrue(self.localdata_check_file_pipe_local.recv())

        self.localdata_get_index_pipe_local.send(None)
        self.localdata_get_index_event.set()
        while not self.localdata_index_avail_event.wait(1):
            pass
        else:
            if self.localdata_get_index_pipe_local.poll():
                res = self.localdata_get_index_pipe_local.recv()
                self.localdata_index_avail_event.clear()
                self.assertEqual(res, expected_res)

        self.localdata_get_index_pipe_local.send(namespace)
        self.localdata_get_index_event.set()
        while not self.localdata_index_avail_event.wait(1):
            pass
        else:
            if self.localdata_get_index_pipe_local.poll():
                res = self.localdata_get_index_pipe_local.recv()
                self.localdata_index_avail_event.clear()
                self.assertEqual(res, expected_ns_res)


if __name__ == '__main__':
    unittest.main(verbosity=2)

#!/usr/bin/env python3
"""
Test local_data_instance

"""
import time
import unittest
import multiprocessing

try:
    # from modules.local_data_instance import DataCopy
    from modules.local_data_manager import LocalDataManager
except ImportError:
    import sys
    sys.path.append('../../..')
    # from modules.local_data_instance import DataCopy
    from modules.local_data_manager import LocalDataManager

from util.loggers import CoreLog as cl, BackendLog as bl, SimulationLog as sl

class Test_Local_Data_Instance(unittest.TestCase):
    def setUp(self):

        cl("debug")

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
        self.localdata_manager.start()

        # self.dc = DataCopy()

    def tearDown(self):
        try:
            self.localdata_manager.terminate()
        except:
            pass
        # self.dc._reset()
        # del self.dc

    def test_add_file_to_queue(self):
        """add file to queue and check if it gets checked in

        """
        key = {"namespace": "some_namespace", "key": "universe.fo.nodes@0000000001.000000", "sha1sum": ""}
        namespace = "some_namespace"
        expected_index = {'some_namespace': {'0000000001.000000': {'nodes': {'object_key': 'universe.fo.nodes@0000000001.000000', 'sha1sum': ''}}}}
        expected_ns_index = {'0000000001.000000': {'nodes': {'object_key': 'universe.fo.nodes@0000000001.000000', 'sha1sum': ''}}}

        different_key = {"namespace": "some_namespace        1", "key": "universe.fo.nodes@0000000001.000000", "sha1sum": ""}
        different_expected_index = {'some_namespace        1': {'0000000001.000000': {'nodes': {'object_key': 'universe.fo.nodes@0000000001.000000', 'sha1sum': ''}}}}
        different_expected_ns_index = {'0000000001.000000': {'nodes': {'object_key': 'universe.fo.nodes@0000000001.000000', 'sha1sum': ''}}}

        self.localdata_add_file_queue.put(key)

        self.localdata_check_file_event.clear()
        self.localdata_check_file_pipe_local.send(key)
        while not self.localdata_check_file_event.wait(1):
            pass
        else:
            if self.localdata_check_file_pipe_local.poll():
                self.assertTrue(self.localdata_check_file_pipe_local.recv())

        self.localdata_check_file_event.clear()
        self.localdata_check_file_pipe_local.send(different_key)
        while not self.localdata_check_file_event.wait(1):
            pass
        else:
            if self.localdata_check_file_pipe_local.poll():
                self.assertFalse(self.localdata_check_file_pipe_local.recv())

        self.localdata_get_index_pipe_local.send(None)
        self.localdata_get_index_event.set()
        while not self.localdata_index_avail_event.wait(1):
            pass
        else:
            if self.localdata_get_index_pipe_local.poll():
                res = self.localdata_get_index_pipe_local.recv()
                self.localdata_index_avail_event.clear()
                self.assertEqual(res, expected_index)
                self.assertNotEqual(res, different_expected_index)

        self.localdata_get_index_pipe_local.send(namespace)
        self.localdata_get_index_event.set()
        while not self.localdata_index_avail_event.wait(1):
            pass
        else:
            if self.localdata_get_index_pipe_local.poll():
                res = self.localdata_get_index_pipe_local.recv()
                self.localdata_index_avail_event.clear()
                self.assertEqual(res, expected_ns_index)
                self.assertEqual(res, different_expected_ns_index)  # without the different namespace they will be identical

    def test_reset_instance(self):
        """killing the process resets the instance

        """
        key = {"namespace": "some_namespace", "key": "universe.fo.nodes@0000000001.000000", "sha1sum": ""}
        namespace = "some_namespace"
        expected_index = {'some_namespace': {'0000000001.000000': {'nodes': {'object_key': 'universe.fo.nodes@0000000001.000000', 'sha1sum': ''}}}}
        expected_ns_index = {'0000000001.000000': {'nodes': {'object_key': 'universe.fo.nodes@0000000001.000000', 'sha1sum': ''}}}

        self.localdata_add_file_queue.put(key)

        self.localdata_check_file_event.clear()
        self.localdata_check_file_pipe_local.send(key)
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
                self.assertEqual(res, expected_index)

        self.localdata_get_index_pipe_local.send(namespace)
        self.localdata_get_index_event.set()
        while not self.localdata_index_avail_event.wait(1):
            pass
        else:
            if self.localdata_get_index_pipe_local.poll():
                res = self.localdata_get_index_pipe_local.recv()
                self.localdata_index_avail_event.clear()
                self.assertEqual(res, expected_ns_index)

    def test_add_every_typical_filetype(self):
        """add all possible file types and check for them in the index

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
            entry = {"namespace": namespace, "key": filename, "sha1sum": arbitraty_hash}
            self.localdata_add_file_queue.put(entry)
        time.sleep(.01)          # wait for queue

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

        self.localdata_get_index_pipe_local.send("namespace_non_existent")
        self.localdata_get_index_event.set()
        while not self.localdata_index_avail_event.wait(1):
            pass
        else:
            if self.localdata_get_index_pipe_local.poll():
                res = self.localdata_get_index_pipe_local.recv()
                self.localdata_index_avail_event.clear()
                self.assertIsNone(res)

    def test_files_are_present(self):
        """add all possible file types and check for them in the index

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
            entry = {"namespace": namespace, "key": filename, "sha1sum": arbitraty_hash}
            self.localdata_add_file_queue.put(entry)
        time.sleep(.01)          # wait for queue

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

if __name__ == '__main__':
    unittest.main(verbosity=2)

#!/usr/bin/env python3
"""
Test local_data_instance

"""
import time
import unittest
import multiprocessing

try:
    from modules.local_data_manager import LocalDataManager
except ImportError:
    import sys
    sys.path.append('../../..')
    from modules.local_data_manager import LocalDataManager

from util.loggers import CoreLog as cl, BackendLog as bl, SimulationLog as sl


class Test_Local_Data_Manager(unittest.TestCase):
    def setUp(self):

        cl("debug")

        # create all necessary queues, pipes and events for inter process
        # communication
        #
        # inter process communication for registering new files
        #
        # a queue for sending information about new files from the simulation to the
        # data copy process
        self.queue_sim_datacopy_new_file = multiprocessing.Queue()
        #
        # a queue for requesting the hash for a new file from the ceph cluster
        self.queue_datacopy_ceph_request_hash_for_new_file = multiprocessing.Queue()
        #
        # a queue for answering the request for a hash for a new file from the ceph
        # cluster. contains the name and the hash
        self.queue_datacopy_ceph_answer_hash_for_new_file = multiprocessing.Queue()
        #
        # a queue for sending the name and hash of a new file to the backend manager
        self.queue_datacopy_backend_new_file_and_hash = multiprocessing.Queue()


        # inter process communication for requesting files from the ceph cluster
        #
        # a queue for sending a request for a file to the ceph manager
        self.queue_backend_ceph_request_file = multiprocessing.Queue()
        #
        # a queue for answering the request for a file with the file name, contents
        # and hash
        self.queue_backend_ceph_answer_file_name_contents_hash = multiprocessing.Queue()


        # inter process communication for requesting the index for the backend
        # manager from the data copy
        #
        # an event for requesting the index for the backend from the data copy
        self.event_datacopy_backend_get_index = multiprocessing.Event()
        #
        # an event for telling the backend that the index from the data copy is
        # ready for pickup
        self.event_datacopy_backend_index_ready = multiprocessing.Event()
        #
        # a pipe that connects the datacopy mgr and the backend class, for
        # transferring the requested index
        (
            self.pipe_this_end_datacopy_backend_index,
            self.pipe_that_end_datacopy_backend_index
        ) = multiprocessing.Pipe()


        # inter process communication for requesting the index for the data manager
        # from the ceph cluster
        #
        # an event for requesting the index for the data copy from the ceph cluster
        self.event_datacopy_ceph_update_index = multiprocessing.Event()
        #
        # a queue for updating the local datacopy with these names and hashes
        self.queue_datacopy_ceph_filename_and_hash = multiprocessing.Queue()


        self.localdata_manager = multiprocessing.Process(
            target=LocalDataManager,
            args=(
                self.queue_sim_datacopy_new_file,
                self.queue_datacopy_ceph_request_hash_for_new_file,
                self.queue_datacopy_ceph_answer_hash_for_new_file,
                self.queue_datacopy_backend_new_file_and_hash,

                self.event_datacopy_backend_get_index,
                self.event_datacopy_backend_index_ready,
                self.pipe_this_end_datacopy_backend_index,

                self.event_datacopy_ceph_update_index,
                self.queue_datacopy_ceph_filename_and_hash
            )
        )
        self.localdata_manager.start()

    def tearDown(self):
        try:
            self.localdata_manager.terminate()
        except:
            pass

    def xtest_add_file_to_queue(self):
        """add file to queue and check if it gets checked in

        """
        key = {"namespace": "some_namespace", "key": "universe.fo.nodes@0000000001.000000", "sha1sum": ""}
        namespace = "some_namespace"
        expected_index = {'some_namespace': {'0000000001.000000': {'nodes': {'object_key': 'universe.fo.nodes@0000000001.000000', 'sha1sum': ''}}}}
        expected_ns_index = {'0000000001.000000': {'nodes': {'object_key': 'universe.fo.nodes@0000000001.000000', 'sha1sum': ''}}}

        different_key = {"namespace": "some_namespace        1", "key": "universe.fo.nodes@0000000001.000000", "sha1sum": ""}
        different_expected_index = {'some_namespace        1': {'0000000001.000000': {'nodes': {'object_key': 'universe.fo.nodes@0000000001.000000', 'sha1sum': ''}}}}
        different_expected_ns_index = {'0000000001.000000': {'nodes': {'object_key': 'universe.fo.nodes@0000000001.000000', 'sha1sum': ''}}}

        self.queue_sim_datacopy_new_file.put(key)

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

    def test_get_index(self):
        """get the index after adding files

        """
        key = {"namespace": "some_namespace", "key": "universe.fo.nodes@0000000001.000000", "sha1sum": ""}
        namespace = "some_namespace"
        expected_index = {'some_namespace': {'0000000001.000000': {'nodes': {'object_key': 'universe.fo.nodes@0000000001.000000', 'sha1sum': ''}}}}
        expected_ns_index = {'0000000001.000000': {'nodes': {'object_key': 'universe.fo.nodes@0000000001.000000', 'sha1sum': ''}}}

        different_key = {"namespace": "some_namespace        1", "key": "universe.fo.nodes@0000000001.000000", "sha1sum": ""}
        different_expected_index = {'some_namespace        1': {'0000000001.000000': {'nodes': {'object_key': 'universe.fo.nodes@0000000001.000000', 'sha1sum': ''}}}}
        different_expected_ns_index = {'0000000001.000000': {'nodes': {'object_key': 'universe.fo.nodes@0000000001.000000', 'sha1sum': ''}}}

        self.queue_sim_datacopy_new_file.put(key)

        res = self.queue_datacopy_ceph_request_hash_for_new_file.get(True, .1)
        # res["sha1sum"] = "SOMEHASH"
        self.queue_datacopy_ceph_answer_hash_for_new_file.put(res)

        time.sleep(1e-2)

        self.event_datacopy_backend_get_index.set()
        while not self.event_datacopy_backend_index_ready.wait(1):
            pass
        else:
            while self.pipe_that_end_datacopy_backend_index.poll():
                res = self.pipe_that_end_datacopy_backend_index.recv()
                self.event_datacopy_backend_index_ready.clear()
                self.assertEqual(res, expected_index)
                self.assertNotEqual(res, different_expected_index)

    def test_reset_instance(self):
        """killing the process resets the instance

        """
        key = {"namespace": "some_namespace", "key": "universe.fo.nodes@0000000001.000000", "sha1sum": ""}
        namespace = "some_namespace"
        expected_index = {'some_namespace': {'0000000001.000000': {'nodes': {'object_key': 'universe.fo.nodes@0000000001.000000', 'sha1sum': ''}}}}
        expected_ns_index = {'0000000001.000000': {'nodes': {'object_key': 'universe.fo.nodes@0000000001.000000', 'sha1sum': ''}}}

        self.queue_sim_datacopy_new_file.put(key)

        res = self.queue_datacopy_ceph_request_hash_for_new_file.get(True, .1)
        # res["sha1sum"] = "SOMEHASH"
        self.queue_datacopy_ceph_answer_hash_for_new_file.put(res)

        time.sleep(1e-2)

        self.event_datacopy_backend_get_index.set()
        while not self.event_datacopy_backend_index_ready.wait(1):
            pass
        else:
            while self.pipe_that_end_datacopy_backend_index.poll():
                res = self.pipe_that_end_datacopy_backend_index.recv()
                self.event_datacopy_backend_index_ready.clear()
                self.assertEqual(res, expected_index)

    def test_update_hash_from_ceph(self):
        """update the hash from the ceph manager

        """
        key = {"namespace": "some_namespace", "key": "universe.fo.nodes@0000000001.000000", "sha1sum": ""}
        namespace = "some_namespace"
        expected_index = {'some_namespace': {'0000000001.000000': {'nodes': {'object_key': 'universe.fo.nodes@0000000001.000000', 'sha1sum': 'SOMEHASH'}}}}
        expected_ns_index = {'0000000001.000000': {'nodes': {'object_key': 'universe.fo.nodes@0000000001.000000', 'sha1sum': 'SOMEHASH'}}}

        self.queue_sim_datacopy_new_file.put(key)

        res = self.queue_datacopy_ceph_request_hash_for_new_file.get(True, .1)
        res["sha1sum"] = "SOMEHASH"
        self.queue_datacopy_ceph_answer_hash_for_new_file.put(res)

        time.sleep(1e-2)

        self.event_datacopy_backend_get_index.set()
        while not self.event_datacopy_backend_index_ready.wait(1):
            pass
        else:
            while self.pipe_that_end_datacopy_backend_index.poll():
                res = self.pipe_that_end_datacopy_backend_index.recv()
                self.event_datacopy_backend_index_ready.clear()
                self.assertEqual(res, expected_index)

    def test_have_hash_right_from_start(self):
        """get the hash from the simulation

        """
        key = {"namespace": "some_namespace", "key": "universe.fo.nodes@0000000001.000000", "sha1sum": "SOMEHASH"}
        namespace = "some_namespace"
        expected_index = {'some_namespace': {'0000000001.000000': {'nodes': {'object_key': 'universe.fo.nodes@0000000001.000000', 'sha1sum': 'SOMEHASH'}}}}
        expected_ns_index = {'0000000001.000000': {'nodes': {'object_key': 'universe.fo.nodes@0000000001.000000', 'sha1sum': 'SOMEHASH'}}}

        self.queue_sim_datacopy_new_file.put(key)

        self.event_datacopy_backend_get_index.set()
        while not self.event_datacopy_backend_index_ready.wait(1):
            pass
        else:
            while self.pipe_that_end_datacopy_backend_index.poll():
                res = self.pipe_that_end_datacopy_backend_index.recv()
                self.event_datacopy_backend_index_ready.clear()
                self.assertEqual(res, expected_index)

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
        arbitraty_hash = "SOMEHASH"

        expected_res = {
            namespace: {
                '0000000001.000000': {
                    'nodes': {
                        'object_key': 'universe.fo.nodes@0000000001.000000', 'sha1sum': 'SOMEHASH'
                    },
                    'elements': {
                        'c3d6': {
                            'object_key': 'universe.fo.elements.c3d6@0000000001.000000', 'sha1sum': 'SOMEHASH'
                        },
                        'c3d8': {
                            'object_key': 'universe.fo.elements.c3d8@0000000001.000000', 'sha1sum': 'SOMEHASH'
                        }
                    },
                    'nodal': {
                        'fieldname': {
                            'object_key': 'universe.fo.nodal.fieldname@0000000001.000000', 'sha1sum': 'SOMEHASH'
                        }
                    },
                    'elemental': {
                        'fieldname': {
                            'c3d6': {
                                'object_key': 'universe.fo.elemental.c3d6.fieldname@0000000001.000000', 'sha1sum': 'SOMEHASH'
                            },
                            'c3d8': {
                                'object_key': 'universe.fo.elemental.c3d8.fieldname@0000000001.000000', 'sha1sum': 'SOMEHASH'
                            }
                        }
                    },
                    'skin': {
                        'c3d6': {
                            'object_key': 'universe.fo.skin.c3d6@0000000001.000000', 'sha1sum': 'SOMEHASH'
                        },
                        'c3d8': {
                            'object_key': 'universe.fo.skin.c3d8@0000000001.000000', 'sha1sum': 'SOMEHASH'
                        }
                    },
                    'elset': {
                        '': {
                            'c3d6': {
                                'object_key': 'universe.fo.elset.c3d6@0000000001.000000', 'sha1sum': 'SOMEHASH'
                            },
                            'c3d8': {
                                'object_key': 'universe.fo.elset.c3d8@0000000001.000000', 'sha1sum': 'SOMEHASH'
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
            self.queue_sim_datacopy_new_file.put(entry)
        time.sleep(.01)          # wait for queue

        self.event_datacopy_backend_get_index.set()
        while not self.event_datacopy_backend_index_ready.wait(1):
            pass
        else:
            while self.pipe_that_end_datacopy_backend_index.poll():
                res = self.pipe_that_end_datacopy_backend_index.recv()
                self.event_datacopy_backend_index_ready.clear()
                self.assertEqual(res, expected_res)


if __name__ == '__main__':
    unittest.main(verbosity=2)

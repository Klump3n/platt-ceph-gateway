#!/usr/bin/env python3
"""
Test ceph_manager.py

"""
import time
import unittest
import pathlib
import multiprocessing

try:
    import modules.ceph_manager as cm
except ImportError:
    import sys
    sys.path.append('../../..')
    import modules.ceph_manager as cm

from util.loggers import CoreLog as cl, BackendLog as bl, SimulationLog as sl

class Test_CephManager(unittest.TestCase):
    def setUp(self):
        cl("debug")
        bl("debug")
        sl("debug")

    def tearDown(self):
        pass

    def test_get_index(self):
        """get the index from the cluster

        """
        self.ceph_config = pathlib.Path.home() / ".ccphi/simuser.ceph.conf"
        self.ceph_pool = "simdata"
        self.pool_user = "simuser"


        self.queue_request_hash_new_file = multiprocessing.Queue()
        self.queue_answer_hash_new_file = multiprocessing.Queue()

        self.queue_request_file = multiprocessing.Queue()
        self.queue_answer_file = multiprocessing.Queue()

        self.event_update_index = multiprocessing.Event()
        self.queue_update_file = multiprocessing.Queue()

        ceph = multiprocessing.Process(
            target=cm.CephManager,
            args=(
                self.ceph_config,
                self.ceph_pool,
                self.pool_user,
                self.queue_request_hash_new_file,
                self.queue_answer_hash_new_file,
                self.queue_request_file,
                self.queue_answer_file,
                self.event_update_index,
                self.queue_update_file
            )
        )

        ceph.start()
        time.sleep(.1)

        self.event_update_index.set()

        # while True:
        #     res = self.queue_update_file.get()
        #     print(res)

        time.sleep(30)
        ceph.terminate()
        time.sleep(.1)

    def test_get_hash(self):
        """get the index from the cluster

        """
        self.ceph_config = pathlib.Path.home() / ".ccphi/simuser.ceph.conf"
        self.ceph_pool = "simdata"
        self.pool_user = "simuser"


        self.queue_request_hash_new_file = multiprocessing.Queue()
        self.queue_answer_hash_new_file = multiprocessing.Queue()

        self.queue_request_file = multiprocessing.Queue()
        self.queue_answer_file = multiprocessing.Queue()

        self.event_update_index = multiprocessing.Event()
        self.queue_update_file = multiprocessing.Queue()

        ceph = multiprocessing.Process(
            target=cm.CephManager,
            args=(
                self.ceph_config,
                self.ceph_pool,
                self.pool_user,
                self.queue_request_hash_new_file,
                self.queue_answer_hash_new_file,
                self.queue_request_file,
                self.queue_answer_file,
                self.event_update_index,
                self.queue_update_file
            )
        )

        ceph.start()
        time.sleep(.1)

        ns = "numsim_napf_tiefziehversuch"

        get_hash_of = {
            "namespace": ns,
            "key": "universe.fo.nodal.PE2@000000000.035000"
        }

        self.queue_request_hash_new_file.put(get_hash_of)

        data = self.queue_answer_hash_new_file.get(True, 2)  # block for 2 seconds

        time.sleep(1)
        ceph.terminate()
        time.sleep(.1)

    def test_get_data(self):
        """get the index from the cluster

        """
        self.ceph_config = pathlib.Path.home() / ".ccphi/simuser.ceph.conf"
        self.ceph_pool = "simdata"
        self.pool_user = "simuser"


        self.queue_request_hash_new_file = multiprocessing.Queue()
        self.queue_answer_hash_new_file = multiprocessing.Queue()

        self.queue_request_file = multiprocessing.Queue()
        self.queue_answer_file = multiprocessing.Queue()

        self.event_update_index = multiprocessing.Event()
        self.queue_update_file = multiprocessing.Queue()

        ceph = multiprocessing.Process(
            target=cm.CephManager,
            args=(
                self.ceph_config,
                self.ceph_pool,
                self.pool_user,
                self.queue_request_hash_new_file,
                self.queue_answer_hash_new_file,
                self.queue_request_file,
                self.queue_answer_file,
                self.event_update_index,
                self.queue_update_file
            )
        )

        ceph.start()
        time.sleep(.1)

        ns = "numsim_napf_tiefziehversuch"

        get_data_of = {
            "namespace": ns,
            "key": "universe.fo.nodal.PE2@000000000.035000"
        }

        self.queue_request_file.put(get_data_of)

        data = self.queue_answer_file.get(True, 2)  # block for 2 seconds

        time.sleep(1)
        ceph.terminate()
        time.sleep(.1)

if __name__ == '__main__':
    unittest.main(verbosity=2)


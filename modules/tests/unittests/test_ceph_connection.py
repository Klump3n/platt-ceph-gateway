#!/usr/bin/env python3
"""
Test ceph_connection.py

"""
import time
import unittest
import pathlib
import hashlib
import multiprocessing

try:
    import modules.ceph_connection as cc
except ImportError:
    import sys
    sys.path.append('../../..')
    import modules.ceph_connection as cc

from util.loggers import CoreLog as cl, BackendLog as bl, SimulationLog as sl


class Test_CephConnection(unittest.TestCase):

    def setUp(self):
        cl("debug")
        bl("debug")
        sl("debug")

        self.ceph_config = pathlib.Path.home() / ".ccphi/simuser.ceph.conf"
        self.ceph_pool = "simdata"
        self.pool_user = "simuser"

        self.queue_ceph_tasks = multiprocessing.Queue()
        self.event_shutdown_process = multiprocessing.Event()

        self.queue_index = multiprocessing.Queue()
        self.queue_namespace_index = multiprocessing.Queue()
        self.queue_object_tags = multiprocessing.Queue()
        self.queue_object_data = multiprocessing.Queue()
        self.queue_object_hash = multiprocessing.Queue()

        num_conns = 10
        print()

        self.conns = []

        for _ in range(num_conns):
            conn = multiprocessing.Process(
                target=cc.CephConnection,
                args=(
                    self.ceph_config,
                    self.ceph_pool,
                    self.pool_user,
                    self.queue_ceph_tasks,
                    self.event_shutdown_process,
                    self.queue_index,
                    self.queue_namespace_index,
                    self.queue_object_tags,
                    self.queue_object_data,
                    self.queue_object_hash
                )
            )
            self.conns.append(conn)

        for conn in self.conns:
            conn.start()

        time.sleep(.1)

    def tearDown(self):
        time.sleep(.3)
        self.event_shutdown_process.set()
        time.sleep(.1)
        for conn in self.conns:
            conn.terminate()

    def test_multiprocess(self):
        """spawn several connections

        """
        for i in range(10):
            self.queue_ceph_tasks.put({"task": "EY_{}".format(i)})

    def xtest_index(self):
        """get the index of the cluster

        """
        # ns = "numsim_napf_tiefziehversuch"
        task = "read_index"

        task_dict = {
            "task": task,
            "task_info": {
            }
        }

        self.queue_ceph_tasks.put(task_dict)
        index = self.queue_index.get(True, 30)  # block for 2 seconds

    def test_namespace_index(self):
        """get the index of a namespace

        """
        ns = "numsim_napf_tiefziehversuch"
        task = "read_namespace_index"

        task_dict = {
            "task": task,
            "task_info": {
                "namespace": ns
            }
        }

        self.queue_ceph_tasks.put(task_dict)
        index = self.queue_namespace_index.get(True, 2)  # block for 2 seconds

    def test_read_object_value(self):
        """read the value of an object

        """
        ns = "numsim_napf_tiefziehversuch"
        task = "read_object_value"

        task_dict = {
            "task": task,
            "task_info": {
                "namespace": ns,
                "object": "universe.fo.nodal.PE2@000000000.035000"
            }
        }
        self.queue_ceph_tasks.put(task_dict)
        data = self.queue_object_data.get(True, 2)  # block for 2 seconds

        val = data["value"]

    def test_read_object_tags(self):
        """read the tags of an object

        """
        ns = "numsim_napf_tiefziehversuch"
        task = "read_object_tags"

        task_dict = {
            "task": task,
            "task_info": {
                "namespace": ns,
                "object": "universe.fo.nodal.PE2@000000000.035000"
            }
        }
        self.queue_ceph_tasks.put(task_dict)

        data = self.queue_object_tags.get(True, 2)  # block for 2 seconds

    def test_read_object_hash(self):
        """read the hash of an object

        """
        ns = "numsim_napf_tiefziehversuch"
        task = "read_object_hash"

        task_dict = {
            "task": task,
            "task_info": {
                "namespace": ns,
                "object": "universe.fo.nodal.PE2@000000000.035000"
            }
        }
        self.queue_ceph_tasks.put(task_dict)

        data = self.queue_object_hash.get(True, 2)  # block for 2 seconds


if __name__ == '__main__':
    unittest.main(verbosity=2)

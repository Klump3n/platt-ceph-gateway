#!/usr/bin/env python3
"""
Test local_data_instance

"""
import time
import queue
import unittest
import multiprocessing

try:
    # from modules.local_data_instance import DataCopy
    from modules.proxy_manager import ProxyManager
except ImportError:
    import sys
    sys.path.append('../../..')
    from modules.proxy_manager import ProxyManager

from util.loggers import CoreLog as cl, BackendLog as bl, SimulationLog as sl

class Test_ProxyManager(unittest.TestCase):
    def setUp(self):

        cl("debug")

        ceph_conf = None
        ceph_pool = "some_pool"
        ceph_user = "some_user"

        # a queue for sending information about new files to the proxy manager
        self.queue_proxy_sim_new_file = multiprocessing.Queue()

        # a queue for sending information about new files from the proxy manager
        # to the data copy process
        self.queue_proxy_datacopy_new_file = multiprocessing.Queue()

        # a queue for sending information about new files from the proxy manager
        # to the backend process
        self.queue_proxy_backend_new_file = multiprocessing.Queue()

        # two queues for getting names of files and then sending the files
        self.queue_proxy_backend_file_request = multiprocessing.Queue()
        self.queue_proxy_backend_file_send = multiprocessing.Queue()


        self.proxy_manager = multiprocessing.Process(
            target=ProxyManager,
            args=(
                ceph_conf, ceph_pool, ceph_user,
                self.queue_proxy_sim_new_file,
                self.queue_proxy_datacopy_new_file,
                self.queue_proxy_backend_new_file,
                self.queue_proxy_backend_file_request,
                self.queue_proxy_backend_file_send,
            )
        )
        self.proxy_manager.start()

    def tearDown(self):
        try:
            self.proxy_manager.terminate()
        except:
            pass

    def test_new_file(self):
        """add a new file to the mgr and see it come out of the other two queues

        """
        insert = "test"
        self.queue_proxy_sim_new_file.put(insert)
        try:
            v1 = self.queue_proxy_datacopy_new_file.get()
        except queue.Empty:
            pass
        try:
            v2 = self.queue_proxy_backend_new_file.get()
        except queue.Empty:
            pass
        self.assertEqual(v1, insert)
        self.assertEqual(v2, insert)

    def test_request_file(self):
        """request a file from ceph and have it delivered via the queue

        """
        self.queue_proxy_backend_file_request.put("name")
        try:
            res = self.queue_proxy_backend_file_send.get()
        except queue.Empty:
            pass
        else:
            print(res)

if __name__ == '__main__':
    unittest.main(verbosity=2)

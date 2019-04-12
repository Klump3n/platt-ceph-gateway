#!/usr/bin/env python3
"""
Test local_data_instance

"""
import unittest
import pathlib

try:
    import modules.ceph_interface as ci
except ImportError:
    import sys
    sys.path.append('../../..')
    import modules.ceph_interface as ci

from util.loggers import CoreLog as cl, BackendLog as bl, SimulationLog as sl

class Test_Ceph_Interface(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_ci(self):
        """get the index

        """
        # cl("debug")
        conf_path = pathlib.Path.home() / ".ccphi/simuser.ceph.conf"
        ns_k = ci.get_namespaces(str(conf_path), "simdata", "simuser")
        print(ns_k)

if __name__ == '__main__':
    unittest.main(verbosity=2)


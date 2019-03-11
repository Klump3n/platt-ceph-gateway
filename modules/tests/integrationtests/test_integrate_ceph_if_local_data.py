#!/usr/bin/env python3
"""
Test integration of ceph interface and local_data_instance

"""
# import unittest
# import pathlib

# try:
#     import modules.ceph_interface as ci
#     from modules.local_data_instance import DataCopy
# except ImportError:
#     import sys
#     sys.path.append('../..')
#     import modules.ceph_interface as ci
#     from modules.local_data_instance import DataCopy

# from modules.loggers import CoreLog as cl, BackendLog as bl, SimulationLog as sl

# class Test_Integrate_CI_and_DataCopy(unittest.TestCase):
#     def setUp(self):
#         self.dc = DataCopy()

#     def tearDown(self):
#         self.dc._reset()
#         del self.dc

#     def test_ci(self):
#         """get files from ceph and add to local data copy

#         """
#         conf_path = pathlib.Path.home() / ".ccphi/simuser.ceph.conf"
#         namespaces_keys = ci.get_namespaces(str(conf_path), "simdata", "simuser")
#         for string in namespaces_keys:
#             # print(string)
#             self.dc.add_file("ns", string, "")

# if __name__ == '__main__':
#     unittest.main(verbosity=2)


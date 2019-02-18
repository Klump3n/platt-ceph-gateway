#!/usr/bin/env python3
"""
Test local_data_instance

"""
import unittest

try:
    from modules.local_data_instance import DataCopy
except ImportError:
    import sys
    sys.path.append('..')
    from modules.local_data_instance import DataCopy

from modules.loggers import CoreLog as cl, BackendLog as bl, SimulationLog as sl

class Test_Local_Data_Instance(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_instance(self):
        """local_data_instance is singleton

        """
        a = DataCopy()
        b = DataCopy()
        self.assertEquals(id(a), id(b))

    def test_add_file(self):
        """add a file to local copy

        """
        a = DataCopy()
        a.add_file("some_file", "some_key")  # wrong format
        not_present = a.name_is_present("some_file")
        self.assertFalse(not_present)


    def test_file_is_present(self):
        """file is present in local copy

        """
        a = DataCopy()
        not_present = a.name_is_present("no_such_file")
        self.assertFalse(not_present)


if __name__ == '__main__':
    unittest.main(verbosity=2)


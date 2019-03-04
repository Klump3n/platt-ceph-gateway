#!/usr/bin/env python3
"""
Test local_data_instance

"""
import unittest
import multiprocessing

try:
    import modules.simulation_manager as sm
except ImportError:
    import sys
    sys.path.append('../..')
    import modules.simulation_manager as sm

from modules.loggers import CoreLog as cl, BackendLog as bl, SimulationLog as sl

class Test_SimulationInterface(unittest.TestCase):
    def setUp(self):
        sl("debug")

    def tearDown(self):
        sl("quiet")

    def test_just_instance(self):
        """class starts

        """
        # sm.SimulationManager()
        # sm.stop()
if __name__ == '__main__':
    unittest.main(verbosity=2)


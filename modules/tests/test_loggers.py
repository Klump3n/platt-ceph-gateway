#!/usr/bin/env python3
"""
Test integration of ceph interface and local_data_instance

"""
import unittest

try:
    from modules.loggers import CoreLog as cl, BackendLog as bl, SimulationLog as sl
except ImportError:
    import sys
    sys.path.append('../..')
    from modules.loggers import CoreLog as cl, BackendLog as bl, SimulationLog as sl

class Test_Loggers(unittest.TestCase):
    def setUp(self):
        pass
    def tearDown(self):
        pass

    def test_instances(self):
        """loggers are singletons

        """
        # quiet
        sl()
        # sl("info")
        sl.info("info 1")
        sl.debug("debug 1")

        sl.info("info 2")
        sl.debug("debug 2")

        sl.info("info 3")
        sl.debug("debug 3")


        sl.info("info 4")
        sl.debug("debug 4")

        # everything
        sl.set_level("debug")
        sl.info("info")
        sl.debug("info")
        sl.critical("info")
        sl.error("info")
        sl.warning("info")

        # no debug?
        sl.set_level("info")
        sl.info("info")
        sl.debug("info")
        sl.critical("info")
        sl.error("info")
        sl.warning("info")

if __name__ == '__main__':
    unittest.main(verbosity=2)


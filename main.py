#!/usr/bin/env python3
"""
Deliver data from the ceph cluster to the platt backend.

"""
import sys
import unittest
import argparse

from modules.loggers import CoreLog as cl, BackendLog as bl, SimulationLog as sl
from modules.local_data_instance import DataCopy

def parse_commandline():
    """
    Parse the command line arguments.

    """
    parser = argparse.ArgumentParser(
        description=__doc__,
        # add_help=False,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # in case of unittests we shouldn't have to supply config, poolname and
    # user name
    unittest_requirements = ('--test' not in sys.argv)

    parser.add_argument(
        "-c", "--config", required=unittest_requirements,  # see above
        help="Path to the configuration file"
    )
    parser.add_argument(
        "-p", "--pool", required=unittest_requirements,  # see above
        help="Name of the ceph pool"
    )
    parser.add_argument(
        "-u", "--user", required=unittest_requirements,  # see above
        help="Ceph user name"
    )
    parser.add_argument(
        "-b", "--backend_port", type=int, default=8009,
        help="The port on which the backend connects"
    )
    parser.add_argument(
        "-s", "--sim_port", type=int, default=8010,
        help="The port on which the simulation can connect"
    )
    parser.add_argument(
        "-l", "--log",
        help="Set the logging level",
        default="info",
        choices=["debug", "info", "warning", "error", "critical", "quiet"]
    )
    parser.add_argument(
        "--test",
        help="Perform unittests and exit afterwards",
        action="store_true",
        default=False
    )

    args = parser.parse_args()
    return args

def build_local_index():
    """
    Build a local index of the data on the ceph cluster.

    """
    cl().info("Building local data index")
    dc = DataCopy()

def start_simulation_interface():
    """
    Start the interface for the simulation.

    """
    sl().info("Starting simulation interface")

def start_backend_interface():
    """
    Start the interface for the backend.

    """
    bl().info("Starting backend interface")

def setup_logging(logging_level):
    """
    Setup the loggers.

    """
    cl(logging_level)           # setup simulation logging
    cl().info("Started Core logging with level '{}'".format(logging_level))
    sl(logging_level)           # setup simulation logging
    sl().info("Started Simulation logging with level '{}'".format(logging_level))
    bl(logging_level)           # setup backend logging
    bl().info("Started Backend logging with level '{}'".format(logging_level))

def start_proxy(args):
    """
    Start the proxy server.

    """
    # create an instance of the local data copy
    build_local_index()

    # start the listening port for the simulation
    start_simulation_interface()

    # start the backend connection
    start_backend_interface()

    # create a local copy of the stuff on the ceph instance

    # merge the buffered stuff with the local copy

def perform_unittests():
    """
    Start unittest for the program.

    """
    tests = unittest.TestLoader().discover('.')
    unittest.runner.TextTestRunner(verbosity=2, buffer=False).run(tests)
    # unittest.runner.TextTestRunner(verbosity=2, buffer=True).run(tests)

    sys.exit("--- Performed unittests, exiting ---")

if __name__ == "__main__":
    args = parse_commandline()
    if args.test:
        perform_unittests()
    setup_logging(args.log)
    start_proxy(args)

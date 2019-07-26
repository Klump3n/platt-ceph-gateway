#!/usr/bin/env python3
"""
Deliver data from the ceph cluster to the platt backend.

"""
import sys
import unittest
import argparse
import pathlib

from util.loggers import CoreLog as cl, BackendLog as bl, SimulationLog as sl
from util.greet import greeting

import modules.start_tasks as start_tasks

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
        "-s", "--simulation_port", type=int, default=8010,
        help="The port on which the simulation can connect"
    )
    parser.add_argument(
        "-l", "--log",
        help="Set the logging level",
        default="info",
        choices=["debug", "verbose", "info", "warning", "error", "critical", "quiet"]
    )
    parser.add_argument(
        "--test",
        help="Perform unittests and exit afterwards",
        action="store_true",
        default=False
    )

    args = parser.parse_args()
    return args

def setup_logging(logging_level):
    """
    Setup the loggers.

    """
    cl(logging_level)           # setup simulation logging
    cl.info("Started Core logging with level '{}'".format(logging_level))
    sl(logging_level)           # setup simulation logging
    sl.info("Started Simulation logging with level '{}'".format(logging_level))
    bl(logging_level)           # setup backend logging
    bl.info("Started Backend logging with level '{}'".format(logging_level))

def greet():
    """
    Print a greeting to stdout.

    """
    print(greeting)

def perform_unittests():
    """
    Start unittest for the program.

    """
    tests = unittest.TestLoader().discover('.')
    # unittest.runner.TextTestRunner(verbosity=2, buffer=False).run(tests)
    unittest.runner.TextTestRunner(verbosity=2, buffer=True).run(tests)

    sys.exit("--- Performed unittests, exiting ---")

if __name__ == "__main__":
    args = parse_commandline()
    if args.test:
        perform_unittests()
    greet()
    setup_logging(args.log)
    start_tasks.start_tasks(args)

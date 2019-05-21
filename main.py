#!/usr/bin/env python3
"""
Deliver data from the ceph cluster to the platt backend.

"""
import sys
import unittest
import argparse
import pathlib

from util.loggers import CoreLog as cl, BackendLog as bl, SimulationLog as sl
from util.greet import greeting_artwork

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

# def build_local_index(ceph_conf, ceph_pool, ceph_user):
#     """
#     Build a local index of the data on the ceph cluster.

#     """
#     cl.info("Building local data index")
#     dc = DataCopy()
#     cl.info("loading list of namespaces from rados on command line")
#     ceph_namespaces = ci.get_namespaces(ceph_conf, ceph_pool, ceph_user)
#     cl.info("Getting index from ceph")
#     for namespace in ceph_namespaces:
#         cl.debug("loading index for namespace {}".format(namespace))
#         namespace_index = ci.namespace_index(ceph_conf, ceph_pool, ceph_user, namespace)
#         cl.debug("placing {} keys in local data copy ".format(len(namespace_index)))
#         for key, val in namespace_index.items():
#             try:
#                 dc.add_file(namespace, key, val["sha1sum"])
#             except KeyError:
#                 dc.add_file(namespace, key, None)

# def start_simulation_interface():
#     """
#     Start the interface for the simulation.

#     """
#     sl.info("Starting simulation interface")

# def start_backend_interface():
#     """
#     Start the interface for the backend.

#     """
#     bl.info("Starting backend interface")

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

# def start_proxy(args):
#     """
#     Start the proxy server.

#     """
#     # start the listening port for the simulation
#     start_simulation_interface()

#     # start the backend connection
#     start_backend_interface()

#     # create an instance of the local data copy
#     ceph_conf = pathlib.Path(args.config)
#     ceph_pool = args.pool
#     ceph_user = args.user
#     build_local_index(ceph_conf, ceph_pool, ceph_user)

def greet():
    """
    Print a greeting to stdout.

    """
    greeting = """
                   a/
          .   ?"'  aa/
     "r   _wQQQQQQQQQ@      +----------------------------------+
  "?"     QQQQQQQQQQQQ      |                                  |
        ]ajQQQQQQQQQQga/    |  This is the platt proxy server  |
        jQQQQQQQQQQQQQQP    |                                  |
        QQQQQQQQQQQQQQ/     |   Connect the platt backend to   |
            wQQWQQQQQQ'     |     receive simulation data.     |
           _QQQWQQQQ?       |                                  |
           jQQmQQQP'        +----------------------------------+
           ???4WWQ'
                )?
    """
    print(greeting)
    # print(greeting_artwork)

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

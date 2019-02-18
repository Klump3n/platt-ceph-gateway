#!/usr/bin/env python3
"""
Deliver data from the ceph cluster to the platt backend.

"""
import argparse

from modules.loggers import BackendLog as bl, SimulationLog as sl

def parse_commandline():
    """
    Parse the command line arguments.

    """
    parser = argparse.ArgumentParser(
        description=__doc__,
        # add_help=False,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "-c", "--config", required=True,
        help="Path to the configuration file"
    )
    parser.add_argument(
        "-p", "--pool", required=True,
        help="Name of the ceph pool"
    )
    parser.add_argument(
        "-u", "--user", required=True,
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
    # parser.add_argument(
    #     "--test",
    #     help="Perform unittests and exit afterwards",
    #     action="store_true",
    #     default=False
    # )
    parser.add_argument(
        "-l", "--log",
        help="Set the logging level",
        default="info",
        choices=["quiet", "debug", "info", "warning", "error", "critical"]
    )
    args = parser.parse_args()
    return args

def start_simulation_interface():
    """
    Start the interface for the simulation.

    """
    pass

def start_backend_interface():
    """
    Start the interface for the backend.

    """
    pass

def setup_logging(logging_level):
    """
    Setup the loggers.

    """
    sl(logging_level)           # setup simulation logging
    sl().info("Started Simulation logging with level '{}'".format(logging_level))
    bl(logging_level)           # setup backend logging
    bl().info("Started Backend logging with level '{}'".format(logging_level))

def start_proxy(args):
    """
    Start the proxy server.

    """
    # first start the listening port for the simulation

    # load everything that comes from the sim into a buffer for later

    # start the backend connection

    # create a local copy of the stuff on the ceph instance

    # merge the buffered stuff with the local copy

if __name__ == "__main__":
    args = parse_commandline()
    setup_logging(args.log)
    start_proxy(args)

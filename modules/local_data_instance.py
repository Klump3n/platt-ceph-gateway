#!/usr/bin/env python3
"""
Maintain a local copy of the data from the ceph cluster.

"""
from modules.loggers import CoreLog as cl, BackendLog as bl, SimulationLog as sl

class DataCopy(object):
    """
    A local copy of the data on the ceph cluster.

    """
    _instance = None

    def __new__(cls):
        if not cls._instance:
            cls._instance = cls
            cls.__init__()
        return cls._instance

    @classmethod
    def __init__(cls):
        cl().info("Initializing local data copy")
        cls._setup_copy()

    @classmethod
    def _setup_copy(cls):
        cl().debug("Setting up copy of data")
        pass

    @classmethod
    def add_file(cls, name):
        pass

    @classmethod
    def get_index(cls):
        pass

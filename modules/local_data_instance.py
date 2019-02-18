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
    _hashset = set()
    _local_copy = dict()

    def __new__(cls):
        if not cls._instance:
            cls._instance = cls
            # cls.__init__()
        return cls._instance

    @classmethod
    def name_is_present(cls, name):
        """
        Check if a name is already present in the local copy.

        """
        hashed_name = hash(name)
        if hashed_name in cls._hashset:
            return True
        else:
            return False

    @classmethod
    def add_file(cls, key, sha1sum):
        sl().debug("Adding file {}/{}".format(key, sha1sum))
        hashed_key = hash(key)

        if hashed_key not in cls._hashset:

            try:
                string = key.split("universe.fo.")[1]
                string, timestep = string.split("@")
                usage_rest = string.split(".")
                usage = usage_rest[0]
                if usage == "elements":
                    elemtype = usage_rest[1]
                    fieldname = None
                if usage == "nodes":
                    elemtype = None
                    fieldname = None
                if usage == "elemental":
                    elemtype = usage_rest[1]
                    fieldname = "_".join(str(r) for r in usage_rest[2:])
                if usage == "nodal":
                    elemtype = None
                    fieldname = "_".join(str(r) for r in usage_rest[1:])
                if usage == "elset":
                    elemtype = usage_rest[1]
                    fieldname = ".".join(str(r) for r in usage_rest[2:])
                if usage == "skin":
                    elemtype = usage_rest[1]
                    fieldname = None

            except:
                # YOU SHALL NOT PARSE
                sl().debug("Can not add file {}".format(key))
                return

            cls._hashset.add(hashed_key)

            # Create the dictionary
            if namespace not in cls._local_copy:
                cls._local_copy[namespace] = {}

            if timestep not in cls._local_copy[namespace]:
                cls._local_copy[namespace][timestep] = {}

            if usage not in cls._local_copy[namespace][timestep]:
                cls._local_copy[namespace][timestep][usage] = {}

            if usage == "nodes":
                i_entry = cls._local_copy[namespace][timestep][usage]

            # if usage == "elements":
            if (usage in ["skin", "elements"]):
            # if (usage in ["elset", "elements"]):

                if elemtype not in cls._local_copy[namespace][timestep][usage]:
                    cls._local_copy[namespace][timestep][usage][elemtype] = {}
                i_entry = cls._local_copy[namespace][timestep][usage][elemtype]

            if usage in ["elemental", "elset"]:
            # if usage == "elemental":
                if fieldname not in cls._local_copy[namespace][timestep][usage]:
                    cls._local_copy[namespace][timestep][usage][fieldname] = {}
                if elemtype not in cls._local_copy[namespace][timestep][usage][fieldname]:
                    cls._local_copy[namespace][timestep][usage][fieldname][elemtype] = {}
                i_entry = cls._local_copy[namespace][timestep][usage][fieldname][elemtype]

            if usage == "nodal":
                if fieldname not in cls._local_copy[namespace][timestep][usage]:
                    cls._local_copy[namespace][timestep][usage][fieldname] = {}
                i_entry = cls._local_copy[namespace][timestep][usage][fieldname]

            i_entry['object_key'] = key
            i_entry['sha1sum'] = meta["sha1sum"]

    @classmethod
    def get_index(cls):
        return cls._local_copy

#!/usr/bin/env python3
"""
Manages the local copy of the data on the ceph cluster.

"""
import queue
import multiprocessing

from modules.loggers import CoreLog as cl, BackendLog as bl, SimulationLog as sl

class LocalDataManager(object):
    """
    A local copy of the data on the ceph cluster.

    """
    _instance = None
    _hashset = set()
    _local_copy = dict()
    _file_queue = None

    def __new__(cls,
                localdata_file_queue,
                localdata_file_check_event,
                localdata_file_check_pipe_local,
                localdata_get_index_event,
                localdata_index_avail_event,
                localdata_index_pipe_local
    ):
        cl.info("Starting LocalDataManager")
        if not cls._instance:
            cls._instance = cls
            cls.__init__(cls,
                         localdata_file_queue,
                         localdata_file_check_event,
                         localdata_file_check_pipe_local,
                         localdata_get_index_event,
                         localdata_index_avail_event,
                         localdata_index_pipe_local
            )
        return cls._instance

    def __init__(cls,
                 localdata_file_queue,
                 localdata_file_check_event,
                 localdata_file_check_pipe_local,
                 localdata_get_index_event,
                 localdata_index_avail_event,
                 localdata_index_pipe_local
    ):
        cls._localdata_file_queue = localdata_file_queue

        # pipe is a 2-tuple; first is for receiving, second is for sending
        cls._localdata_file_check_pipe_local = localdata_file_check_pipe_local
        cls._localdata_file_check_event = localdata_file_check_event

        cls._localdata_get_index_event = localdata_get_index_event
        cls._localdata_index_avail_event = localdata_index_avail_event
        cls._localdata_index_pipe_local = localdata_index_pipe_local

        while True:
            try:
                # try to read the queue
                try:
                    new_file_dict = cls._localdata_file_queue.get(block=False)
                    namespace = new_file_dict["namespace"]
                    key = new_file_dict["key"]
                    sha1sum = new_file_dict["sha1sum"]
                    cls.add_file(namespace, key, sha1sum)
                except queue.Empty:
                    pass

                # try to read the file check connection
                if cls._localdata_file_check_pipe_local.poll():
                    file_check_dict = cls._localdata_file_check_pipe_local.recv()
                    try:
                        namespace = file_check_dict["namespace"]
                        key = file_check_dict["key"]
                        cls._localdata_file_check_pipe_local.send(cls.name_is_present(namespace, key))
                        cls._localdata_file_check_event.set()
                    except KeyError:
                        pass

                # try to read the get index connection
                if cls._localdata_get_index_event.is_set():
                    cls._localdata_get_index_event.clear()
                    if cls._localdata_index_pipe_local.poll():
                        return_index = cls._localdata_index_pipe_local.recv()
                        cls._localdata_index_pipe_local.send(cls.get_index(return_index))
                        cls._localdata_index_avail_event.set()
                    else:
                        pass


            except KeyboardInterrupt:
                break

    @classmethod
    def _reset(cls):
        """
        Delete the instance.

        """
        cl.debug("Resetting LocalDataManager")
        cls._instance = None
        cls._hashset = set()
        cls._local_copy = dict()
        del cls

    @classmethod
    def name_is_present(cls, namespace, name):
        """
        Check if a name is already present in the local copy.

        """
        hashed_name = hash(str("{}\t{}".format(namespace, name)))
        if hashed_name in cls._hashset:
            return True
        else:
            return False

    @classmethod
    def add_file(cls, namespace, key, sha1sum):
        cl.debug("Adding file {}/{}/{}".format(namespace, key, sha1sum))
        hashed_key = hash(str("{}\t{}".format(namespace, key)))

        if hashed_key not in cls._hashset:

            try:
                string = key.split("universe.fo.")[1]
                field, timestep = string.split("@")
                usage_rest = field.split(".")
                usage = usage_rest[0]
                if usage == "elements":
                    elemtype = usage_rest[1]
                    fieldname = None
                elif usage == "nodes":
                    elemtype = None
                    fieldname = None
                elif usage == "elemental":
                    elemtype = usage_rest[1]
                    fieldname = "_".join(str(r) for r in usage_rest[2:])
                elif usage == "nodal":
                    elemtype = None
                    fieldname = "_".join(str(r) for r in usage_rest[1:])
                elif usage == "elset":
                    elemtype = usage_rest[1]
                    fieldname = ".".join(str(r) for r in usage_rest[2:])
                elif usage == "skin":
                    elemtype = usage_rest[1]
                    fieldname = None
                else:
                    cl.debug("Can not add file {}/{}".format(namespace, key))
                    return
            except:
                # YOU SHALL NOT PARSE
                cl.debug("Can not add file {}/{}".format(namespace, key))
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
            # i_entry['object_key'] = namespace_key.split("\t")[1]
            i_entry['sha1sum'] = sha1sum

    @classmethod
    def get_index(cls, namespace=None):
        if namespace:
            try:
                return cls._local_copy[namespace]
            except KeyError:
                return None
        return cls._local_copy

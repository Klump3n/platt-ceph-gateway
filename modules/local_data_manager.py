#!/usr/bin/env python3
"""
Manages the local copy of the data on the ceph cluster.

"""
import queue
import asyncio
import multiprocessing

from util.loggers import CoreLog as cl, BackendLog as bl, SimulationLog as sl

class LocalDataManager(object):
    """
    A local copy of the data on the ceph cluster.

    """
    _instance = None
    _hashset = set()
    _local_copy = dict()
    _file_queue = None

    def __new__(cls,
                queue_sim_datacopy_new_file,
                queue_datacopy_ceph_request_hash_for_new_file,
                queue_datacopy_ceph_answer_hash_for_new_file,
                queue_datacopy_backend_new_file_and_hash,
                event_datacopy_backend_get_index,
                queue_datacopy_backend_index_data,
                # event_datacopy_backend_index_ready,
                # pipe_this_end_datacopy_backend_index,
                event_datacopy_ceph_update_index,
                queue_datacopy_ceph_filename_and_hash
    ):
        cl.info("Starting LocalDataManager")
        if not cls._instance:
            cls._instance = cls
            cls.__init__(cls,
                         queue_sim_datacopy_new_file,
                         queue_datacopy_ceph_request_hash_for_new_file,
                         queue_datacopy_ceph_answer_hash_for_new_file,
                         queue_datacopy_backend_new_file_and_hash,
                         event_datacopy_backend_get_index,
                         queue_datacopy_backend_index_data,
                         # event_datacopy_backend_index_ready,
                         # pipe_this_end_datacopy_backend_index,
                         event_datacopy_ceph_update_index,
                         queue_datacopy_ceph_filename_and_hash
            )
        return cls._instance

    def __init__(cls,
                 queue_sim_datacopy_new_file,
                 queue_datacopy_ceph_request_hash_for_new_file,
                 queue_datacopy_ceph_answer_hash_for_new_file,
                 queue_datacopy_backend_new_file_and_hash,
                 event_datacopy_backend_get_index,
                 queue_datacopy_backend_index_data,
                 # event_datacopy_backend_index_ready,
                 # pipe_this_end_datacopy_backend_index,
                 event_datacopy_ceph_update_index,
                 queue_datacopy_ceph_filename_and_hash
    ):

        # receive new file information from the simulation
        cls._queue_sim_datacopy_new_file = queue_sim_datacopy_new_file

        # request a hash for the file
        cls._queue_datacopy_ceph_request_hash_for_new_file = queue_datacopy_ceph_request_hash_for_new_file
        cls._queue_datacopy_ceph_answer_hash_for_new_file = queue_datacopy_ceph_answer_hash_for_new_file

        # forward file and hash to the backend
        cls._queue_datacopy_backend_new_file_and_hash = queue_datacopy_backend_new_file_and_hash

        # serve index requests from the backend
        cls._event_datacopy_backend_get_index = event_datacopy_backend_get_index
        cls._queue_datacopy_backend_index_data = queue_datacopy_backend_index_data
        # cls._event_datacopy_backend_index_ready = event_datacopy_backend_index_ready
        # cls._pipe_this_end_datacopy_backend_index = pipe_this_end_datacopy_backend_index

        # request the index from the ceph cluster
        cls._event_datacopy_ceph_update_index = event_datacopy_ceph_update_index
        cls._queue_datacopy_ceph_filename_and_hash = queue_datacopy_ceph_filename_and_hash


        try:
            #
            # asyncio: watch the queue and the shutdown event
            cls._loop = asyncio.get_event_loop()

            # task for reading the queues
            cls._queue_reader_task = cls._loop.create_task(
                cls._queue_reader_coro(cls))

            # task for periodically updating the index
            cls._index_updater_task = cls._loop.create_task(
                cls._index_updater_coro(cls))

            cls._periodic_index_update_task = cls._loop.create_task(
                cls._periodic_index_update_coro(cls))

            tasks = [
                cls._queue_reader_task,
                cls._index_updater_task,
                cls._periodic_index_update_task
            ]
            cls._loop.run_until_complete(asyncio.wait(tasks))

            # stop the event loop
            cls._loop.call_soon_threadsafe(cls._loop.stop())

            cls.__del__()
            cl.debug("Shutdown of ceph_connection process complete")

        except KeyboardInterrupt:
            # Ctrl C passes quietly
            pass

    async def _queue_reader_coro(cls):
        """
        Read the queue for new things to do.

        """
        while True:

            # if cls._shutdown_local_data_manager.is_set():
            #     return

            try:
                # try to read the queue for new files
                try:
                    new_file_dict = cls._queue_sim_datacopy_new_file.get(block=False)
                    namespace = new_file_dict["namespace"]
                    key = new_file_dict["key"]
                    sha1sum = new_file_dict["sha1sum"]

                    # if we received a sha1sum we drop the file in the database
                    if not sha1sum == "":
                        cls.add_file(namespace, key, sha1sum)
                    # else we ask ceph for the hash
                    else:
                        cls._queue_datacopy_ceph_request_hash_for_new_file.put(new_file_dict)
                        # NOTE: If ceph does not answer this request we have to make sure that it still lands in the database
                except queue.Empty:
                    pass

                # try to read the queue for the attempt to get the hash from ceph
                try:
                    new_file_dict = cls._queue_datacopy_ceph_answer_hash_for_new_file.get(block=False)

                    # forward to backend
                    cls._queue_datacopy_backend_new_file_and_hash.put(new_file_dict)

                    # add file to index
                    namespace = new_file_dict["namespace"]
                    key = new_file_dict["key"]
                    sha1sum = new_file_dict["sha1sum"]
                    # sha1sum might still be not set but what can we do now
                    cls.add_file(namespace, key, sha1sum)

                except queue.Empty:
                    pass

                # try serving the index
                if cls._event_datacopy_backend_get_index.is_set():
                    cls._event_datacopy_backend_get_index.clear()
                    index = cls.get_index()
                    cls._queue_datacopy_backend_index_data.put(index)
                    # cls._pipe_this_end_datacopy_backend_index.send(index)
                    # print("3")
                    # cls._event_datacopy_backend_index_ready.set()
                    # print("4")
                await asyncio.sleep(1e-4)

            except KeyboardInterrupt:
                return

    async def _periodic_index_update_coro(cls):
        """
        Update the index periodically.

        """
        await asyncio.sleep(5)  # wait for other processes to get their stuff together
        while True:
            cl.info("Updating index")
            cls._event_datacopy_ceph_update_index.set()
            await asyncio.sleep(600)  #  wait 10 minutes

    async def _index_updater_coro(cls):
        """
        Read the queue for new things to do.

        """
        new_task = await cls._loop.run_in_executor(
            None, cls._index_updater_executor, cls)

    def _index_updater_executor(cls):
        """
        Read the queue in a separate executor.

        """
        while True:

            # if cls._shutdown_local_data_manager.is_set():
            #     return

            try:

                # add whatever ceph is throwing at us to the local data copy
                try:
                    new_file_dict = cls._queue_datacopy_ceph_filename_and_hash.get(block=False)
                    namespace = new_file_dict["namespace"]
                    key = new_file_dict["key"]
                    sha1sum = new_file_dict["sha1sum"]
                    cls.add_file(namespace, key, sha1sum)
                except queue.Empty:

                    # reset the old index
                    pass

            except KeyboardInterrupt:
                pass

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

            simtype = None

            to_parse = None

            field_type = None

            fieldname = None

            skintype = None

            elemtype = None

            try:

                string = key.split("universe.fo.")[1]
                objects, timestep = string.split("@")

                objects_definition = objects.split(".")

                simtype = objects_definition[0]

                # parse mesh or field, only field has field_type
                if objects_definition[1] in ["nodal", "elemental"]:
                    to_parse = "field"
                else:
                    to_parse = "mesh"

                if to_parse == "field":
                    usage = objects_definition[1]
                    fieldname = objects_definition[2]
                    try:
                        elemtype = objects_definition[3]
                    except IndexError:
                        pass

                elif to_parse == "mesh":
                    usage = objects_definition[1]

                    if usage == "nodes":
                        pass

                    elif usage == "elements":
                        elemtype = objects_definition[2]

                    elif usage == "skin":
                        skintype = objects_definition[2]
                        elemtype = objects_definition[3]

                    elif usage == "elementactivationbitmap":
                        elemtype = objects_definition[2]

                    elif usage == "elset":
                        fieldname = objects_definition[2]
                        elemtype = objects_definition[3]

                    elif usage == "nset":
                        fieldname = objects_definition[2]

                    elif usage == "boundingbox":
                        pass

                else:
                    cl.debug("Can not add file {}/{}".format(namespace, key))
                    return

                # usage_rest = field.split(".")
                # usage = usage_rest[0]
                # if usage == "elements":
                #     elemtype = usage_rest[1]
                #     fieldname = None
                # elif usage == "nodes":
                #     elemtype = None
                #     fieldname = None
                # elif usage == "nodes_ma":
                #     elemtype = None
                #     fieldname = None
                # elif usage == "elemental":
                #     elemtype = usage_rest[1]
                #     fieldname = "_".join(str(r) for r in usage_rest[2:])
                # elif usage == "nodal":
                #     elemtype = None
                #     fieldname = "_".join(str(r) for r in usage_rest[1:])
                # elif usage == "elset":
                #     elemtype = usage_rest[1]
                #     fieldname = ".".join(str(r) for r in usage_rest[2:])
                # elif usage == "skin":
                #     elemtype = usage_rest[1]
                #     fieldname = None


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

            if simtype not in cls._local_copy[namespace][timestep]:
                cls._local_copy[namespace][timestep][simtype] = {}

            if usage not in cls._local_copy[namespace][timestep][simtype]:
                cls._local_copy[namespace][timestep][simtype][usage] = {}

            # if usage == "nodes":
            if usage in ["nodes", "boundingbox"]:
                i_entry = cls._local_copy[namespace][timestep][simtype][usage]

            if usage == "elements":
                if elemtype not in cls._local_copy[namespace][timestep][simtype][usage]:
                    cls._local_copy[namespace][timestep][simtype][usage][elemtype] = {}
                i_entry = cls._local_copy[namespace][timestep][simtype][usage][elemtype]

            if usage == "skin":
                if skintype not in cls._local_copy[namespace][timestep][simtype][usage]:
                    cls._local_copy[namespace][timestep][simtype][usage][skintype] = {}
                if elemtype not in cls._local_copy[namespace][timestep][simtype][usage][skintype]:
                    cls._local_copy[namespace][timestep][simtype][usage][skintype][elemtype] = {}
                i_entry = cls._local_copy[namespace][timestep][simtype][usage][skintype][elemtype]

            if usage in ["elemental", "elset"]:
            # if usage == "elemental":
                if fieldname not in cls._local_copy[namespace][timestep][simtype][usage]:
                    cls._local_copy[namespace][timestep][simtype][usage][fieldname] = {}
                if elemtype not in cls._local_copy[namespace][timestep][simtype][usage][fieldname]:
                    cls._local_copy[namespace][timestep][simtype][usage][fieldname][elemtype] = {}
                i_entry = cls._local_copy[namespace][timestep][simtype][usage][fieldname][elemtype]

            if usage == "nodal":
                if fieldname not in cls._local_copy[namespace][timestep][simtype][usage]:
                    cls._local_copy[namespace][timestep][simtype][usage][fieldname] = {}
                i_entry = cls._local_copy[namespace][timestep][simtype][usage][fieldname]

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

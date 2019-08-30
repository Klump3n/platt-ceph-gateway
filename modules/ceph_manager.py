#!/usr/bin/env python3
"""
Manages requests from the ceph cluster.

"""
import time
import queue
import asyncio
import multiprocessing
from contextlib import suppress

import modules.ceph_connection as cc

from util.loggers import CoreLog as cl, BackendLog as bl, SimulationLog as sl


class CephManager(object):
    def __init__(self,
                 ceph_conf,
                 ceph_pool,
                 ceph_user,

                 event_ceph_shutdown,

                 queue_datacopy_ceph_request_hash_for_new_file,
                 queue_datacopy_ceph_answer_hash_for_new_file,

                 queue_backend_ceph_request_file,
                 queue_backend_ceph_answer_file_name_contents_hash,

                 event_datacopy_ceph_update_index,
                 queue_datacopy_ceph_filename_and_hash,

                 lock_datacopy_ceph_filename_and_hash
    ):

        self._ceph_conf = ceph_conf
        self._ceph_pool = ceph_pool
        self._ceph_user = ceph_user

        self._event_ceph_shutdown = event_ceph_shutdown

        self._queue_datacopy_ceph_request_hash_for_new_file = (
            queue_datacopy_ceph_request_hash_for_new_file
        )
        self._queue_datacopy_ceph_answer_hash_for_new_file = (
            queue_datacopy_ceph_answer_hash_for_new_file
        )

        self._queue_backend_ceph_request_file = queue_backend_ceph_request_file
        self._queue_backend_ceph_answer_file_name_contents_hash = (
            queue_backend_ceph_answer_file_name_contents_hash
        )

        self._event_datacopy_ceph_update_index = event_datacopy_ceph_update_index
        self._queue_datacopy_ceph_filename_and_hash = (
            queue_datacopy_ceph_filename_and_hash
        )

        self._lock_datacopy_ceph_filename_and_hash = lock_datacopy_ceph_filename_and_hash

        # inter process communication between ceph manager and cepj connections
        self._queue_ceph_process_new_task = multiprocessing.Queue()
        self._queue_ceph_process_new_task_data = multiprocessing.Queue()
        self._queue_ceph_process_new_task_hashes = multiprocessing.Queue()
        self._queue_ceph_process_new_task_index_hashes = multiprocessing.Queue()
        self._queue_ceph_process_new_task_index_namespaces = multiprocessing.Queue()
        self._queue_ceph_process_new_task_index = multiprocessing.Queue()
        self._event_ceph_process_shutdown = multiprocessing.Event()

        self._queue_ceph_process_index = multiprocessing.Queue()  # gets a list of namespaces
        self._queue_ceph_process_namespace_index = multiprocessing.Queue()  # gets an index for a namespace
        self._queue_ceph_process_object_tags = multiprocessing.Queue()
        self._queue_ceph_process_object_data = multiprocessing.Queue()
        self._queue_ceph_process_object_hash = multiprocessing.Queue()

        # start the ceph connections
        self._start_ceph_connections()

        self._loop = asyncio.get_event_loop()

        ceph_tasks_loop_task = self._loop.create_task(
            self._ceph_task_coro())

        self._tasks = [
            ceph_tasks_loop_task
        ]

        try:
            # start the tasks
            self._loop.run_until_complete(asyncio.wait(self._tasks))

        except KeyboardInterrupt:
            pass

        finally:

            self._loop.stop()

            all_tasks = asyncio.Task.all_tasks()

            for task in all_tasks:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    self._loop.run_until_complete(task)

            self._loop.close()

            bl.debug("CephManager is shut down")


    def _start_ceph_connections(self):
        """
        Start a list of ceph connections.

        """
        # number of concurrent connections to the ceph cluster; pool size
        num_conns = 10          # at least 2

        assert(num_conns > 1)

        self._conns = []

        # for _ in range(num_conns):
        #     conn = multiprocessing.Process(
        #         target=cc.CephConnection,
        #         args=(
        #             self._ceph_conf,
        #             self._ceph_pool,
        #             self._ceph_user,
        #             queue_task_pattern,
        #             self._queue_ceph_process_new_task,
        #             self._queue_ceph_process_new_task_data,
        #             self._queue_ceph_process_new_task_hashes,
        #             self._queue_ceph_process_new_task_index_hashes,
        #             self._queue_ceph_process_new_task_index_namespaces,
        #             self._queue_ceph_process_new_task_index,
        #             self._event_ceph_process_shutdown,
        #             self._queue_ceph_process_index,
        #             self._queue_ceph_process_namespace_index,
        #             self._queue_ceph_process_object_tags,
        #             self._queue_ceph_process_object_data,
        #             self._queue_ceph_process_object_hash
        #         )
        #     )
        #     self._conns.append(conn)

        workers = [
            {"pattern": "data", "count": 4},
            {"pattern": "hashes", "count": 6},
            # {"pattern": "index_hashes", "count": 5},
            {"pattern": "index_namespaces", "count": 8},
            {"pattern": "index", "count": 1}
        ]

        for wrkr_type in workers:

            # need at least 1 of each
            assert(wrkr_type["count"] >= 1)

            for _ in range(wrkr_type["count"]):

                conn = multiprocessing.Process(
                    target=cc.CephConnection,
                    args=(
                        self._ceph_conf,
                        self._ceph_pool,
                        self._ceph_user,
                        wrkr_type["pattern"],
                        self._queue_ceph_process_new_task,
                        self._queue_ceph_process_new_task_data,
                        self._queue_ceph_process_new_task_hashes,
                        # self._queue_ceph_process_new_task_index_hashes,
                        self._queue_ceph_process_new_task_index_namespaces,
                        self._queue_ceph_process_new_task_index,
                        self._event_ceph_process_shutdown,
                        self._queue_ceph_process_index,
                        self._queue_ceph_process_namespace_index,
                        self._queue_ceph_process_object_tags,
                        self._queue_ceph_process_object_data,
                        self._queue_ceph_process_object_hash
                    )
                )
                self._conns.append(conn)

        if len(self._conns) < 2:
            raise Exception("Need at least two concurrent connections to "
                            "the cluster")

        # start the connections
        for conn in self._conns:
            conn.start()

        # give them some time to boot up
        time.sleep(.1)

    async def _ceph_task_coro(self):
        """
        Loop over all the possible task queues for ceph.

        """
        try:

            # seems sane.. 100 checks per second
            # set to 0 if we need speed
            loop_throttle_time = 1e-2

            while True:

                # check for ceph shutdown
                if self._event_ceph_shutdown.is_set():
                    break

                ################################################################

                # first go through all the tasks and give them to the processes
                #
                # index request
                if self._event_datacopy_ceph_update_index.is_set():
                    self._event_datacopy_ceph_update_index.clear()
                    task = {
                        "task": "read_index",
                        "task_info": {}
                    }
                    self._queue_ceph_process_new_task_index.put(task)
                    # self._queue_ceph_process_new_task.put(task)

                # request for hash of file
                try:
                    hash_request = (
                        self._queue_datacopy_ceph_request_hash_for_new_file.get(
                            block=False))
                    namespace = hash_request["namespace"]
                    key = hash_request["key"]
                    task = {
                        "task": "read_object_hash",
                        "task_info": {
                            "namespace": namespace,
                            "object": key
                        }
                    }
                    self._queue_ceph_process_new_task_hashes.put(task)
                    # self._queue_ceph_process_new_task.put(task)
                except queue.Empty:
                    pass

                # request for everything of file
                try:
                    file_request = (
                        self._queue_backend_ceph_request_file.get(block=False))
                    namespace = file_request["namespace"]
                    key = file_request["key"]
                    task = {
                        "task": "read_object_value",
                        "task_info": {
                            "namespace": namespace,
                            "object": key
                        }
                    }
                    self._queue_ceph_process_new_task_data.put(task)
                    # self._queue_ceph_process_new_task.put(task)
                except queue.Empty:
                    pass

                ################################################################

                # then go through everything that the processes have done and
                # return that to the other managers
                #
                # get the index and dump it into the data copy
                try:
                    fresh_index = self._queue_ceph_process_index.get(block=False)["index"]

                except queue.Empty:
                    # turn rate throttling back on
                    if not loop_throttle_time:
                        cl.verbose("Throttling loop again (index is updated)")
                        loop_throttle_time = 1e-2

                else:
                    # disable rate throttling so we can get the whole index quickly
                    cl.verbose("Unthrottling loop (updating index)")
                    loop_throttle_time = 0

                    with self._lock_datacopy_ceph_filename_and_hash:  # LOCK

                        for namespace_index in fresh_index:
                            namespace = namespace_index["namespace"]

                            for obj in namespace_index["index"].keys():
                                tags = namespace_index["index"][obj]

                                try:
                                    sha1sum = tags["sha1sum"]
                                except KeyError:
                                    sha1sum = ""

                                ns_name_hash = {
                                    "namespace": namespace,
                                    "key": obj,
                                    "sha1sum": sha1sum
                                }

                                self._queue_datacopy_ceph_filename_and_hash.put(ns_name_hash)

                # get the hash for an object
                try:
                    obj_hash = self._queue_ceph_process_object_hash.get(block=False)
                    new_file_dict = dict()
                    new_file_dict["namespace"] = obj_hash["namespace"]
                    new_file_dict["key"] = obj_hash["object"]
                    new_file_dict["sha1sum"] = obj_hash["tags"]["sha1sum"]
                    self._queue_datacopy_ceph_answer_hash_for_new_file.put(new_file_dict)
                except queue.Empty:
                    pass

                # get everything for an object
                try:
                    obj_everything = self._queue_ceph_process_object_data.get(block=False)
                    self._queue_backend_ceph_answer_file_name_contents_hash.put(obj_everything)
                except queue.Empty:
                    pass

                ################################################################

                await asyncio.sleep(loop_throttle_time)    #  rate throttling

        finally:
            # shut down the ceph connections
            self._event_ceph_process_shutdown.set()
            time.sleep(.1)
            for conn in self._conns:
                conn.terminate()

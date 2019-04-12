#!/usr/bin/env python3
"""
Manages requests from the ceph cluster.

"""
import time
import queue
import multiprocessing

import modules.ceph_connection as cc

from util.loggers import CoreLog as cl, BackendLog as bl, SimulationLog as sl

class CephManager(object):
    def __init__(self,
                 ceph_conf,
                 ceph_pool,
                 ceph_user,

                 queue_datacopy_ceph_request_hash_for_new_file,
                 queue_datacopy_ceph_answer_hash_for_new_file,

                 queue_backend_ceph_request_file,
                 queue_backend_ceph_answer_file_name_contents_hash,

                 event_datacopy_ceph_update_index,
                 queue_datacopy_ceph_filename_and_hash
    ):

        self._ceph_conf = ceph_conf
        self._ceph_pool = ceph_pool
        self._ceph_user = ceph_user

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

        # inter process communication between ceph manager and cepj connections
        self._queue_ceph_process_new_task = multiprocessing.Queue()
        self._event_ceph_process_shutdown = multiprocessing.Event()

        self._queue_ceph_process_index = multiprocessing.Queue()  # gets a list of namespaces
        self._queue_ceph_process_namespace_index = multiprocessing.Queue()  # gets an index for a namespace
        self._queue_ceph_process_object_tags = multiprocessing.Queue()
        self._queue_ceph_process_object_data = multiprocessing.Queue()
        self._queue_ceph_process_object_hash = multiprocessing.Queue()

        # number of concurrent connections to the ceph cluster; pool size
        num_conns = 10          # at least 2

        self.conns = []

        for _ in range(num_conns):
            conn = multiprocessing.Process(
                target=cc.CephConnection,
                args=(
                    self._ceph_conf,
                    self._ceph_pool,
                    self._ceph_user,
                    self._queue_ceph_process_new_task,
                    self._event_ceph_process_shutdown,
                    self._queue_ceph_process_index,
                    self._queue_ceph_process_namespace_index,
                    self._queue_ceph_process_object_tags,
                    self._queue_ceph_process_object_data,
                    self._queue_ceph_process_object_hash
                )
            )
            self.conns.append(conn)

        try:

            if len(self.conns) < 2:
                raise Exception("Need at least two concurrent connections to "
                                "the cluster")

            # start the connections
            for conn in self.conns:
                conn.start()

            # give them some time to boot up
            time.sleep(.1)

            throttle_loop = True

            while True:

                # first go through all the tasks and give them to the processes
                #
                # index request
                if self._event_datacopy_ceph_update_index.is_set():
                    self._event_datacopy_ceph_update_index.clear()
                    task = {
                        "task": "read_index",
                        "task_info": {}
                    }
                    self._queue_ceph_process_new_task.put(task)

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
                    self._queue_ceph_process_new_task.put(task)
                except queue.Empty:
                    pass

                # # request for tags of file
                # try:
                #     tags_request = (
                #         self._queue_datacopy_ceph_request_tags_for_new_file.get(
                #             block=False))
                #     namespace = hash_request["namespace"]
                #     key = hash_request["key"]
                #     task = {
                #         "task": "read_object_tags",
                #         "task_info": {
                #             "namespace": namespace,
                #             "object": key
                #         }
                #     }
                #     self._queue_ceph_process_new_task.put(task)
                # except queue.Empty:
                #     pass

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
                    self._queue_ceph_process_new_task.put(task)
                except queue.Empty:
                    pass


                # then go through everything that the processes have done and
                # return that to the other managers
                #
                # get the index and dump it into the data copy
                try:
                    fresh_index = self._queue_ceph_process_index.get(block=False)["index"]

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

                    # disable rate throttling so we can get the whole index quickly
                    throttle_loop = False

                except queue.Empty:
                    # turn rate throttling back on
                    throttle_loop = True

                # get the hash for an object
                try:
                    obj_hash = self._queue_ceph_process_object_hash.get(block=False)
                    self._queue_datacopy_ceph_answer_hash_for_new_file.put(obj_hash)
                except queue.Empty:
                    pass

                # get everything for an object
                try:
                    obj_everything = self._queue_ceph_process_object_data.get(block=False)
                    self._queue_backend_ceph_answer_file_name_contents_hash.put(obj_everything)
                except queue.Empty:
                    pass

            if throttle_loop:
                time.sleep(1e-4)    #  rate throttling

        except KeyboardInterrupt:
            # exit without errors
            pass

        finally:

            self._event_ceph_process_shutdown.set()
            time.sleep(.1)
            for conn in self.conns:
                conn.terminate()

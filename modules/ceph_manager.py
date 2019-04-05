#!/usr/bin/env python3
"""
Manages requests from the ceph cluster.

"""
import queue

import modules.ceph_interface as ci

from util.loggers import CoreLog as cl, BackendLog as bl, SimulationLog as sl

class ProxyManager(object):
    def __init__(self,
                 ceph_conf, ceph_pool, ceph_user,
                 queue_datacopy_ceph_request_hash_for_new_file,
                 queue_datacopy_ceph_answer_hash_for_new_file,
                 queue_backend_ceph_request_file,
                 queue_backend_ceph_answer_file_name_contents_hash,
                 event_datacopy_ceph_update_index,
                 queue_datacopy_ceph_filename_and_hash
    ):

        try:

            while True:

                # if new simulation files have been checked in we have to enter
                # them into our data copy and forward it to the backend
                try:
                    new_sim_file = queue_proxy_sim_new_file.get_nowait()
                except queue.Empty:
                    pass
                else:
                    # see if we have a hash that goes with it
                    queue_proxy_datacopy_new_file.put(new_sim_file)
                    queue_proxy_backend_new_file.put(new_sim_file)

                try:
                    requested_file_name = queue_proxy_backend_file_request.get_nowait()
                except queue.Empty:
                    pass
                except Exception as e:
                    cl.error("Exception: {}".format(e))
                else:
                    file_dict = self.get_file_from_ceph(requested_file_name)
                    queue_proxy_backend_file_send.put(file_dict)

        except KeyboardInterrupt:
            # exit without errors
            pass

    def get_file_from_ceph(self, requested_file_name):
        pass

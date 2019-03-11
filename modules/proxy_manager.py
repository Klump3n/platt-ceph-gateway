#!/usr/bin/env python3
"""
Manages the operations of the proxy.

Holds the local data copy and manages connections to the ceph instance.

"""
import queue

import modules.ceph_interface as ci

from util.loggers import CoreLog as cl, BackendLog as bl, SimulationLog as sl

class ProxyManager(object):
    def __init__(self,
                 ceph_conf, ceph_pool, ceph_user,
                 queue_proxy_sim_new_file,
                 queue_proxy_datacopy_new_file,
                 queue_proxy_backend_new_file,
                 queue_proxy_backend_file_request,
                 queue_proxy_backend_file_send
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


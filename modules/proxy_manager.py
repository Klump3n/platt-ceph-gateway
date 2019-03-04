#!/usr/bin/env python3
"""
Manages the operations of the proxy.

Holds the local data copy and manages connections to the ceph instance.

"""
import modules.local_data_instance as ldi

class ProxyManager(object):
    def __init__(self,
                 ceph_conf, ceph_pool, ceph_user,
                 localdata_check_file_pipe,
                 localdata_get_index_pipe,
                 localdata_add_file_queue,
                 backend_add_file_queue,
                 backend_file_request_queue,
                 proxy_file_request_queue
    ):
        pass

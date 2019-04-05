#!/usr/bin/env python3
"""
Start the three main tasks
 - managing the local data copy
 - starting an endpoint for the simulation to update the local data copy
 - starting an endpoint for the backend to get the local data copy and files
    from the ceph instance

"""
import time
import pathlib
import multiprocessing

from util.loggers import CoreLog as cl, BackendLog as bl, SimulationLog as sl

from modules.local_data_manager import LocalDataManager
# from modules.proxy_manager import ProxyManager
from modules.backend_manager import BackendManager
from modules.simulation_manager import SimulationManager
from modules.ceph_manager import CephManager


def start_tasks(args):
    """
    Start the three main tasks.

    """
    cl.debug("Starting program tasks")

    ceph_conf = pathlib.Path(args.config)
    ceph_pool = args.pool
    ceph_user = args.user

    host = ""
    simulation_port = args.simulation_port
    backend_port = args.backend_port

    # create all necessary queues, pipes and events for inter process
    # communication
    #
    # inter process communication for registering new files
    #
    # a queue for sending information about new files from the simulation to the
    # data copy process
    queue_sim_datacopy_new_file = multiprocessing.Queue()
    #
    # a queue for requesting the hash for a new file from the ceph cluster
    queue_datacopy_ceph_request_hash_for_new_file = multiprocessing.Queue()
    #
    # a queue for answering the request for a hash for a new file from the ceph
    # cluster. contains the name and the hash
    queue_datacopy_ceph_answer_hash_for_new_file = multiprocessing.Queue()
    #
    # a queue for sending the name and hash of a new file to the backend manager
    queue_datacopy_backend_new_file_and_hash = multiprocessing.Queue()


    # inter process communication for requesting files from the ceph cluster
    #
    # a queue for sending a request for a file to the ceph manager
    queue_backend_ceph_request_file = multiprocessing.Queue()
    #
    # a queue for answering the request for a file with the file name, contents
    # and hash
    queue_backend_ceph_answer_file_name_contents_hash = multiprocessing.Queue()


    # inter process communication for requesting the index for the backend
    # manager from the data copy
    #
    # an event for requesting the index for the backend from the data copy
    event_datacopy_backend_get_index = multiprocessing.Event()
    #
    # an event for telling the backend that the index from the data copy is
    # ready for pickup
    event_datacopy_backend_index_ready = multiprocessing.Event()
    #
    # a pipe that connects the datacopy mgr and the backend class, for
    # transferring the requested index
    (
        pipe_this_end_datacopy_backend_index,
        pipe_that_end_datacopy_backend_index
    ) = multiprocessing.Pipe()


    # inter process communication for requesting the index for the data manager
    # from the ceph cluster
    #
    # an event for requesting the index for the data copy from the ceph cluster
    event_datacopy_ceph_update_index = multiprocessing.Event()
    #
    # a queue for updating the local datacopy with these names and hashes
    queue_datacopy_ceph_filename_and_hash = multiprocessing.Queue()


    # inter process communication for shutting down processes
    #
    # an event for shutting down the backend manager
    event_backend_manager_shutdown = multiprocessing.Event()
    # #
    # # an event for shutting down the
    # event__shutdown = multiprocessing.Event()


    localdata_manager = multiprocessing.Process(
        target=LocalDataManager,
        args=(
            queue_sim_datacopy_new_file,
            queue_datacopy_ceph_request_hash_for_new_file,
            queue_datacopy_ceph_answer_hash_for_new_file,
            queue_datacopy_backend_new_file_and_hash,
            event_datacopy_backend_get_index,
            event_datacopy_backend_index_ready,
            pipe_this_end_datacopy_backend_index,
            event_datacopy_ceph_update_index,
            queue_datacopy_ceph_filename_and_hash
        )
    )
    simulation_manager = multiprocessing.Process(
        target=SimulationManager,
        args=(
            host,
            simulation_port,
            queue_sim_datacopy_new_file,
        )
    )
    backend_manager = multiprocessing.Process(
        target=BackendManager,
        args=(
            host,
            backend_port,
            queue_datacopy_backend_new_file_and_hash,
            event_datacopy_backend_get_index,
            event_datacopy_backend_index_ready,
            pipe_that_end_datacopy_backend_index,
            queue_backend_ceph_request_file,
            queue_backend_ceph_answer_file_name_contents_hash,
            event_backend_manager_shutdown
        )
    )
    ceph_manager = multiprocessing.Process(
        target=CephManager,
        args=(
            ceph_conf, ceph_pool, ceph_user,
            queue_datacopy_ceph_request_hash_for_new_file,
            queue_datacopy_ceph_answer_hash_for_new_file,
            queue_backend_ceph_request_file,
            queue_backend_ceph_answer_file_name_contents_hash,
            event_datacopy_ceph_update_index,
            queue_datacopy_ceph_filename_and_hash
        )
    )

    # proxy_manager = multiprocessing.Process(
    #     target=ProxyManager,
    #     args=(
    #         ceph_conf, ceph_pool, ceph_user,
    #         queue_proxy_sim_new_file,         # receiving a new entry
    #         queue_proxy_datacopy_new_file,    # fwd to datacopy
    #         queue_proxy_backend_new_file,     # fwd to backend
    #         queue_proxy_backend_file_request,  # requesting file from ceph
    #         queue_proxy_backend_file_send,     # sending requested file
    #         # localdata_check_file_pipe,
    #         # localdata_get_index_pipe,
    #         # backend_add_file_queue,
    #         # backend_file_request_queue,
    #         # proxy_file_request_queue,
    #     )
    # )

    try:
        localdata_manager.start()
        proxy_manager.start()
        backend_manager.start()
        simulation_manager.start()

        localdata_manager.join()
        proxy_manager.join()
        backend_manager.join()
        simulation_manager.join()


    except KeyboardInterrupt:
        print()
        cl.info('Detected KeyboardInterrupt -- Shutting down')
        event_backend_manager_shutdown.set()
        time.sleep(.1)          # Give the process some time to flush it all out

    finally:
        localdata_manager.terminate()
        proxy_manager.terminate()
        backend_manager.terminate()
        simulation_manager.terminate()


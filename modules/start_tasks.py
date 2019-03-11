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
from modules.proxy_manager import ProxyManager
from modules.backend_manager import BackendManager
from modules.simulation_manager import SimulationManager


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

    # a queue for adding files to the local data copy
    localdata_add_file_queue = multiprocessing.Queue()

    # an event that gets set when the file lookup in the local data copy is done
    localdata_check_file_event = multiprocessing.Event()

    # a pipe for looking up if a file is present in the local data copy
    (
        localdata_check_file_pipe_this_end,
        localdata_check_file_pipe_that_end
    ) = multiprocessing.Pipe()

    # an event that tells the local data copy that we want the index
    localdata_get_index_event = multiprocessing.Event()

    # an event that gets set when the index is ready for pickup
    localdata_index_avail_event = multiprocessing.Event()

    # a pipe for sending the namespace we want an index for, and for the index
    # that we requested
    (
        localdata_get_index_pipe_this_end,
        localdata_get_index_pipe_that_end
    ) = multiprocessing.Pipe()



    # a queue for sending information about new files from the simulation to the
    # data copy process
    queue_sim_datacopy_new_file = multiprocessing.Queue()

    # a queue for sending information about new files from the simulation to the
    # backend process
    queue_sim_backend_new_file = multiprocessing.Queue()



    # # a queue for sending information about new files to the proxy manager
    # queue_proxy_sim_new_file = multiprocessing.Queue()

    # # a queue for sending information about new files from the proxy manager
    # # to the data copy process
    # queue_proxy_datacopy_new_file = multiprocessing.Queue()

    # # a queue for sending information about new files from the proxy manager
    # # to the backend process
    # queue_proxy_backend_new_file = multiprocessing.Queue()


    # a queue for requesting files from the proxy (for the backend process)
    queue_proxy_backend_file_request = multiprocessing.Queue()

    # a queue for sending requested files from the proxy (to the backend
    # process)
    queue_proxy_backend_file_send = multiprocessing.Queue()


    # an event for requesting the index from the proxy (for the backend process)
    event_proxy_backend_get_index = multiprocessing.Event()

    # an event telling the backend process that the requested index from the
    # proxy is ready for pickup
    event_proxy_backend_index_ready = multiprocessing.Event()

    # a pipe that connects the proxy mgr and the backend class, for transferring
    # the requested index
    (
        pipe_this_end_proxy_backend_index,
        pipe_that_end_proxy_backend_index
    ) = multiprocessing.Pipe()


    # a queue for adding a file to the backends data copy
    backend_add_file_queue = multiprocessing.Queue()

    # a queue for files that have to be pushed to the backend
    backend_file_request_queue = multiprocessing.Queue()

    # a queue for requesting files from the ceph cluster
    proxy_file_request_queue = multiprocessing.Queue()



    localdata_manager = multiprocessing.Process(
        target=LocalDataManager,
        args=(
            queue_sim_datacopy_new_file,
            localdata_check_file_event,
            localdata_check_file_pipe_this_end,
            localdata_get_index_event,
            localdata_index_avail_event,
            localdata_get_index_pipe_this_end
        )
    )
    simulation_manager = multiprocessing.Process(
        target=SimulationManager,
        args=(
            host,
            simulation_port,
            queue_sim_backend_new_file,
            queue_sim_datacopy_new_file,
        )
    )
    backend_manager = multiprocessing.Process(
        target=BackendManager,
        args=(
            host,
            backend_port,
            queue_sim_backend_new_file,
            backend_file_request_queue,
        )
    )
    proxy_manager = multiprocessing.Process(
        target=ProxyManager,
        args=(
            ceph_conf, ceph_pool, ceph_user,
            queue_proxy_sim_new_file,         # receiving a new entry
            queue_proxy_datacopy_new_file,    # fwd to datacopy
            queue_proxy_backend_new_file,     # fwd to backend
            queue_proxy_backend_file_request,  # requesting file from ceph
            queue_proxy_backend_file_send,     # sending requested file
            # localdata_check_file_pipe,
            # localdata_get_index_pipe,
            # backend_add_file_queue,
            # backend_file_request_queue,
            # proxy_file_request_queue,
        )
    )

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
        time.sleep(.1)          # Give the process some time to flush it all out

    finally:
        localdata_manager.terminate()
        proxy_manager.terminate()
        backend_manager.terminate()
        simulation_manager.terminate()


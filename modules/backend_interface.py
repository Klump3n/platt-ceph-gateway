#!/usr/bin/env python3
"""
Interface for the backend.

Backend connects and can request files from ceph, can request a list of files
and namespaces, and can be told when new timesteps are available.

"""
import json
import asyncio
# import time
# import hashlib
# import struct
# import pathlib


async def _rw_handler(reader, writer):
    """
    This gets called when a connection is established.

    """
    connection_info = writer.get_extra_info('peername')
    p_host = connection_info[0]
    p_port = connection_info[1]
    f_logger.info('Connection established from {}:{}'.format(
        p_host, p_port))

    try:
        # While the buffer is open
        while not reader.at_eof():

            todo_json = await what_todo(reader, writer)
            if todo_json is None: continue

            task_result = await perf_task(reader, writer, todo_json)
            if task_result is None: continue

    except Exception as e:
        logger.debug("Exception: {}".format(e))

    finally:
        f_logger.info('Connection terminated')
        writer.close()


async def what_todo(reader, writer):
    """
    Find out what the frontend wants us to send.

    """
    # Wait until we receive something that passes for 8 bytes of data
    jsn_len_b = await rd_len(reader)

    # Length is binary encode long int
    try:
        jsn_len = struct.unpack('L', jsn_len_b)[0]

    except Exception as e:

        # Send NACK and wait until buffer is drained
        writer.write("NACK".encode())
        await writer.drain()

        return None

    if jsn_len == 0:

        # Send NACK and wait until buffer is drained
        writer.write("NACK".encode())
        await writer.drain()

        return None

    # Send ACK and wait until buffer is drained
    writer.write("ACK".encode())
    await writer.drain()

    # Try to read exactly jsn_len bytes from the reader
    try:
        jsn_data_b = await asyncio.wait_for(rd_data(reader, jsn_len), timeout=5)

    except asyncio.TimeoutError:

        # Send NACK and wait until buffer is drained
        writer.write("NACK".encode())
        await writer.drain()

        return None

    # Decode the data into UTF8 and parse the JSON
    jsn_data = json.loads(jsn_data_b.decode("UTF-8"))

    # DO NOT send an ACK, instead do once we have decided what we have to do

    # Only return when we really have something
    return jsn_data


async def perf_task(reader, writer, task_jsn):
    """
    Perform the meta stuff around the task, defer the actual task to other
    functions.

    """
    task = task_jsn['do']

    if task not in [
            'index',
            'file'
    ]:
        writer.write("NAK".encode())
        # Send NAK and wait until buffer is drained
        await writer.drain()
        return None
    else:
        writer.write("ACK".encode())
        # Send ACK and wait until buffer is drained
        await writer.drain()

    # Read an ACK from the frontend and then proceed with our side of the task
    ack_b = await asyncio.wait_for(rd_len(reader), timeout=5)
    if not ack_b.decode('UTF8') == 'ACK':
        return None

    if task == 'index':
        namespace = task_jsn['namespace'] if task_jsn['namespace'] != "" else None
        jsn_package = json.dumps(ceph_index(namespace))
        bin_package = jsn_package.encode()
        sha1_package = hash_functions.calc_binary_sha1(bin_package)

    if task == 'file':
        namespace = task_jsn['namespace'] if task_jsn['namespace'] != "" else None
        object_key = task_jsn['object_key']
        bin_package = ceph_file(object_key, namespace)
        sha1_package = hash_functions.calc_binary_sha1(bin_package)

    data_len = len(bin_package)
    length = struct.pack('L', data_len)
    writer.write(length)
    await writer.drain()

    ack_b = await rd_len(reader)
    if not ack_b.decode('UTF8') == 'ACK':
        return None

    writer.write(bin_package)
    await writer.drain()

    frontend_sha1 = await asyncio.wait_for(rd_data(reader, 40), timeout=5)  # sha1 is 40 bytes

    if not frontend_sha1.decode() == sha1_package:
        writer.write("NAK".encode())
        # Send NAK and wait until buffer is drained
        await writer.drain()
        return None
    else:
        writer.write("ACK".encode())
        # Send ACK and wait until buffer is drained
        await writer.drain()


async def rd_len(reader):
    """
    Read 8 bytes for length information.

    """
    logger.debug("Called 'rd_len(reader)'")

    return await reader.read(8)


async def rd_data(reader, length):
    """
    Read exactly the specified amount of bytes.

    """
    logger.debug("Called 'rd_data(reader, length)'")

    return await reader.readexactly(length)


# def ceph_index(namespace=None):
#     """
#     Provide a JSON dict with the index of a namespace on the ceph cluster.

#     """
#     return provided_tasks.ceph_index(namespace)

# def ceph_file(object_key, namespace=None):
#     """
#     Get a file from the ceph cluster.

#     """
#     return provided_tasks.ceph_file(object_key, namespace)

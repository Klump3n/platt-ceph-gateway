#!/usr/bin/env python3
"""
Manages the operations of the backend.

"""
import json
import asyncio
import struct

from util.loggers import CoreLog as cl, BackendLog as bl, SimulationLog as sl

class BackendManager(object):

    def __init__(self,
                 host, port,
                 backend_add_file_queue
    ):
        self.host = host
        self.port = port

        self.backend_add_file_queue = backend_add_file_queue

        self.loop = asyncio.get_event_loop()
        self.coro = asyncio.start_server(
            self._rw_handler,
            self.host, self.port, loop=self.loop, backlog=100
        )
        self.server = self.loop.run_until_complete(self.coro)

        self.start()

    def start(self):
        try:
            bl.info('Starting BackendManager on port {}'.format(self.port))
            bl.info("\tConnect the platt backend to {}:{}".format(self.host, self.port))

            self.loop.run_forever()

        except KeyboardInterrupt:
            self.stop()

        finally:
            bl.debug('BackendManager closed')
            self.loop.close()

    def stop(self):
        self.server.close()
        self.loop.run_until_complete(self.server.wait_closed())
        self.loop.close()

        bl.info('BackendManager stopped')

    async def _rw_handler(self, reader, writer):
        """
        This gets called when a connection is established.

        """
        connection_info = writer.get_extra_info('peername')
        p_host = connection_info[0]
        p_port = connection_info[1]
        bl.info('Connection established from {}:{}'.format(
            p_host, p_port))

        self.new_file_task = self.loop.create_task(self.push_new_file(reader, writer))
        self.data_index_task = self.loop.create_task(self.get_data_and_index(reader, writer))

        try:

            await self.new_file_task
            await self.data_index_task

        except Exception as e:
            bl.error("Exception: {}".format(e))

        finally:
            bl.info('Connection terminated')
            writer.close()

    async def push_new_file(self, reader, writer):
        lock = asyncio.Lock()
        while True:
            res = self.backend_add_file_queue.get()
            jsn_pkg = json.dumps(res)
            jsn_bin = jsn_pkg.encode()
            jsn_len = len(jsn_bin)
            jsn_len_b = struct.pack('L', jsn_len)

            async with lock:

                writer.write(jsn_len_b)
                await writer.drain()

                rx_str = await self.rd_len(reader)
                rx_str = rx_str.decode()
                if rx_str == "ACK":
                    pass
                else:
                    continue

                writer.write(jsn_bin)
                await writer.drain()

                rx_str = await self.rd_len(reader)
                rx_str = rx_str.decode()
                if rx_str == "ACK":
                    pass
                else:
                    continue


    async def get_data_and_index(self, reader, writer):
        # While the buffer is open
        while not reader.at_eof():

            todo_json = await self.what_todo(reader, writer)
            if todo_json is None: continue

            task_result = await self.perf_task(reader, writer, todo_json)
            if task_result is None: continue

    async def what_todo(self, reader, writer):
        """
        Find out what the frontend wants us to send.

        """
        # Wait until we receive something that passes for 8 bytes of data
        jsn_len_b = await self.rd_len(reader)

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

    async def perf_task(self, reader, writer, task_jsn):
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
        ack_b = await asyncio.wait_for(self.rd_len(reader), timeout=5)
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

        ack_b = await self.rd_len(reader)
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

    async def rd_len(self, reader):
        """
        Read 8 bytes for length information.

        """
        bl.debug("Called 'rd_len(reader)'")
        return await reader.read(8)

    async def rd_data(self, reader, length):
        """
        Read exactly the specified amount of bytes.

        """
        bl.debug("Called 'rd_data(reader, length)'")
        return await reader.readexactly(length)

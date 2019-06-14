#!/usr/bin/env python3
"""
The backend connects to this process to stay up to date with regard to new
files. Can also request index and files from this instance.

"""
import json
import time
import queue
import base64
import struct
import asyncio
import functools
import threading
import multiprocessing
from contextlib import suppress


from util.loggers import CoreLog as cl, BackendLog as bl, SimulationLog as sl


class BackendManager(object):
    def __init__(self,
                 host,
                 port,
                 new_file_send_queue,
                 get_index_server_event,
                 index_data_queue,
                 file_name_request_server_queue,
                 file_content_name_hash_server_queue,
                 shutdown_backend_manager_event
    ):
        bl.info("BackendManager init: {}:{}".format(host, port))
        self._host = host
        self._port = port

        # expose events, pipes and queues to the class
        self._new_file_send_queue = new_file_send_queue
        self._get_index_server_event = get_index_server_event
        self._index_data_queue = index_data_queue
        self._file_name_request_server_queue = file_name_request_server_queue
        self._file_content_name_hash_server_queue = file_content_name_hash_server_queue
        self._shutdown_backend_manager_event = shutdown_backend_manager_event

        # create a server
        self._loop = asyncio.get_event_loop()
        self._coro = asyncio.start_server(
            self._rw_handler,
            self._host, self._port, loop=self._loop, backlog=100
        )
        self._server = self._loop.run_until_complete(self._coro)

        self._new_file_connection_active = False
        self._index_connection_active = False
        self._file_requests_connection_active = False
        self._file_answers_connection_active = False

        # manage the queue cleanup when there are no active connections
        queue_cleanup_task = self._loop.create_task(self._queue_cleanup_coro())
        shutdown_watch_task = self._loop.create_task(
            self._watch_shutdown_event_coro())

        bl.info("Starting BackendManager")
        try:
            self._loop.run_forever()
        except KeyboardInterrupt:
            pass                # quiet KeyboardInterrupt
        finally:
            bl.info("BackendManager stopped")

    def stop(self):
        bl.info("Stopping BackendManager")

        # await self._cancel()

        try:
            self._cancel_new_file_executor_event.set()
        except AttributeError:
            pass
        try:
            self._cancel_file_request_answer_executor_event.set()
        except AttributeError:
            pass

        self._loop.call_soon_threadsafe(self._loop.close())

    async def _cancel(self):

        self._loop.call_soon_threadsafe(self._loop.stop())

        pending = asyncio.Task.all_tasks()
        for task in pending:
            task.cancel()

    async def _rw_handler(self, reader, writer):
        """
        This gets called when a connection is established.

        """
        connection_info = writer.get_extra_info('peername')
        p_host = connection_info[0]
        p_port = connection_info[1]

        bl.info("Connection open from {}/{}".format(p_host, p_port))

        # perform a handshake with the new connection
        task_dict = await self.read_data(reader, writer)
        if not task_dict:
            await self.send_nack(writer)
            return
        await self.send_ack(writer)
        task = task_dict["task"]

        bl.info("Connection from port {} is tasked with {}".format(
            p_port, task))

        try:

            # watch the connection
            self.connection_active_task = self._loop.create_task(
                self._connection_active_coro(reader, writer))

            # depending on the task that is to be performed this creates on of
            # four tasks
            #
            # push information about new files to the client
            if task == "new_file_message":
                self._new_file_connection_active = True

                self.send_new_files_task = self._loop.create_task(
                    self._new_file_information_coro(reader, writer))

                await self.send_new_files_task

                # watch the connection
                conn_active = await self.connection_active_task
                if not conn_active:
                    bl.info("Connection to {}/{} lost".format(p_host, p_port))
                    bl.debug("Cancelling send_new_files_task")
                    self.send_new_files_task.cancel()

                self._new_file_connection_active = False

            # manage requests for the complete index from the client
            if task == "index":
                self._index_connection_active = True
                self.get_index_task = self._loop.create_task(
                    self._index_request_coro(reader, writer))

                await self.get_index_task

                # watch the connection
                conn_active = await self.connection_active_task
                if not conn_active:
                    bl.info("Connection to {}/{} lost".format(p_host, p_port))
                    bl.debug("Cancelling get_index_task")
                    self.get_index_task.cancel()

                self._index_connection_active = False


            # manage requests for file data from the ceph cluster
            if task == "file_download":

                self.file_download_task = self._loop.create_task(
                    self._file_download_coro(reader, writer))
                # optional?
                await self.file_download_task

        except Exception as e:
            bl.error("Exception: {}".format(e))
            raise

        finally:
            writer.close()


    ##################################################################
    # watch the shutdown event
    #
    async def _watch_shutdown_event_coro(self):
        """
        Watch the shutdown event and stop the backend manager if shutdown is
        requested.

        """
        while True:

            if self._shutdown_backend_manager_event.is_set():
                self.stop()
                return

            await asyncio.sleep(1e-1)


    ##################################################################
    # watch the connections and cancel tasks if connections die
    #
    async def _connection_active_coro(self, reader, writer):
        """
        Watch a connection.

        Return False if the connection dies.

        """
        while True:
            if reader.at_eof():
                return False
            await asyncio.sleep(.1)


    ##################################################################
    # handle the cleanup of queues when connections are not active
    #
    async def _queue_cleanup_coro(self):
        """
        If there are no connections to the client the queues have to be emptied.

        If they are not then there will be an unnecessary burst of information
        on connection.

        """
        await self._loop.run_in_executor(
            None, self._queue_cleanup_executor)

    def _queue_cleanup_executor(self):
        """
        Run this in a separate executor.

        """
        time.sleep(.5)          # wait for start

        while True:

            # repeat 1000 times per second, acts as rate throttling
            time.sleep(1e-3)

            if not self._new_file_connection_active:
                try:
                    self._new_file_send_queue.get(False)
                except queue.Empty:
                    pass

            if not self._index_connection_active:
                self._get_index_server_event.clear()
                try:
                    self._index_data_queue.get(False)
                except queue.Empty:
                    pass


    ##################################################################
    # handle the pushing of information about new files to the client
    #
    async def _new_file_information_coro(self, reader, writer):
        """
        Coroutine for sending information about new files to the client.

        Checks the queue for new files in a separate executor, when this
        executor returns a dictionary it gets sent to the client on the
        registered connection.

        """
        self._cancel_new_file_executor_event = threading.Event()

        new_file_watchdog = self._loop.create_task(
            self._check_file_connection(reader, writer))

        # while the connection is open ...
        while not reader.at_eof():
            # check the queue in a separate executor
            new_file_in_queue = await self._loop.run_in_executor(
                None, self._check_file_queue)

            if not new_file_in_queue:
                return

            bl.debug("Received {} for sending via socket".format(
                new_file_in_queue))

            await self._inform_client_new_file(
                reader, writer, new_file_in_queue)

    async def _check_file_connection(self, reader, writer):
        """
        Check the connection and set an event if it drops.

        """
        while True:
            if reader.at_eof():
                self._cancel_new_file_executor_event.set()
                return
            await asyncio.sleep(.1)

    def _check_file_queue(self):
        """
        Check the queue and return an element if it's not empty.

        """
        while True:
            if self._cancel_new_file_executor_event.is_set():
                self._cancel_new_file_executor_event.clear()
                return None
            try:
                new_file = self._new_file_send_queue.get(True, .1)
            except queue.Empty:
                pass
            else:
                return new_file

    async def _inform_client_new_file(self, reader, writer, new_file):
        """
        Prepares a dictionary with information about the new file at the ceph
        cluster and sends it out via the socket connection.

        """
        bl.debug("Sending information about new file to client ({})".format(
            new_file))

        todo_val = "new_file"
        new_file_dictionary = {
            "todo": todo_val,
            todo_val: new_file
        }

        await self._send_dictionary(reader, writer, new_file_dictionary)


    ##################################################################
    # handle returning the complete index for the ceph cluster if requested
    #
    async def _index_request_coro(self, reader, writer):
        """
        Listens for index requests and answers them.

        Listens for incoming data. If request for index is spotted we obtain the
        index from the local data copy and return this to the client.

        """
        # while the connection is open ...
        while not reader.at_eof():

            # wait for incoming data from the client
            res = await self.read_data(reader, writer)
            if not res:
                await self.send_nack(writer)
                return
            await self.send_ack(writer)

            if res["todo"] == "index":

                bl.debug("Received index request")

                # tell the local data copy that we request the index (index
                # event)
                self._get_index_server_event.set()

                index = self._index_data_queue.get(True, 10)  # wait up to 10 seconds
                if index:
                    await self._send_index_to_client(reader, writer, index)


    async def _send_index_to_client(self, reader, writer, index):
        """
        Prepares a dictionary with the requested index.

        """
        bl.debug("Sending index to client")

        todo_val = "index"
        index_dictionary = {
            "todo": todo_val,
            todo_val: index
        }

        await self._send_dictionary(reader, writer, index_dictionary)


    ##################################################################
    # handle requests for file contents from the client
    #
    async def _file_download_coro(self, reader, writer):
        """
        Respond to download requests.

        """
        # while the connection is open ...
        if not reader.at_eof():

            self._cancel_file_download_executor_event = threading.Event()
            download_connection_watchdog = self._loop.create_task(
                self._check_download_connection(reader, writer))

            # wait for incoming traffic from the client
            res = await self.read_data(reader, writer)

            if not res:
                await self.send_nack(writer)
                return

            res = res["requested_file"]
            namespace = res["namespace"]
            key = res["key"]
            request_json = {
                "namespace": namespace,
                "key": key
            }

            bl.debug("Request for {}/{} received".format(namespace, key))

            await self.send_ack(writer)

            # drop the request in the queue for the proxy manager
            self._file_name_request_server_queue.put(request_json)

            send_this = await self._loop.run_in_executor(
                None, self._watch_file_send_queue)

            if not send_this:
                return None

            bl.debug("Got file contents from queue")

            await self._send_file_to_client(reader, writer, send_this)


    async def _check_download_connection(self, reader, writer):
        """
        Check the connection and set an event if it drops.

        """
        while True:
            if reader.at_eof():
                self._cancel_file_download_executor_event.set()
                return
            await asyncio.sleep(.1)


    def _watch_file_send_queue(self):
        """
        Watch the queue for sending out the answer for file requests.

        """
        while True:
            if self._cancel_file_download_executor_event.is_set():
                self._cancel_file_download_executor_event.clear()
                return None
            try:
                push_file = self._file_content_name_hash_server_queue.get(True, .1)
            except queue.Empty:
                pass
            else:
                return push_file

    async def _send_file_to_client(self, reader, writer, file_dictionary):
        """
        Answer file requests.

        Encode the binary data in the file_dictionary as a base64 string. This
        has to be reversed on the other side.

        """
        bl.debug("Sending file request answer via socket")

        out_dict = dict()
        out_dict["namespace"] = file_dictionary["namespace"]
        out_dict["object"] = file_dictionary["object"]
        out_dict["contents"] = base64.b64encode(
            file_dictionary["value"]).decode()
        out_dict["tags"] = file_dictionary["tags"]

        todo_val = "file_request"
        request_answer_dictionary = {
            "todo": todo_val,
            todo_val: out_dict
        }

        await self._send_dictionary(reader, writer, request_answer_dictionary)


    ##################################################################
    # utility functions for sending and receiving data to and from the client
    #
    async def read_data(self, reader, writer):
        """
        Read data from the connection.

        NOTE: Do not forget to send an ACK or NACK after using this method.
        Otherwise the connection might hang up.

        await self.send_ack(writer)
        await self.send_nack(writer)

        """
        # wait until we have read something that is up to 1k (until the newline
        # comes)
        length_b = await reader.read(1024)

        if reader.at_eof():
            return

        try:
            # try and parse it as an int (expecting the length of the data)
            length = struct.unpack("L", length_b)[0]
        except Exception as e:
            # if something goes wrong send a nack and start anew
            await self.send_nack(writer)
            bl.error("An Exception occured: {}".format(e))
            raise
            return
        else:
            # otherwise send the ack
            await self.send_ack(writer)

        try:
            # try and read exactly the length of the data
            data = await reader.readexactly(length)
            res = data.decode("UTF-8")
            res = json.loads(res)
        except json.decoder.JSONDecodeError:
            # if we can not parse the json send a nack and start from the
            # beginning
            bl.debug("Parsing {} as json failed".format(res))
            await self.send_nack(writer)
            raise
        except Exception as e:
            # if ANYTHING else goes wrong send a nack and start from the
            # beginning
            await self.send_nack(writer)
            bl.error("An Exception occured: {}".format(e))
            raise
        else:
            # otherwise return the received data
            return res

        # await self.send_ack(writer)
        # await self.send_nack(writer)
        # NOTE: do not forget to send a final ack or nack

    async def _send_dictionary(self, reader, writer, dictionary):
        """
        Send a dictionary to the connected client.

        """
        dictionary_str = json.dumps(dictionary)
        binary_dictionary = dictionary_str.encode()

        binary_dictionary_length = len(binary_dictionary)
        binary_dictionary_length_encoded = struct.pack(
            "L", binary_dictionary_length)

        # send length
        writer.write(binary_dictionary_length_encoded)
        await writer.drain()

        # check ack or nack
        is_ack = await self.check_ack(reader)
        if not is_ack:
            return False

        # send actual file
        writer.write(binary_dictionary)
        await writer.drain()

        # check ack or nack
        is_ack = await self.check_ack(reader)
        if not is_ack:
            return False

        return True


    ##################################################################
    # small helper functions for communication with the client
    #
    async def check_ack(self, reader):
        """
        Check for ack or nack.

        Returns True (ACK) or False (NACK).

        """
        ck = await reader.read(8)
        try:
            ck = ck.decode("UTF-8")
        except Exception as e:
            bl.error("An Exception occured: {}".format(e))
            raise
        else:
            if ck.lower() == "ack":
                return True
        return False

    async def send_ack(self, writer):
        """
        Send an ACK.

        """
        writer.write("ack".encode())
        await writer.drain()

    async def send_nack(self, writer):
        """
        Send an ACK.

        """
        writer.write("nack".encode())
        await writer.drain()

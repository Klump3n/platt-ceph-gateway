#!/usr/bin/env python3
"""
Client side.

Client connects to server via one port. Server can always push messages to the
client, client can always request stuff from the server and gets it delivered in
reasonable time.

"""
import json
import time
import struct
import asyncio
import socket
import threading
import multiprocessing
import base64
import logging
import queue
from contextlib import suppress

from util.loggers import CoreLog as cl, BackendLog as bl, SimulationLog as sl

class Client(object):
    def __init__(
            self,
            host, port,
            new_file_receive_queue,
            get_index_event,
            index_avail_event,
            index_pipe_remote,
            file_name_request_client_queue,
            file_contents_name_hash_client_queue,
            shutdown_client_event
    ):
        cl.info("Client init")

        self._host = host
        self._port = port

        # set up queues and events
        self._new_file_receive_queue = new_file_receive_queue
        self._get_index_event = get_index_event
        self._index_avail_event = index_avail_event
        self._index_pipe_remote = index_pipe_remote
        self._file_name_request_client_queue = file_name_request_client_queue
        self._file_contents_name_hash_client_queue = file_contents_name_hash_client_queue
        self._shutdown_client_event = shutdown_client_event

        self._index_connection_active = False
        self._file_request_connection_active = False
        self._file_answer_connection_active = False
        self._new_file_information_connection_active = False

        self._loop = asyncio.get_event_loop()

        # create tasks for the individual connections
        new_file_information_connection_task = self._loop.create_task(
            self._new_file_information_connection_coro())
        index_connection_task = self._loop.create_task(
            self._index_connection_coro())
        file_request_connection_task = self._loop.create_task(
            self._file_request_connection_coro())
        file_answer_connection_task = self._loop.create_task(
            self._file_answer_connection_coro())

        # manage the queue cleanup when there are no active connections
        queue_cleanup_task = self._loop.create_task(
            self._queue_cleanup_coro())
        # shutdown_watch_task = self._loop.create_task(
        #     self._watch_shutdown_event_coro())

        self.tasks = [
            index_connection_task,
            file_request_connection_task,
            file_answer_connection_task,
            new_file_information_connection_task,
            queue_cleanup_task# ,
            # shutdown_watch_task
        ]

        try:
            # start the tasks
            self._loop.run_until_complete(asyncio.wait(self.tasks))
            if self._shutdown_client_event.wait():
                self.stop()
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        cl.info("Shutdown client")
        self._loop.stop()

        for task in self.tasks:
            task.cancel()
            with suppress(asyncio.CancelledError):
                self._loop.run_until_complete(task)

        # pending = asyncio.Task.all_tasks()
        # for task in pending:
        #     task.cancel()
        #     with suppress(asyncio.CancelledError):
        #         self._loop.run_until_complete(task)
        #     print(task)
        # # for task in pending:
        # #     print(task)

        self._loop.close()



        # clear when shutdown complete
        self._shutdown_client_event.clear()
        cl.info("Client shutdown complete")

    ##################################################################
    # watch the shutdown event
    #
    async def _watch_shutdown_event_coro(self):
        """
        Watch the shutdown event and stop the client if shutdown is requested.

        """
        while True:

            if self._shutdown_client_event.is_set():
                self.stop()

            await asyncio.sleep(1e-1)


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
        while True:

            # repeat 1000 times per second, acts as rate throttling
            time.sleep(1e-3)

            if not self._new_file_information_connection_active:
                # nothing to do
                pass

            if not self._index_connection_active:
                self._get_index_event.clear()

            if not self._file_request_connection_active:
                try:
                    self._file_name_request_client_queue.get(False)
                except queue.Empty:
                    pass

            if not self._file_answer_connection_active:
                # nothing to do
                pass


    ##################################################################
    # handle the pushing of information about new files from the server
    #
    async def _new_file_information_connection_coro(self):
        """
        Handle information about new files from the server.

        """
        # try to maintain the connection
        while True:

            try:
                # open a connection
                cl.info("Attempting to open a connection for receiving "
                             "new file information")
                new_file_reader, new_file_writer = (
                    await asyncio.open_connection(
                        self._host, self._port, loop=self._loop)
                )

            except (OSError, asyncio.TimeoutError):
                cl.info("Can't establish a connection to receive new file "
                             "information, waiting a bit")
                await asyncio.sleep(3.5)

            else:
                # perform a handshake on this connection
                task_handshake = {"task": "new_file_message"}
                await self.send_connection(
                    new_file_reader, new_file_writer, task_handshake)

                self._new_file_information_connection_active = True

                try:
                    while not new_file_reader.at_eof():
                        read_connection_task = self._loop.create_task(
                            self.read_new_file(new_file_reader, new_file_writer)
                        )
                        await read_connection_task

                except Exception as e:
                    cl.error("Exception in new_files: {}".format(e))

                finally:
                    new_file_writer.close()
                    self._new_file_information_connection_active = False
                    cl.info("New file connection closed")

    async def read_new_file(self, reader, writer):
        """
        Read a new file from the server.

        """
        res = await self.read_data(reader, writer)
        if not res:
            await self.send_nack(writer)
            return

        # send a final ack
        await self.send_ack(writer)

        cl.info("New file from server: {}".format(res))
        self._new_file_receive_queue.put(res)


    ##################################################################
    # handle requesting the complete index from the ceph cluster
    #
    async def _index_connection_coro(self):
        """
        Handle index requests.

        """
        while True:
            try:
                # open a connection
                cl.info("Attempting to open a connection for receiving "
                             "the index")
                index_request_reader, index_request_writer = (
                    await asyncio.open_connection(
                        self._host, self._port, loop=self._loop)
                )

            except (OSError, asyncio.TimeoutError):
                cl.info("Can't establish a connection to receive the "
                             "index, waiting a bit")
                await asyncio.sleep(3.5)

            else:
                # perform a handshake for this connection
                task_handshake = {"task": "index"}
                await self.send_connection(
                    index_request_reader, index_request_writer, task_handshake)

                self._index_connection_active = True

                self._cancel_index_executor_event = threading.Event()

                index_connection_watchdog = self._loop.create_task(
                    self._watch_index_connection(
                        index_request_reader, index_request_writer))

                try:
                    while not index_request_reader.at_eof():
                        index_event_watch_task = self._loop.create_task(
                            self.watch_index_events(
                                index_request_reader, index_request_writer)
                        )
                        await index_event_watch_task

                except Exception as e:
                    cl.error("Exception in index: {}".format(e))

                finally:
                    index_request_writer.close()
                    self._index_connection_active = True
                    cl.info("Index connection closed")

    async def _watch_index_connection(self, reader, writer):
        """
        Check the connection and set an event if it drops.

        """
        while True:
            if reader.at_eof():
                self._cancel_index_executor_event.set()
                return
            await asyncio.sleep(.1)

    async def watch_index_events(self, reader, writer):
        """
        Watch index events in a separate executor.

        """
        new_file_in_queue = await self._loop.run_in_executor(
            None, self.index_event_executor)

        if not new_file_in_queue:
            return None

        cl.info("Index request received")

        index = await self.get_index(reader, writer)
        self._index_pipe_remote.send(index)
        self._index_avail_event.set()

        return True             # something other than None

    def index_event_executor(self):
        """
        Watch the index events.

        """
        while True:

            if self._cancel_index_executor_event.is_set():
                self._cancel_index_executor_event.clear()
                return None

            get_index_event = self._get_index_event.wait(.1)

            if get_index_event:
                self._get_index_event.clear()
                return get_index_event
            else:
                pass

    async def get_index(self, reader, writer):
        """
        Write something over the connection.

        """
        dictionary = {"todo": "index"}
        if await self.send_connection(reader, writer, dictionary):
            index = await self.read_data(reader, writer)
            if not index:
                await self.send_nack(writer)
                return
            await self.send_ack(writer)
            self._index_pipe_remote.send(index)


    ##################################################################
    # handle requests for file contents from the server
    #
    async def _file_request_connection_coro(self):
        """
        Handle file requests from the server.

        """
        while True:
            try:
                # open a connection
                cl.info("Attempting to open a connection for requesting "
                             "files")
                file_request_reader, file_request_writer = (
                    await asyncio.open_connection(
                        self._host, self._port, loop=self._loop)
                )

            except (OSError, asyncio.TimeoutError):
                cl.info("Can't establish a connection to request files, "
                             "waiting a bit")
                await asyncio.sleep(3.5)

            else:
                # perform a handshake for this connection
                task_handshake_request = {"task": "file_requests"}
                await self.send_connection(
                    file_request_reader,
                    file_request_writer,
                    task_handshake_request
                )

                self._file_request_connection_active = True

                self._cancel_file_request_executor_event = threading.Event()

                file_request_connection_watchdog = self._loop.create_task(
                    self._watch_file_request_connection(
                        file_request_reader, file_request_writer))

                try:
                    while not file_request_reader.at_eof():
                        watch_file_request_queue_task = self._loop.create_task(
                            self.watch_file_request_queue(
                                file_request_reader, file_request_writer)
                        )
                        await watch_file_request_queue_task

                except Exception as e:
                    cl.error("Exception in requests: {}".format(e))

                finally:
                    file_request_writer.close()
                    self._file_request_connection_active = False
                    cl.info("Request connection closed")

    async def _watch_file_request_connection(self, reader, writer):
        """
        Check the connection and set an event if it drops.

        """
        while True:
            if reader.at_eof():
                self._cancel_file_request_executor_event.set()
                return
            await asyncio.sleep(.1)

    async def watch_file_request_queue(self, reader, writer):
        """
        Watch index events in a separate executor.

        """
        file_request_in_queue = await self._loop.run_in_executor(
            None, self.file_request_event_executor)

        if not file_request_in_queue:
            return None

        cl.info("Requested file data and hash")
        await self.send_connection(reader, writer, file_request_in_queue)

    def file_request_event_executor(self):
        """
        Watch the index events.

        """
        while True:

            if self._cancel_file_request_executor_event.is_set():
                self._cancel_file_request_executor_event.clear()
                return None

            try:
                push_file = self._file_name_request_client_queue.get(True, .1)
            except queue.Empty:
                pass
            else:
                return push_file


    ##################################################################
    # handle answers to requests for file contents from the server
    #
    async def _file_answer_connection_coro(self):
        """
        Handle answers to file requests from the server.

        """
        while True:
            try:
                # open a connection
                cl.info("Attempting to open a connection for receiving "
                             "requested files")
                file_answer_reader, file_answer_writer = (
                    await asyncio.open_connection(
                        self._host, self._port, loop=self._loop)
                )

            except (OSError, asyncio.TimeoutError):
                cl.info("Can't establish a connection to receive "
                             "requested files, waiting a bit")
                await asyncio.sleep(3.5)

            else:
                # perform a handshake for this connection
                task_handshake_answer = {"task": "file_answers"}
                await self.send_connection(
                    file_answer_reader, file_answer_writer, task_handshake_answer)

                self._file_answer_connection_active = True

                try:
                    while not file_answer_reader.at_eof():
                        watch_file_request_server_answer_task = (
                            self._loop.create_task(
                                self.watch_file_request_server_answer(
                                    file_answer_reader, file_answer_writer)
                            )
                        )
                        await watch_file_request_server_answer_task

                except Exception as e:
                    cl.error("Exception in requests: {}".format(e))

                finally:
                    file_answer_writer.close()
                    self._file_answer_connection_active = False
                    cl.info("Answer connection closed")

    async def watch_file_request_server_answer(self, reader, writer):
        """
        Watch index events in a separate executor.

        """
        res = await self.read_data(reader, writer)
        if not res:
            await self.send_nack(writer)
            return
        await self.send_ack(writer)
        res["file_request"]["contents"] = base64.b64decode(
            res["file_request"]["contents"].encode())
        self._file_contents_name_hash_client_queue.put(res)


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
            cl.error("An Exception occured: {}".format(e))
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
            cl.debug("Parsing {} as json failed".format(res))
            await self.send_nack(writer)
            raise
            return
        except Exception as e:
            # if ANYTHING else goes wrong send a nack and start from the
            # beginning
            await self.send_nack(writer)
            cl.error("An Exception occured: {}".format(e))
            raise
            return
        else:
            # otherwise return the received data
            return res

        # await self.send_ack(writer)
        # await self.send_nack(writer)
        # NOTE: do not forget to send a final ack or nack


    async def send_connection(self, reader, writer, dictionary):
        """
        Write something over the connection.

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
    # small helper functions for communication with the server
    #
    async def check_ack(self, reader):
        """
        Check for ack or nack.

        """
        ck = await reader.read(8)
        try:
            ck = ck.decode("UTF-8")
        except Exception as e:
            cl.error("An Exception occured: {}".format(e))
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

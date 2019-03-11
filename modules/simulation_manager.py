#!/usr/bin/env python3
"""
Manages the operations of the simulation.

"""
import struct
import asyncio

from util.loggers import CoreLog as cl, BackendLog as bl, SimulationLog as sl

class SimulationManager(object):
    def __init__(
            self, host, port,
            queue_sim_backend_new_file,
            queue_sim_datacopy_new_file
    ):

        self.host = host
        self.port = port

        self.queue_sim_datacopy_new_file = queue_sim_datacopy_new_file
        self.queue_sim_backend_new_file = queue_sim_backend_new_file

        self.loop = asyncio.get_event_loop()
        self.coro = asyncio.start_server(
            self._rw_handler,
            self.host, self.port, loop=self.loop, backlog=100
        )
        self.server = self.loop.run_until_complete(self.coro)

        self.start()

    def start(self):
        try:
            sl.info("Starting simulation manager on port {}".format(self.port))
            sl.info("\tSend information about new objects on the ceph cluster "
                    "to {}:{}".format(self.host, self.port))
            self.loop.run_forever()

        except KeyboardInterrupt:
            self.stop()

        finally:
            sl.debug("SimulationManager closed")
            self.loop.close()

    def stop(self):
        self.server.close()
        self.loop.run_until_complete(self.server.wait_closed())
        self.loop.close()

        sl.info("SimulationManager stopped")

    async def _rw_handler(self, reader, writer):

        sl.debug("Called 'rw_handler(reader, writer)' in simulation_interface")

        connection_info = writer.get_extra_info('peername')
        p_host = connection_info[0]
        p_port = connection_info[1]
        sl.debug('Connection established from {}:{}'.format(p_host, p_port))

        try:
            await self.conn_data(reader, writer)

        except Exception as e:
            sl.error("Exception: {}".format(e))

        finally:
            writer.close()
            sl.debug('Connection closed')

    async def rd_onek(self, reader):
        """
        Read a max of 1k of data.

        """
        sl.debug("Called 'rd_data(reader)'")
        return await reader.read(1024)

    async def conn_data(self, reader, writer):
        """
        Read 1kB of string data and enter it into the local data copy

        """
        sl.debug("Called 'conn_data(reader, writer)'")

        try:
            data_binary = await asyncio.wait_for(self.rd_onek(reader), timeout=5)
        except asyncio.TimeoutError:
            sl.debug("Failed reading 1kB from the open connection")
            writer.close()
            return

        data = data_binary.decode()

        # Close connection if len is 0
        if len(data) == 0:
            sl.debug("Received package is empty")
            writer.close()
            return

        # decompose the received string
        try:
            data_tab_split = data.split("\t")
            if not len(data_tab_split) == 3:
                sl.debug("Received package is not formatted correctly")
                print(data)
                writer.close()
                return

            namespace = data_tab_split[0]
            key = data_tab_split[1]
            sha1sum = data_tab_split[2]
            entry = {"namespace": namespace, "key": key, "sha1sum": sha1sum}

            # drop the dictionary into the queues to the backend and the local
            # data copy
            self.queue_sim_backend_new_file.put(entry)
            self.queue_sim_datacopy_new_file.put(entry)

        except Exception as e:
            sl.error("Exception: {}".format(e))
        finally:
            writer.close()
            return

#!/usr/bin/env python3
"""
Manages the operations of the backend.

"""
import asyncio

from util.loggers import CoreLog as cl, BackendLog as bl, SimulationLog as sl

import modules.backend_interface as bi

class BackendManager(object):
    def __init__(self, host, port, backend_add_file_queue, backend_file_request_queue):
        self.host = host
        self.port = port

        self.loop = asyncio.get_event_loop()
        self.coro = asyncio.start_server(
            bi._rw_handler,
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

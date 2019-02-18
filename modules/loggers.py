#!/usr/bin/env python3
"""
Logging classes for the proxy.

"""
import logging

class BaseLog(object):
    _instance = None
    _logger = None
    _logging_level = None

    def __init__(self, name=None, logging_level=None):

        if logging_level is None:
            logging_level = "quiet"

        if not name or logging_level not in ["quiet", "debug", "info", "warning",
                                 "error", "critical"]:
            raise AttributeError(
                "Need name and logging_level ('debug', 'info', "
                "'warning', 'error', 'critical', 'quiet')"
            )
        if logging_level == "quiet":
            logging_level = logging.NOTSET
        elif logging_level == 'debug':
            logging_level = logging.DEBUG
        elif logging_level == 'info':
            logging_level = logging.INFO
        elif logging_level == 'warning':
            logging_level = logging.WARNING
        elif logging_level == 'error':
            logging_level = logging.ERROR
        elif logging_level == 'critical':
            logging_level = logging.CRITICAL

        self._logging_level = logging_level

        if not self._logger:
            self._logger = logging.getLogger(name)
            self._logger.setLevel(logging_level)
            formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(name)s: %(message)s')
            ch = logging.StreamHandler()
            ch.setLevel(logging_level)
            ch.setFormatter(formatter)
            self._logger.addHandler(ch)


class CoreLog(BaseLog):
    def __new__(cls, logging_level=None):
        if cls._logging_level is not None and logging_level is not None:
            raise AttributeError("CoreLog needs to be set to a logging level first")
        if not cls._instance:
            super().__init__(cls, "CORE", logging_level)
            cls._instance = cls
        return cls._instance._logger

class SimulationLog(BaseLog):
    def __new__(cls, logging_level=None):
        if cls._logging_level is not None and logging_level is not None:
            raise AttributeError("SimulationLog needs to be set to a logging level first")
        if not cls._instance:
            super().__init__(cls, "SIMULATION", logging_level)
            cls._instance = cls
        return cls._instance._logger

class BackendLog(BaseLog):
    def __new__(cls, logging_level=None):
        if cls._logging_level is not None and logging_level is not None:
            raise AttributeError("BackendLog needs to be set to a logging level first")
        if not cls._instance:
            super().__init__(cls, "BACKEND", logging_level)
            cls._instance = cls
        return cls._instance._logger

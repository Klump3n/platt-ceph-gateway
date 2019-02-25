#!/usr/bin/env python3
"""
Logging classes for the proxy.

"""
import logging

class BaseLogger(object):

    class _BaseLogger(object):
        _name = None
        _logging_level = None

        _logger = None
        _ch = None

        def __init__(self, name, logging_level=None):
            if not name:
                raise AttributeError("Need to provide a name for the logger")
            self._name = name
            self.set_level(logging_level)

        def set_level(self, logging_level):
            if logging_level is None:
                logging_level = "quiet"

            if logging_level not in ["quiet", "debug", "info", "warning",
                                     "error", "critical"]:
                raise AttributeError(
                    "Need name and logging_level ('debug', 'info', "
                    "'warning', 'error', 'critical', 'quiet')"
                )

            self._logging_level = logging_level

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

            self._logger = logging.getLogger(self._name)
            self._logger.setLevel(logging_level)
            formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(name)s: %(message)s')
            if self._ch:
                self._logger.removeHandler(self._ch)
            self._ch = logging.StreamHandler()
            self._ch.setLevel(logging_level)
            self._ch.setFormatter(formatter)
            self._logger.addHandler(self._ch)

    _logger = None
    _name = None

    def __init__(self, name, logging_level=None):
        self.__class__._name = name
        if not self.__class__._logger:
            self.__class__._logger = BaseLogger._BaseLogger(
                self.__class__._name, logging_level)

    @classmethod
    def set_level(cls, logging_level=None):
        class_exists = (cls._logger)
        if not class_exists:
            raise AttributeError("Instantiate class first to set the level")
        name_exists = (cls._name is not None)
        level_is_different = (cls._logger._logging_level != logging_level)
        if class_exists:
            if name_exists:
                cls._logger.set_level(logging_level)

    @classmethod
    def debug(cls, msg=None):
        if cls._logger is None or msg is None:
            pass
        else:
            cls._logger._logger.debug(msg)

    @classmethod
    def info(cls, msg=None):
        if cls._logger is None or msg is None:
            pass
        else:
            cls._logger._logger.info(msg)

    @classmethod
    def warning(cls, msg=None):
        if cls._logger is None or msg is None:
            pass
        else:
            cls._logger._logger.warning(msg)

    @classmethod
    def error(cls, msg=None):
        if cls._logger is None or msg is None:
            pass
        else:
            cls._logger._logger.error(msg)

    @classmethod
    def critical(cls, msg=None):
        if cls._logger is None or msg is None:
            pass
        else:
            cls._logger._logger.critical(msg)


class CoreLog(BaseLogger):
    def __new__(cls, logging_level=None):
        super().__init__("CORE", logging_level)

class SimulationLog(BaseLogger):
    def __init__(self, logging_level=None):
        super().__init__("SIMULATION", logging_level)

class BackendLog(BaseLogger):
    def __new__(cls, logging_level=None):
        super().__init__("BACKEND", logging_level)

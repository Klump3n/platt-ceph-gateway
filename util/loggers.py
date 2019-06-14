#!/usr/bin/env python3
"""
Logging classes for the proxy.

"""
import logging


class BaseLogger(object):

    _logger = None
    _name = None

    class _BaseLogger(object):
        _name = None
        _logging_level = None

        _logger = None
        _ch = None

        def __init__(self, name=None, logging_level="info", time=True):
            if self._logger:
                pass
            else:
                if not name:
                    raise AttributeError("Need to provide a name for the logger")
                self._name = name
                self._time = time
                self.set_level(logging_level)

        def set_level(self, logging_level):
            if logging_level is None:
                logging_level = "quiet"

            if logging_level not in ["quiet", "debug", "verbose", "info",
                                     "warning", "error", "critical"]:
                raise AttributeError(
                    "Need name and logging_level ('debug', 'verbose', 'info', "
                    "'warning', 'error', 'critical', 'quiet')"
                )

            self._logging_level = logging_level


            if not logging_level:
                logging_level = logging.INFO

            elif logging_level == "quiet":
                logging_level = logging.NOTSET

            elif logging_level == "debug":
                logging_level = logging.DEBUG
            elif logging_level == "debug_warning":
                logging_level = 11  # custom

            elif logging_level == "verbose":
                logging_level = 15  # custom
            elif logging_level == "verbose_warning":
                logging_level = 16  # custom

            elif logging_level == "info":
                logging_level = logging.INFO
            elif logging_level == "warning":
                logging_level = logging.WARNING

            elif logging_level == "error":
                logging_level = logging.ERROR
            elif logging_level == "critical":
                logging_level = logging.CRITICAL
            elif logging_level == "fatal":
                logging_level = logging.FATAL

            # add a verbose setting
            logging.addLevelName(11, "DEBUG WARNING")

            # add a verbose setting
            logging.addLevelName(15, "VERBOSE")

            # add a verbose setting
            logging.addLevelName(16, "VERBOSE WARNING")


            if self._name and self._time:
                fmt = "[%(asctime)s,%(msecs)03d; %(name)s; %(levelname)s] %(message)s"
            elif self._name and not self._time:
                fmt = "[%(name)s; %(levelname)s] %(message)s"
            elif (not self._name or self._name == "") and self._time:
                fmt = "[%(asctime)s,%(msecs)03d; %(levelname)s] %(message)s"
            else:
                fmt = "[%(levelname)s] %(message)s"


            self._logger = logging.getLogger(self._name)

            self._logger.setLevel(logging_level)
            self._logger.propagate = False  # log things only once, here

            fmt_date = "%d.%m.%Y %T"
            formatter = logging.Formatter(fmt, fmt_date)

            if self._ch:
                self._logger.removeHandler(self._ch)

            self._ch = logging.StreamHandler()
            self._ch.setLevel(logging_level)
            self._ch.setFormatter(formatter)

            self._logger.addHandler(self._ch)

            if logging_level == logging.NOTSET:
                logging.disable()

    def __init__(self, name, logging_level=None, time=True):
        self.__class__._name = name

        if not self.__class__._logger:
            self.__class__._logger = BaseLogger._BaseLogger(
                self.__class__._name, logging_level, time)

    # @classmethod
    # def set_level(cls, logging_level=None):
    #     class_exists = (cls._logger)
    #     if not class_exists:
    #         raise AttributeError("Instantiate class first to set the level")
    #     name_exists = (cls._name is not None)
    #     level_is_different = (cls._logger._logging_level != logging_level)
    #     if class_exists:
    #         if name_exists:
    #             cls._logger.set_level(logging_level)


    @classmethod
    def debug(cls, msg=None):
        if cls._logger is None or msg is None:
            pass
        else:
            cls._logger._logger.debug("\u001b[36m{}\u001b[0m".format(msg))

    @classmethod
    def debug_warning(cls, msg=None):
        if cls._logger is None or msg is None:
            pass
        else:
            cls._logger._logger.log(11, "\u001b[33m{}\u001b[0m".format(msg))


    @classmethod
    def verbose(cls, msg=None):
        if cls._logger is None or msg is None:
            pass
        else:
            cls._logger._logger.log(15, "{}".format(msg))

    @classmethod
    def verbose_warning(cls, msg=None):
        if cls._logger is None or msg is None:
            pass
        else:
            cls._logger._logger.log(16, "\u001b[33m{}\u001b[0m".format(msg))


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
            cls._logger._logger.warning("\u001b[33m{}\u001b[0m".format(msg))


    @classmethod
    def error(cls, msg=None):
        if cls._logger is None or msg is None:
            pass
        else:
            cls._logger._logger.error("\u001b[31m{}\u001b[0m".format(msg))

    @classmethod
    def critical(cls, msg=None):
        if cls._logger is None or msg is None:
            pass
        else:
            cls._logger._logger.critical("\u001b[31;1m{}\u001b[0m".format(msg))



class CoreLog(BaseLogger):
    def __init__(self, logging_level=None):
        super().__init__("CORE", logging_level, True)

class SimulationLog(BaseLogger):
    def __init__(self, logging_level=None):
        super().__init__("SIMULATION", logging_level, True)

class BackendLog(BaseLogger):
    def __init__(self, logging_level=None):
        super().__init__("BACKEND", logging_level, True)

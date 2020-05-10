"""
Author: Liran Funaro <liran.funaro@gmail.com>

Copyright (C) 2006-2018 Liran Funaro

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""
import threading
import logging


class LoggedObject:
    """
    This is a base class to be inherit from.
    Is creates a logger for the child class with the name of the class and a specialized name.
    """
    slots = '__entity_name__', '__log_name__', 'logger'

    def __init__(self, name=None):
        self.__entity_name__ = None
        self.__log_name__ = None
        self.logger = None
        self.rename_logger(name)

    @property
    def entity_name(self):
        return self.__entity_name__

    @property
    def logger_name(self):
        return self.__log_name__

    def __repr__(self):
        return self.__log_name__

    def rename_logger(self, name=None, use_class_name=True):
        """
        Rename the logger.
        :param name: The name of the logger. If is None, then only the class name will be used
        :param use_class_name: If true, will use the class name as prefix.
        """
        self.__entity_name__ = name
        if name is not None and use_class_name:
            self.__log_name__ = f"{self.__class__.__name__}-{name}"
        elif name is not None:
            self.__log_name__ = name
        else:
            self.__log_name__ = self.__class__.__name__

        self.logger = logging.getLogger(self.__log_name__)


class LoggedThread(LoggedObject, threading.Thread):
    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs=None, *, daemon=None, verbose=True):
        LoggedObject.__init__(self, name)
        threading.Thread.__init__(self, group, target, self.__log_name__, args, kwargs, daemon=daemon)
        self.verbose = verbose

    def rename_logger(self, name=None, use_class_name=True):
        LoggedObject.rename_logger(self, name, use_class_name=self.__class__.__name__ != 'LoggedThread')
        if self._initialized:
            self.setName(self.__log_name__)

    def run(self) -> None:
        try:
            if self.verbose:
                self.logger.info("Started")
            self.logged_run()
            if self.verbose:
                self.logger.info("Ended")
        except Exception as e:
            self.logger.exception("Exception in thread: %s", e)

    def logged_run(self) -> None:
        threading.Thread.run(self)

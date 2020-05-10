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
import logging
import subprocess
from multiprocessing import Event
from typing import Optional, Union, Iterable

from cloudexp.guest.application.control import DynamicResourceControl

from cloudexp.util.logs import OutputLogThread, get_verbosity_level

from mom.logged_object import LoggedObject
from mom.util.terminable import Terminable


########################################################################################################################
# Application
########################################################################################################################

def run_app_with_args(args, **kwargs):
    return subprocess.Popen(args, encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs)


class Application(LoggedObject, Terminable):
    """
    Guest side functions
    Used by the guest remote_prog.py script to control the program
    """
    def __init__(self, port=None, wait_timeout=60, decrease_mem_time=0, dynamic=True,
                 stdout_log_level=logging.INFO, stderr_log_level=logging.ERROR):
        LoggedObject.__init__(self)
        Terminable.__init__(self)

        self.guest_server_port = port
        self.wait_timeout = wait_timeout
        self.decrease_mem_time = decrease_mem_time
        self.dynamic = dynamic

        self.stdout_log_level = get_verbosity_level(stdout_log_level)
        self.stderr_log_level = get_verbosity_level(stderr_log_level)

        self._app_proc: Optional[subprocess.Popen] = None
        self._resource_control_thread: Optional[DynamicResourceControl] = None

    def set_guest_server_port(self, port=None):
        self.guest_server_port = port

    @property
    def is_alive(self):
        return (self._app_proc is not None) and (self._app_proc.poll() is None)

    @property
    def pid(self):
        if self._app_proc is not None:
            return self._app_proc.pid

    def run(self):
        if self.is_alive:
            return
        self.terminate_on_signal()
        if self.dynamic:
            self._resource_control_thread = self.start_resource_control()
            if self._resource_control_thread is not None:
                self._resource_control_thread.start()
        while self.should_run:
            self._app_proc = self.start_application()
            if self._app_proc is None:
                return
            stdout = getattr(self._app_proc, 'stdout', None)
            stderr = getattr(self._app_proc, 'stderr', None)
            if stdout is not None:
                OutputLogThread(stdout, name=f'{self.__log_name__}-stdout', log_level=self.stdout_log_level).start()
            if stderr is not None:
                OutputLogThread(stderr, name=f'{self.__log_name__}-stderr', log_level=self.stderr_log_level).start()
            self._app_proc.wait()
            out, err = self._app_proc.communicate()
            if out:
                self.logger.info(out)
            if err:
                self.logger.error(err)

    def restart_application(self):
        self.terminate_application()

    def terminate(self):
        Terminable.terminate(self)
        self.terminate_application()
        if self._resource_control_thread is not None:
            self._resource_control_thread.terminate()

    def start_application(self):
        """ Start the program """
        raise NotImplementedError

    def start_resource_control(self) -> Optional['DynamicResourceControl']:
        """ Start resource control thread """
        raise NotImplementedError

    def terminate_application(self):
        """ Terminate the program """
        raise NotImplementedError

    @staticmethod
    def get_image_name() -> str:
        return "generic-master.qcow2"

    @staticmethod
    def get_bin_path() -> Union[None, str, Iterable[str]]:
        return None


class NoApplication(Application):
    def __init__(self, dynamic=False):
        Application.__init__(self, dynamic=dynamic)
        self.q = None
        self._is_alive = False

    @property
    def is_alive(self):
        return self._is_alive

    def start_application(self):
        if self.q is None:
            self.q = Event()
        self.q.clear()
        self._is_alive = True
        self.q.wait()
        self._is_alive = False

    def start_resource_control(self):
        return None

    def terminate_application(self):
        if self.q is None:
            return
        self.q.set()


class NoApplicationDynamic(NoApplication):
    def __init__(self):
        NoApplication.__init__(self, dynamic=True)

    def start_resource_control(self):
        return DynamicResourceControl(self.guest_server_port, self.wait_timeout, self.decrease_mem_time,
                                      change_mem_func=self.change_mem_func, shared_terminable=self)

    def change_mem_func(self, mem_total, mem_usage, mem_cache_and_buff, app_rss):
        pass

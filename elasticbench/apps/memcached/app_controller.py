"""
Author: Liran Funaro <liran.funaro@gmail.com>
Based on code by Eyal Posner

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
import os
import signal

from cloudexp.guest.application import Application, run_app_with_args
from elasticbench.apps.memcached import memcached_port
from elasticbench.apps.memcached.mem_control import MemcachedMemControl

from mom.util.parsers import parameter_int, parameter_bool


class Memcached(Application):
    def __init__(self, init_mem_size, spare_mem, wait_timeout=60, decrease_mem_time=6, dynamic=True, elastic=True):
        Application.__init__(self, wait_timeout=wait_timeout, decrease_mem_time=decrease_mem_time, dynamic=dynamic)
        self.init_mem_size = parameter_int(init_mem_size)
        self.spare_mem = parameter_int(spare_mem)
        self.elastic = parameter_bool(elastic)

    @staticmethod
    def get_image_name(*_args, **_kwargs):
        return "generic-master.qcow2"

    @staticmethod
    def get_elastic_args():
        return "-o", "slab_reassign", "-o", "slab_automove=2"

    @property
    def memcached_cmd(self):
        memcahced_folder = 'memcached-dynamic' if self.elastic else 'memcached'
        return os.path.join(os.path.expanduser("~"), memcahced_folder, 'memcached')

    def start_application(self):
        # Documentation: https://linux.die.net/man/1/memcached
        args = [
            self.memcached_cmd,
            "-p", str(memcached_port),      # -p <num>: Listen on TCP port <num>, the default is port 11211.
            "-u", "nobody",                 # -u <username>: Assume the identity of <username> (only when run as root).
            "-m", str(self.init_mem_size),  # -m <num>: Use <num> MB memory max to use for object storage;
                                            #           the default is 64 megabytes.
            "-r",                           # -r: Raise the core file size limit to the maximum allowable.
            # "-I", "1m",                     # -I <size>: Override the default size of each slab page.
            #                                 #            Default is 1m, minimum is 1k, max is 128m.
            #                                 #            Adjusting this value changes the item size limit.
            #                                 #            Beware that this also increases the number of slabs
            #                                 #            (use -v to view),
            #                                 #            and the overall memory usage of memcached.
        ]

        if self.elastic:
            args.extend(self.get_elastic_args())

        return run_app_with_args(args)

    def start_resource_control(self):
        """ Start resource control thread """
        return MemcachedMemControl(self, self.guest_server_port, self.wait_timeout, self.decrease_mem_time,
                                   self.spare_mem, elastic=self.elastic, shared_terminable=self)

    def terminate_application(self):
        try:
            self._app_proc.send_signal(signal.SIGINT)
            self._app_proc.kill()
        except OSError as ex:
            self.logger.error("couldn't kill memcached pid: %i, reason: %s", self._app_proc.pid, ex)

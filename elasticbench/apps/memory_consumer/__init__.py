"""
Author: Eyal Posener, Orna Agmon Ben Yehuda, Liran Funaro <liran.funaro@gmail.com>
Technion - Israel Institute of Technology

Copyright (C) 2006-2019 Liran Funaro

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


MemoryConsumer is a program which its performance depends on the
total memory and the load of the program.

spare_mem: amount of memory to leave untouched: memory size
    will be total_memory - spare_memory.
saturation_mem: saturation point in MB which above additional
    memory won't increase the performance
max_mem: The maximal memory to allocate (defaults to double the saturation_mem)
wait_timeout: time constant in seconds waiting for an update on the
    load and memory allocation of the program.
sleep_after_write: amount of time in seconds that each memory
    writing thread will sleep after writing 1MB of memory.

The program is composed of a server on port [MemoryConsumerServer.port]
that can get load and return the current performance of the program.
The program is constantly controlling an array of 1MB objects such that
it will occupy (total_memory - spare_mem) of the machine available
memory.
According to the current load, the program create threads which
constantly writing to the memory. this causes the performance to
increase as the load increases.
writing to the memory is done by picking a random number between 0 and
saturation point, and if the number is with the 1MB object limit, the
program is writing the object to the memory and increasing a counter.
This causes the performance to increase as the available memory
increases up to the point it reaches (saturation - spare_mem).
performance is calculated each time the method get_perf is called by the
amount of MBs written divided by the elapsed time - kind of throughput.
"""
import os
import time

from elasticbench import settings
from elasticbench.apps.memory_consumer.process_backend import MemoryConsumerProcessBackend
from elasticbench.apps.memory_consumer.python_backend import MemoryConsumerPythonBackend

from cloudexp.guest.application import Application, DynamicResourceControl
from mom.util.memory_type import InvalidMemory, is_memory_close

from mom.util.parsers import parameter_int, parameter_float
from mom.communication.pickle_socket_server import PickleTcpThreadedServer


DEFAULT_PORT = 1938


class MemoryConsumer(Application):
    def __init__(self, saturation_mem, wait_timeout, sleep_after_write, max_mem=None, spare_mem=100, backend='cpp'):
        Application.__init__(self, wait_timeout=wait_timeout, decrease_mem_time=3, dynamic=True)
        self.spare_mem = parameter_int(spare_mem)
        self.saturation_mem = parameter_int(saturation_mem)
        self.sleep_after_write = parameter_float(sleep_after_write)
        self.max_mem = parameter_int(max_mem, 2 * saturation_mem)
        self.backend_name = backend
        if self.saturation_mem <= self.spare_mem:
            raise ValueError("Saturation must be greater then spare memory!")

        self._last_target = InvalidMemory
        self.min_update_diff = 15

        self.server = None
        if self.backend_name == 'python':
            self.backend = MemoryConsumerPythonBackend(self.max_mem, self.sleep_after_write)
        elif self.backend_name in ('java', 'cpp'):
            self.backend = MemoryConsumerProcessBackend(self.max_mem, self.sleep_after_write, self.backend_name)
        else:
            raise ValueError(f"Unknown backend: '{self.backend_name}'")

    @staticmethod
    def get_image_name(*_args, **_kwargs):
        return "generic-master.qcow2"

    def start_application(self):
        self.backend.start_backend()
        self.backend.set_max_rand(self.saturation_mem - self.spare_mem)
        self.backend.reset_perf()

        self.server = PickleTcpThreadedServer("", DEFAULT_PORT, message_handler=self.process_message)
        self.server.serve_forever()
        return self

    def start_resource_control(self):
        """ Start resource control thread """
        return DynamicResourceControl(self.guest_server_port, self.wait_timeout, self.decrease_mem_time,
                                      spare_mem=self.spare_mem, change_mem_func=self.change_mem_func,
                                      application_rss_func=self.application_rss, shared_terminable=self)

    def terminate_application(self):
        # closing controlling threads
        self.logger.debug("Shutting down server")
        if self.server is not None:
            self.server.shutdown()
        # kill all workers and free memory
        self.logger.debug("Terminating backend")
        self.backend.terminate_backend()
        self.logger.info("Ended")

    def poll(self):
        if self.backend is None:
            return None
        return self.backend.poll()

    def communicate(self):
        if self.backend is None:
            return None, None
        return self.backend.communicate()

    def process_message(self, msg):
        """ Get load as msg and return performance of program. """
        load = 0
        duration = 0
        try:
            load = msg["load"]
            duration = msg["duration"]
        except Exception as e:
            self.logger.exception("While parsing load '%s': %s", msg, e)

        self.backend.set_load(load)
        self.backend.reset_perf()  # to reset the performance
        if duration > 1e-2:
            time.sleep(duration)
        return self.backend.get_perf()

    def application_rss(self):
        return self.backend.memory()

    def change_mem_func(self, mem_total, mem_usage, mem_cache_and_buff, app_rss):
        spare = mem_usage - mem_cache_and_buff - app_rss + self.spare_mem
        if spare < 0:
            self.logger.error("Spare memory should be grater than 0. But spare=%s.", spare)
        target = int(max(0, mem_total - spare))

        # update max_rand if no allocation yet
        if app_rss == 0:
            max_rand = self.saturation_mem - self.spare_mem - mem_usage
            if max_rand < 0:
                self.logger.error("Max rand must be grater than 0. Got %s. Setting to zero.", max_rand)
                max_rand = 0
            self.backend.set_max_rand(max_rand)
            self.logger.info("Setting max rand to: %i", max_rand)

        if target > self.max_mem:
            self.logger.warning("Target must be less than %s. Got %s. Setting to %s.",
                                self.max_mem, target, self.max_mem)
            target = self.max_mem

        if is_memory_close(target, self._last_target, self.min_update_diff):
            self.logger.debug("Insignificant memory target change: from %.2f to %.2f.", self._last_target, target)
        else:
            self.backend.set_memory(target)
            self._last_target = target

        return self._last_target

    def get_bin_path(self):
        backend_sub_path = self.backend.get_bin_sub_path()
        if backend_sub_path is None:
            return None
        return os.path.join(settings.read_applications_bin(), *backend_sub_path)

"""
Author: Liran Funaro <liran.funaro@gmail.com>

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
"""
import logging
import random
import subprocess
import threading
from typing import Optional, Any

from cloudexp.util.logs import OutputLogThread
from cloudexp.util.timeformat import time_delta
from mom.util import parsers
from mom.logged_object import LoggedObject, LoggedThread


class ThreadSafeDict:
    def __init__(self):
        self._data = {}
        self._events = {}
        self._event_lock = threading.RLock()

    def get_event(self, item):
        with self._event_lock:
            e = self._events.get(item)
            if e is None:
                e = threading.Event()
                e.clear()
                self._events[item] = e
            return e

    def pop(self, item, block=True, timeout=None):
        e = self.get_event(item)
        if block:
            e.wait(timeout)
        if e.is_set():
            return self._data.get(item)

    def put(self, item, value):
        e = self.get_event(item)
        self._data[item] = value
        e.set()


class MemoryConsumerOutputParseThread(LoggedThread):
    def __init__(self, stream, name=None):
        LoggedThread.__init__(self, name=name, daemon=True)
        self.stream = stream
        self.q = ThreadSafeDict()

    @staticmethod
    def _parse_perf(perf_line):
        memory = parsers.parse_int(r'memory\s*:\s*(\d+)', perf_line)
        load = parsers.parse_int(r'load\s*:\s*(\d+)', perf_line)
        hit_rate = parsers.parse_float(r'hit-rate\s*:\s*([\d\.]+)', perf_line)
        throughput = parsers.parse_float(r'throughput\s*:\s*([\d\.]+)', perf_line)
        duration = parsers.parse_float(r'duration\s*:\s*([\d\.]+)', perf_line)
        uuid = parsers.parse_int(r'UUID\s*:\s*([\d]+)', perf_line)
        if any((e is None) for e in (memory, load, hit_rate, throughput, duration, uuid)):
            return None
        return {
            "memory": memory,
            "load": load,
            "hit-rate": hit_rate,
            "throughput": throughput,
            "duration": duration,
            "UUID": uuid,
        }

    def logged_run(self) -> None:
        for perf_line in iter(self.stream.readline, ''):
            perf = self._parse_perf(perf_line)
            if perf is not None:
                self.q.put(perf['UUID'], perf)
            else:
                self.logger.error("Could not parse output: %s", perf_line)


###################################################################################################################
# Process (c++/java) backend
###################################################################################################################

class MemoryConsumerProcessBackend(LoggedObject):
    def __init__(self, max_memory, sleep_after_write, backend='cpp'):
        LoggedObject.__init__(self)
        self.max_memory = max_memory
        self.sleep_after_write = sleep_after_write
        self.backend = backend.lower()

        self._proc: Optional[subprocess.Popen] = None
        self._qthread = None
        self._command_lock = None

    def get_bin_sub_path(self):
        if self.backend == 'cpp':
            return 'memory-consumer-cpp', 'bin'
        elif self.backend == 'java':
            return 'memory-consumer-java', 'jar'
        else:
            raise ValueError(f"Unknown backend: '{self.backend}'")

    def start_backend(self):
        self._command_lock = threading.RLock()
        if self.backend == 'cpp':
            args = ['bin/memory_consumer']
        elif self.backend == 'java':
            args = ['java', '-jar', 'jar/memory_consumer.jar']
        else:
            raise ValueError(f"Unknown backend: '{self.backend}'")
        parameters = str(self.max_memory), str(self.sleep_after_write)
        args.extend(parameters)
        self._proc = subprocess.Popen(args, encoding='utf-8', stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE)
        self._qthread = MemoryConsumerOutputParseThread(self._proc.stdout)
        self._qthread.start()
        OutputLogThread(self._proc.stderr, name=self.__log_name__, log_level=logging.ERROR).start()

    def terminate_backend(self):
        self._command('quit')
        try:
            self._proc.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            self._proc.kill()
            self._proc.communicate()

    def poll(self):
        if self._proc is None:
            return None
        return self._proc.poll()

    def communicate(self):
        if self._proc is None:
            return None, None
        else:
            return self._proc.communicate()

    def _command(self, command: str, param: Optional[Any] = None):
        if param is None:
            cmd_string = f'{command}\n'
        else:
            cmd_string = f'{command}: {param}\n'

        with self._command_lock:
            self._proc.stdin.write(cmd_string)
            self._proc.stdin.flush()

    def _read_perf_from_queue(self, uuid, timeout=1):
        ret = self._qthread.q.pop(uuid, True, timeout)
        if ret is None:
            self.logger.error("'perf' did not respond after %s", time_delta(timeout))
            return ret

        if 'UUID' in ret:
            del ret['UUID']
        return ret

    def get_perf(self):
        """ calculate performance """
        uuid = random.randrange(0, 1 << 32)
        with self._command_lock:
            self._command('perf', uuid)
            return self._read_perf_from_queue(uuid, 1)

    def reset_perf(self):
        self._command('resetperf')

    def set_max_rand(self, max_rand: int):
        self._command('maxrand', max_rand)

    def set_memory(self, target_memory: int):
        """ change memory array size """
        prev_memory = self.memory()
        if prev_memory != target_memory:
            self.logger.debug("Changing memory from %s to %s", prev_memory, target_memory)
        self._command('memory', target_memory)

    def set_load(self, target_load: int):
        """ Change number of workers according to load """
        target_load = max(0, target_load)  # load must be not-negative
        prev_load = self.load()
        if target_load != prev_load:
            self.logger.debug("Changing load from %s to %s", prev_load, target_load)
        self._command('load', target_load)

    def load(self):
        perf = self.get_perf()
        return perf['load']

    def memory(self):
        perf = self.get_perf()
        return perf['memory']

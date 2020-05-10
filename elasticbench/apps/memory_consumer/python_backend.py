"""
Author: Liran Funaro <liran.funaro@gmail.com>
Original Author: Eyal

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
import random
import threading
import time
from typing import List, Optional

import gc
import numpy as np

from mom.logged_object import LoggedObject, LoggedThread
from mom.util.terminable import Terminable


###################################################################################################################
# Python backend
###################################################################################################################


class MemoryConsumerPythonBackend(LoggedObject):
    def __init__(self, max_memory, sleep_after_write):
        LoggedObject.__init__(self)
        self.max_memory = max_memory
        self.sleep_after_write = sleep_after_write

        self.measure_start = None
        self.hits = AtomicCounter()
        self.throughput = AtomicCounter()

        self.max_rand = 0
        self.mem_arr: List[np.array] = []
        self.workers: List[MemoryConsumerWorker] = []

        self.change_load_lock = None

    @staticmethod
    def get_bin_sub_path():
        return None

    def start_backend(self):
        self.change_load_lock = threading.RLock()

    def terminate_backend(self):
        self.set_load(0)
        self.set_memory(0)

    @staticmethod
    def poll():
        return None

    @staticmethod
    def communicate():
        return None, None

    def set_max_rand(self, max_rand):
        self.max_rand = max_rand

    def set_memory(self, target):
        """ change memory array size """
        top_before = self.memory()
        sample_size = int(2 ** 20 / (np.finfo(float).bits / 8))

        # add 1MB objects to end
        try:
            while self.memory() < target:
                self.mem_arr.append(np.random.random_sample(sample_size))
        except Exception as e:
            self.logger.exception("Error allocating. current: %i, target: %i, Error: %s",
                                  len(self.mem_arr), target, e)

        # remove and delete the objects from the beginning
        while self.memory() > target:
            self.mem_arr.pop(-1)
        gc.collect()

        if top_before != self.memory():
            self.logger.debug("Memory changed from %s to %s (target=%s)", top_before, self.memory(), target)

    def get_perf(self):
        """ calculate performance """
        duration = time.time() - self.measure_start
        hit_rate = 0
        throughput = 0
        if duration > 1e-6:
            hit_rate = self.hits.value / duration
            throughput = self.throughput.value / duration
        return {
            "memory": self.memory(),
            "max-rand": self.max_rand,
            "load": self.load(),
            "hit-rate": hit_rate,
            "throughput": throughput,
            "duration": duration,
        }

    def reset_perf(self):
        self.measure_start = time.time()
        self.hits.reset()
        self.throughput.reset()

    def set_load(self, load):
        """ Change number of workers according to load """
        load = max(0, load)  # load must be not-negative
        change = load != self.load()

        with self.change_load_lock:
            # add workers
            while load > self.load():
                try:
                    w = MemoryConsumerWorker(self)
                    w.start()
                    self.workers.append(w)
                except Exception as ex:
                    self.logger.exception("While starting worker: %s, current load: %i", ex, self.load)
                    break
            # remove workers
            # first terminate needed amount workers and then wait for
            # terminated workers to finish
            terminated = []
            while load < self.load():
                w = self.workers.pop()
                w.terminate()
                terminated.append(w)
            for w in terminated:
                w.join()

        if change:
            self.logger.debug("Load changed to: %i", self.load)

    def load(self):
        return len(self.workers)

    def memory(self):
        return len(self.mem_arr)


class MemoryConsumerWorker(LoggedThread, Terminable):

    def __init__(self, owner: MemoryConsumerPythonBackend, shared_terminable: Optional[Terminable] = None):
        LoggedThread.__init__(self)
        Terminable.__init__(self, shared_terminable=shared_terminable)
        self.owner = owner
        self.sleep_duration = owner.sleep_after_write
        # set a worker own random number generator
        self.random = random.Random(threading.get_ident())

    def run(self):
        while self.should_run:
            self.write_random_cell(self.random)
            self.terminable_sleep(self.sleep_duration)

    def write_random_cell(self, random_generator):
        """ write 1MB into random cell """
        # generate random index
        i = random_generator.randrange(0, max(self.owner.max_rand, self.owner.memory()))
        # increment throughput
        self.owner.throughput.increment()
        # check for index validity
        if i >= self.owner.memory():
            return
        # write to cell
        np.random.shuffle(self.owner.mem_arr[i])
        # if write succeed increment the hits
        self.owner.hits.increment()


class AtomicCounter:
    def __init__(self, initial=0):
        self._value = initial
        self._lock = threading.RLock()

    @property
    def value(self):
        with self._lock:
            return self._value

    def increment(self, num=1):
        with self._lock:
            self._value += num
            return self._value

    def reset(self, initial=0):
        with self._lock:
            self._value = initial

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['_lock']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._lock = threading.RLock()

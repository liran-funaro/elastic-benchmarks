import multiprocessing
import os
import signal
import threading
import time
from typing import Set

import gc

from mom.logged_object import LoggedObject
from mom.util.terminable import Terminable


class AliveThreadMonitor(LoggedObject, Terminable):
    def __init__(self, interval, timeout=60):
        LoggedObject.__init__(self)
        Terminable.__init__(self)
        self.interval = interval
        self.timeout = timeout

    @staticmethod
    def default_threads():
        return {threading.current_thread().getName(), threading.main_thread().getName()}

    @classmethod
    def filter_threads(cls, thread_set: Set[str]):
        thread_set = thread_set - cls.default_threads()
        return set(filter(lambda t: 'dummy' not in t.lower(), thread_set))

    @staticmethod
    def get_alive_threads():
        return {t.getName() for t in threading.enumerate()}

    @classmethod
    def get_real_alive_threads(cls):
        return cls.filter_threads(cls.get_alive_threads())

    def run(self) -> None:
        alive_threads_set = set()

        begin_time = time.time()
        end_time = begin_time + self.timeout
        while self.should_run and time.time() < end_time:
            gc.collect()
            self.terminable_sleep(self.interval)
            gc.collect()
            new_alive_threads_set = self.get_real_alive_threads()

            if alive_threads_set != new_alive_threads_set:
                alive_threads_set = new_alive_threads_set
                if len(alive_threads_set) > 0:
                    self.logger.debug("Alive: %s", ", ".join(alive_threads_set))

            if len(alive_threads_set) == 0:
                self.logger.info("All background threads were terminated...")
                return

        # If after timeout the we still have threads, signal the process
        os.kill(multiprocessing.current_process().pid, signal.SIGINT)
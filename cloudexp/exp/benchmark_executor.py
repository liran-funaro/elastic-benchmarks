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
import time
import threading
from typing import Optional, Iterator

from cloudexp.util.timeformat import time_delta
from mom.util.data_logger import DataLogger
from mom.util.terminable import Terminable, DeferredStartThread


class BenchmarkExecutor(DeferredStartThread):
    def __init__(self, benchmark, load_function, default_load_duration, expected_load_rounds=None, name=None,
                 shared_terminable: Terminable = None, shared_deferred_start: Optional[DeferredStartThread] = None):
        DeferredStartThread.__init__(self, name, shared_deferred_start=shared_deferred_start,
                                     shared_terminable=shared_terminable)
        self.benchmark = benchmark
        self.load_function = load_function
        self.default_load_duration = default_load_duration

        self.expected_load_rounds = expected_load_rounds
        self.actual_load_rounds = 0

        self.data_logger = DataLogger('performance', name)

        self.end_event = threading.Event()

    def get_load(self):
        load = self.load_function(time.time() - self.start_time)
        if type(load) in (tuple, list):
            return load
        else:
            return load, self.default_load_duration

    def logged_run(self) -> None:
        if self.benchmark is None or self.load_function is None:
            self.logger.warning("No benchmark or load function")
            return

        try:
            while self.should_run:
                self.actual_load_rounds += 1

                load, duration = self.get_load()

                if load is None:
                    self.terminable_sleep(duration)
                    continue

                try:
                    self._apply_load(load, duration)
                except Exception as e:
                    if self.should_run:
                        raise e
        finally:
            self.end_event.set()

    def _apply_load(self, load, duration):
        """ blocking """
        self.logger.debug("Applying load %i for %s", load, time_delta(duration))

        # actually applying load:
        benchmark_start = time.time()
        perf = self.benchmark.run_benchmark(load, duration)  # blocking or generator
        benchmark_end = time.time()
        is_iterator = isinstance(perf, Iterator)

        actual_duration = benchmark_end - benchmark_start
        if not is_iterator and actual_duration + 1e-2 < duration and self.should_run:
            self.logger.warning("Benchmark finished before its time: %s instead of %s",
                                time_delta(actual_duration), time_delta(duration))

        if not perf:
            # No results
            if self.should_run:
                self.logger.warning("Benchmark finished without results.")
        elif not is_iterator:
            # Regular results
            self.log_sample(benchmark_start, benchmark_end, load, perf)
        else:
            last_offset = 0
            # Continually generated results
            for time_offset, perf_sample in perf:
                self.log_sample(benchmark_start + last_offset + 1e-2, benchmark_start + time_offset, load, perf_sample)
                last_offset = time_offset

    def log_sample(self, start, end, load, perf_sample):
        if not isinstance(perf_sample, dict):
            self.logger.error("Performance sample must be a dict. Instead got: %s", type(perf_sample))
        self.data_logger.append_data(dict(perf=perf_sample, load=load), start, end)

    def terminate(self, force=True):
        if self.benchmark is None:
            DeferredStartThread.terminate(self)
            return

        if self.expected_load_rounds is not None:
            if self.actual_load_rounds < self.expected_load_rounds:
                self.logger.info("Waiting for benchmark to finish at-least %s round(s)", self.expected_load_rounds)
            while self.actual_load_rounds < self.expected_load_rounds:
                self.end_event.wait(self.default_load_duration * 0.01)

        DeferredStartThread.terminate(self)
        if not force:
            self.end_event.wait(self.default_load_duration * 0.1)

        self.logger.info("Terminating benchmark after %s load rounds", self.actual_load_rounds)
        self.benchmark.terminate()

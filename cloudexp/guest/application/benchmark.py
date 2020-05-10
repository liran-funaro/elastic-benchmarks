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
import time
from multiprocessing import Event
from typing import Union, Dict, Any, Iterator, Tuple, Optional

from cloudexp.util import shell
from cloudexp.util.timeformat import time_delta
from cloudexp.drivers.guest_machine import GuestMachine

from mom.logged_object import LoggedObject
from mom.util.terminable import Terminable


BenchmarkLoad = Union[int, float]
BenchmarkDuration = Union[int, float]
BenchmarkResults = Union[Dict[str, Any], Iterator[Tuple[BenchmarkDuration, Dict[str, Any]]]]


class BenchmarkError(Exception):
    pass


class Benchmark(LoggedObject, Terminable):
    def __init__(self, is_remote=False, name=None):
        LoggedObject.__init__(self, name)
        Terminable.__init__(self)
        self.is_remote = is_remote
        self.guest_machine: Optional[GuestMachine] = None
        self._running_process = set()
        self._alloc_cpus = None

    def set_guest_machine(self, guest_machine: GuestMachine):
        self.guest_machine = guest_machine
        self.rename_logger(guest_machine.vm_name)
        self.share_termination(guest_machine)

    @property
    def application_ip(self):
        if self.guest_machine is None:
            return None
        return self.guest_machine.ip

    def consume(self, load: BenchmarkLoad, duration: BenchmarkDuration) -> BenchmarkResults:
        """
        blocking
        consumes load for a duration running benchmark on CPU number cpu_pin in the range: [0,N-1]
        return a dict of performance.
        raises BenchmarkError when benchmark fails
        """
        raise NotImplementedError

    def get_required_cpus(self):
        """ If the benchmark needs more than 1 CPU, it should override this method """
        return 0 if self.is_remote else 1

    def _wait_for_command_response(self, process_object: shell.RunProcess, timeout=None, raise_stderr=False):
        output, error = process_object.communicate(timeout=timeout)
        ret_code = process_object.poll()
        killed = False
        if ret_code is None:
            killed = True
            process_object.kill()
            ret_code = process_object.poll()
        self._running_process.remove(process_object)

        if killed:
            raise BenchmarkError(f"Benchmark command killed and finished with error-code '{ret_code}' "
                                 f"after {time_delta(timeout)}.%s%s" % (
                                     f'\n[STDOUT]\n{output}' if output else '',
                                     f'\n[STDERR]\n{error}' if error else '',
                                 ))
        elif ret_code != 0:
            raise BenchmarkError(f"Benchmark command failed with error code: {ret_code}.%s%s" % (
                f'\n[STDOUT]\n{output}' if output else '',
                f'\n[STDERR]\n{error}' if error else '',
            ))
        elif raise_stderr and error:
            raise BenchmarkError(f"Benchmark command failed with error: {error}.%s" % (
                f'\n[STDOUT]\n{output}' if output else ''
            ))
        if raise_stderr:
            return output
        else:
            return output, error

    def async_popen(self, cmd, **kwargs):
        """ Execute command locally and stream the output (optional timeout, cpu pinning) """
        if self._alloc_cpus is not None:
            if not isinstance(self._alloc_cpus, list):
                self._alloc_cpus = [self._alloc_cpus]

            cpus_bit_vector = 0

            for cpu in self._alloc_cpus:
                cpus_bit_vector |= (1 << int(cpu))

            cmd = ["taskset", "-a", hex(cpus_bit_vector), *cmd]

        p = shell.RunProcess(cmd, name=self.__log_name__, **kwargs)
        self._running_process.add(p)
        return p

    def async_ssh(self, cmd, **kwargs):
        """ Execute command on guest machine and stream the output (optional timeout) """
        if self.guest_machine is None:
            raise BenchmarkError("Cannot SSH to guest machine. No GuestMachine object supplied.")

        p = self.guest_machine.ssh(cmd, name=self.__log_name__, **kwargs)
        self._running_process.add(p)
        return p

    def async_rsync(self, source, destination, **kwargs):
        """ Rsync to guest machine and stream the output (optional timeout) """
        if self.guest_machine is None:
            raise BenchmarkError("Cannot rsync to guest machine. No GuestMachine object supplied.")

        p = self.guest_machine.rsync(source, destination, name=self.__log_name__, **kwargs)
        self._running_process.add(p)
        return p

    def popen(self, cmd, timeout=None, raise_stderr=False, **kwargs):
        p = self.async_popen(cmd, **kwargs)
        return self._wait_for_command_response(p, timeout, raise_stderr=raise_stderr)

    def ssh(self, cmd, timeout=None, raise_stderr=False, **kwargs):
        p = self.async_ssh(cmd, **kwargs)
        return self._wait_for_command_response(p, timeout, raise_stderr=raise_stderr)

    def rsync(self, source, destination, timeout=None, raise_stderr=False, **kwargs):
        p = self.async_rsync(source, destination, **kwargs)
        return self._wait_for_command_response(p, timeout, raise_stderr=raise_stderr)

    def async_run_command(self, args, verbose=True):
        if self.is_remote:
            return self.async_ssh(args, verbose=verbose)
        else:
            return self.async_popen(args, verbose=verbose)

    def run_command(self, args, timeout=None, verbose=True, **kwargs):
        if self.is_remote:
            return self.ssh(args, timeout=timeout, verbose=verbose, **kwargs)
        else:
            return self.popen(args, timeout=timeout, verbose=verbose, **kwargs)

    def test(self, ignore_errors=False):
        """
        Test if the program is alive.
        just a very short and light benchmark
        @return: boolean: True if test passed, else False
        """
        try:
            perf = self.consume(1, 1)
            if isinstance(perf, Iterator):
                for _ in perf:
                    pass
            return True
        except BenchmarkError as e:
            if not ignore_errors:
                self.logger.exception("Benchmark error: %s", e)
        except Exception as e:
            self.logger.exception("Error while testing benchmark: %s", e)
            return False

    def wait_for_application(self, interval=10, max_retries=50):
        count = 0
        init_time = time.time()
        self.logger.debug("Testing if application is alive")
        while self.should_run:
            begin_time = time.time()
            if self.test(ignore_errors=count < (max_retries / 2)):
                return

            count += 1
            if count >= max_retries:
                total_time = time.time() - init_time
                raise Exception(f"Application did not respond after {count} attempts ({time_delta(total_time)} sec.)")

            self.terminable_sleep(interval - (time.time() - begin_time))

    def set_benchmark_cpus(self, alloc_cpus=None):
        self.logger.info("Benchmark using CPUs: %s", alloc_cpus)
        self._alloc_cpus = alloc_cpus

    def get_benchmark_cpus(self):
        return self._alloc_cpus

    def run_benchmark(self, load: Union[int, float], duration: Union[int, float]) -> BenchmarkResults:
        """ Run the benchmark with a 'load' for a 'duration' """
        try:
            return self.consume(load, duration)
        except BenchmarkError as er:
            if self.should_run:
                self.logger.error("Error in benchmark (load:%i, duration:%i): %s", load, duration, er)
            return {}

    def terminate(self):
        Terminable.terminate(self)
        for p in list(self._running_process):
            try:
                p.kill()
            except OSError as e:
                self.logger.error('Failed to terminate benchmark: %s', e)
            self._running_process.remove(p)


class NoBenchmark(Benchmark):
    def __init__(self):
        Benchmark.__init__(self)
        self.q = None

    def consume(self, load: Union[int, float], duration: Union[int, float]) -> BenchmarkResults:
        if self.q is None:
            self.q = Event()
        self.q.clear()
        self.q.wait(duration)
        return {'fake': 0}

    def terminate(self):
        Benchmark.terminate(self)
        if self.q is None:
            return
        self.q.set()
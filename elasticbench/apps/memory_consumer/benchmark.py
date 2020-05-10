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
"""
import time

from cloudexp.guest.application.benchmark import BenchmarkError, BenchmarkLoad, BenchmarkDuration, BenchmarkResults, \
    Benchmark
from elasticbench.apps.memory_consumer import DEFAULT_PORT
from mom.communication.pickle_socket_server import PickleTcpClient


class MemoryConsumerBenchmark(Benchmark):
    def __init__(self):
        Benchmark.__init__(self)
        self.mc_client = None

    def get_mc_client(self, timeout=None):
        if self.mc_client is None:
            self.mc_client = PickleTcpClient(self.application_ip, DEFAULT_PORT, timeout=timeout,
                                             base_name="MC-Benchmark")
        self.mc_client.set_timeout(timeout)
        return self.mc_client

    def consume(self, load: BenchmarkLoad, duration: BenchmarkDuration) -> BenchmarkResults:
        client = self.get_mc_client(duration * 2 if duration > 1 else 100)
        start = time.time()
        try:
            msg = client.send_recv({'load': load, 'duration': duration})
        except Exception as err:
            raise BenchmarkError("Error in communication: %s" % err)

        self.terminable_sleep(duration - (time.time() - start))
        return msg

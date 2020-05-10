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
import re
import signal

from cloudexp.guest.application import Application, run_app_with_args
from cloudexp.guest.application.benchmark import BenchmarkError, BenchmarkLoad, BenchmarkDuration, BenchmarkResults, \
    Benchmark

result_pattern = re.compile(r'\s*\[\s*(SUM|\d+)\s*\]\s+[.\d\s-]+\s+[a-z]+\s+([\d.]+)\s+MBytes\s+([\d.]+)\s+MBytes/sec',
                            re.I | re.M)


class IPerf(Application):
    def __init__(self):
        Application.__init__(self, dynamic=False)

    @staticmethod
    def get_image_name(*_args, **_kwargs):
        return "generic-master.qcow2"

    def start_application(self):
        # Documentation: ?
        args = [
            'iperf',
            '--server',       # -s, --server: run in server mode
            '--format', 'M',  # -f, --format [kmgKMG]: format to report: Kbits, Mbits, KBytes, MBytes
        ]
        return run_app_with_args(args)

    def start_resource_control(self):
        """ Start resource control thread """
        return None

    def terminate_application(self):
        try:
            self._app_proc.send_signal(signal.SIGINT)
        except OSError as ex:
            self.logger.error("Couldn't kill iperf pid: %i, reason: %s", self._app_proc.pid, ex)


class IPerfBenchmark(Benchmark):

    def __init__(self, mode='remote'):
        Benchmark.__init__(self, is_remote=(mode == 'remote'))

    @property
    def iperf_address(self):
        if self.is_remote:
            return "localhost"
        else:
            return self.application_ip

    def consume(self, load: BenchmarkLoad, duration: BenchmarkDuration) -> BenchmarkResults:
        args = [
            'iperf',
            '--client', self.iperf_address,  # -c, --client <host>: run in client mode, connecting to <host>
            '--time', f"{duration}s",        # -t, --time #: time in seconds to transmit for (default 10 secs)
            '--format', 'M',                 # -f, --format [kmgKMG]: format to report: Kbits, Mbits, KBytes, MBytes
            '--parallel', load,              # -P, --parallel #: number of parallel client threads to run
        ]

        out = self.run_command(args, raise_stderr=True)  # blocking

        transfer, bandwidth = 0, 0
        match_list = result_pattern.findall(out)
        if len(match_list) == 0:
            raise BenchmarkError(f"No results found in benchmark output: {out}")
        elif len(match_list) == 1:
            m = match_list[0][1:]
        else:
            match_dict = {k: m for k, *m in match_list}
            m = match_dict['SUM']

        if m is not None:
            transfer, bandwidth = map(float, m)

        return dict(transfer=transfer, bandwidth=bandwidth)

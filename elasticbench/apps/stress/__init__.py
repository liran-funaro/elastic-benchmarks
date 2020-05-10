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
from cloudexp.guest.application import NoApplication
from cloudexp.guest.application.benchmark import BenchmarkLoad, BenchmarkDuration, BenchmarkResults, Benchmark
from mom.util.parsers import parameter_int_or_float

result_headers = 'ops', 'real-time', 'user-time', 'sys-time', 'real-time-ops/s', 'user+sys-time-ops/s'
result_pattern = re.compile(r'cpu\s+(\d+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s*$', re.I | re.M)


def parse_stress_ng(out: str):
    m = result_pattern.search(out)
    return dict(zip(result_headers, map(parameter_int_or_float, m.groups())))


class Stress(NoApplication):
    pass


class StressBenchmark(Benchmark):

    def __init__(self):
        Benchmark.__init__(self, is_remote=True)

    def consume(self, load: BenchmarkLoad, duration: BenchmarkDuration) -> BenchmarkResults:
        args = [
            'stress-ng',
            '--cpu', str(load),           # -c N, --cpu N: start N workers spinning on sqrt(rand())
            '--timeout', f"{duration}s",  # -t, --timeout N: timeout after N seconds
            # '--perf',                     # --perf:  display perf statistics
            '--metrics',                  # -M, --metrics: print pseudo metrics of activity
            '--log-brief',                # --log-brief          less verbose log messages
        ]

        out, err = self.run_command(args, verbose=False)  # blocking

        return parse_stress_ng(err)

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
import os

from cloudexp.guest.application.benchmark import BenchmarkLoad, BenchmarkDuration, BenchmarkResults, Benchmark
from elasticbench import settings
from elasticbench.apps.java_app import JavaApplication

########################################################################################################################
# dacapo --help
########################################################################################################################
# usage: DaCapo Benchmark suite
#  -c,--callback <callback>               Use class <callback> to bracket
#                                         benchmark runs
#  -C,--converge                          Allow benchmark times to converge
#                                         before timing
#  -d,--debug                             Verbose debugging information
#  -h,--help                              Print this help
#  -i,--information                       Display benchmark information
#     --ignore-validation                 Don't halt on validation failure
#  -k,--thread-factor <thread_per_cpu>    Set the number of threads per CPU
#                                         to drive the workload (mutually
#                                         exclusive with -t)
#  -l,--list-benchmarks                   List available benchmarks
#     --max-iterations <max_iterations>   Run a max of <max_iterations>
#                                         iterations (default 20)
#  -n,--iterations <iter>                 Run the benchmark <iter> times
#     --no-digest-output                  Turn off SHA1 digest of
#                                         stdout/stderr
#     --no-pre-iteration-gc               Skip performing System.gc() before
#                                         the start of each iteration
#     --no-validation                     Don't validate at all
#     --preserve                          Preserve output files (debug)
#  -r,--release-notes                     Print the release notes
#  -s,--size <SIZE>                       Size of input data
#     --scratch-directory <dir>           Specify an alternate scratch
#                                         directory <dir>
#  -t,--thread-count <thread_count>       Set the thread count to drive the
#                                         workload (mutually exclusive -k)
#  -v,--verbose                           Verbose output
#     --validation-report <report_file>   Report digests, line counts etc
#     --variance <pct>                    Target coefficient of variation
#                                         <pct> (default 3.0)
#     --window <window>                   Measure variance over <window>
#                                         runs (default 3)

# java Harness -l

BENCHMARKS = {'avrora', 'batik', 'eclipse', 'fop', 'h2', 'jython', 'luindex', 'lusearch', 'pmd', 'sunflow', 'tomcat',
              'tradebeans', 'tradesoap', 'xalan'}


# Main website: http://dacapobench.org/
# TODO: Need to write dacapo server
# TODO: See dacapo/jar for benchmarks app.
# TODO: See the following link for example on how to start a benchmark
#  https://github.com/dacapobench/dacapobench/blob/master/benchmarks/bms/lusearch/src/org/dacapo/lusearch/Search.java


class Dacapo(JavaApplication):
    def __init__(self, *args, **kwargs):
        JavaApplication.__init__(self, *args, **kwargs)

    @staticmethod
    def get_java_app_bin_path():
        return os.path.join(settings.read_applications_bin(), 'dacapo')


class DacapoBenchmark(Benchmark):
    def __init__(self, benchmark: str):
        if benchmark not in BENCHMARKS:
            raise ValueError(f"Benchmark '{benchmark}' does not exit.")
        self.benchmark = benchmark
        Benchmark.__init__(self, is_remote=True, name=benchmark)

    def consume(self, load: BenchmarkLoad, duration: BenchmarkDuration) -> BenchmarkResults:
        raise NotImplementedError


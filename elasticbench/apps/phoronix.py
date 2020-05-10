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
import re
import time
import ast
import numpy as np

from cloudexp.guest.application import NoApplication
from cloudexp.guest.application.benchmark import BenchmarkError, BenchmarkLoad, BenchmarkDuration, BenchmarkResults, \
    Benchmark


class PhoronixTestSuite(NoApplication):
    def __init__(self):
        NoApplication.__init__(self)

    @staticmethod
    def get_image_name(*args):
        return "phoronix-master.qcow2"


all_benchmarks = {
    # Didn't work:
    # "bork": dict(benchmark="bork"),

    # Failed to install:
    # ["povray","lammps", "hpcc", "hpcg"]

    # Streaming apps (uneffected by cache)
    "c-ray": dict(benchmark="c-ray", nice_name="C-Ray", short_name="C-Ray",
                  scale="Seconds"),
    "sqlite": dict(benchmark="sqlite", nice_name="SQLite", short_name="SQLite",
                   scale="Seconds"),
    "gcrypt": dict(benchmark="gcrypt", nice_name="Gcrypt (CAMELLIA256-ECB Cipher)", short_name="Gcrypt",
                   scale="Microseconds"),
    "openssl": dict(benchmark="openssl", nice_name="OpenSSL (RSA 4096-bit)", short_name="OpenSSL",
                    scale="Signs Per Second"),
    "scimark-monte": dict(benchmark="scimark2", nice_name="SciMark (Monte Carlo)", short_name="Monte-Carlo",
                          scale="Mflops",
                          options={"scimark2.compute-test": "3"}),
    "bullet-ray": dict(benchmark="bullet", nice_name="Bullet Physics Engine (Raytests)", short_name="Raytests",
                       scale="Seconds",
                       options={"bullet.run-test": "raytests"}),

    # Cache users (affected by cache)
    "pbzip2": dict(benchmark="compress-pbzip2", nice_name="BZIP2 (256MB Parallel Compression)", short_name="BZIP2",
                   scale="Seconds"),
    "hmmer": dict(benchmark="hmmer", nice_name="HMMer (Pfam Database Search)", short_name="HMMer",
                  scale="Seconds"),
    "x264": dict(benchmark="x264", nice_name="H.264 Video Encoding", short_name="H.264",
                 scale="Frames Per Second"),
    "scimark-sparse": dict(benchmark="scimark2", nice_name="SciMark (Sparse Matrix Multiply)",
                           short_name="Sparse Matrix Multiply",
                           scale="Mflops",
                           options={"scimark2.compute-test": "4"}),
    "scimark-dense": dict(benchmark="scimark2", nice_name="SciMark (Dense LU Matrix Factorization)",
                          short_name="Dense LU Matrix Factorization",
                          scale="Mflops",
                          options={"scimark2.compute-test": "5"}),
    "scimark-sor": dict(benchmark="scimark2", nice_name="SciMark (Jacobi Successive Over-Relaxation)",
                        short_name="Jacobi Successive Over-Relaxation",
                        scale="Mflops",
                        options={"scimark2.compute-test": "2"}),
    "scimark-fft": dict(benchmark="scimark2", nice_name="SciMark (Fast Fourier Transform)",
                        short_name="Fast Fourier Transform",
                        scale="Mflops",
                        options={"scimark2.compute-test": "1"}),
    "scimark-comp": dict(benchmark="scimark2", nice_name="SciMark (Composite)", short_name="Composite-SciMark",
                         scale="Mflops",
                         options={"scimark2.compute-test": "0"}),
}

benchmark_groups = {
    "all": all_benchmarks.keys(),

    "cache-users": ["x264", "scimark-comp", "hmmer", "pbzip2", "scimark-dense", "scimark-sor", "scimark-fft",
                    "scimark-sparse"],
    "prefetchers": ["x264"],
    "non-prefetchers": ["scimark-sparse", "scimark-dense", "scimark-sor", "scimark-fft",
                        "scimark-comp", "pbzip2", "hmmer"],

    "streaming-apps": ["c-ray", "scimark-monte", "openssl", "bullet-ray", "gcrypt"],
    "cache-abusers": ["scimark-monte", "bullet-ray"]
}


class PhoronixTestSuiteBenchmark(Benchmark):
    phoronixcmd = "PRESET_OPTIONS='%s' phoronix-test-suite python_results.run %s"
    phoronixcmd_timed = "PRESET_OPTIONS='%s' TIME_TO_EXECUTE_SEC=%s phoronix-test-suite python_results.run %s"

    result_list_wrapper = '########## RESULT-LIST ##########'
    result_list_re_pattern = "(.*)%s(.*)%s(.*)" % (result_list_wrapper, result_list_wrapper)
    result_list_re = re.compile(result_list_re_pattern, re.M | re.S)

    def __init__(self, benchmark_name):
        Benchmark.__init__(self, name=benchmark_name)
        self.benchmark_name = benchmark_name
        bench_dict = all_benchmarks[benchmark_name]

        benchmark_name = bench_dict["benchmark"]
        preset_options = ";".join("%s=%s" % (key, value) for key, value in bench_dict.get("options", {}).items())

        self.cmd_time_parameter = self.phoronixcmd_timed % (preset_options, "%s", benchmark_name)

    def get_required_cpus(self):
        """ Does not need its own CPU. Will use system CPUs. """
        return 0

    def consume(self, load: BenchmarkLoad, duration: BenchmarkDuration) -> BenchmarkResults:
        self.logger.info("Apply for duration: %i", duration)

        try:
            start = time.time()
            ms_output = self.ssh(self.cmd_time_parameter % duration, raise_stderr=True)  # blocking
            end = time.time()
            duration = end - start
        except Exception as e:
            self.logger.warning("Error while executing benchmark: %s", e)
            return {}

        try:
            res, raw = self.parse_benchmark_result(ms_output, duration)
        except Exception as e:
            self.logger.warning("Error while parsing output: %s", e)
            self.logger.debug("Stdout: %s", ms_output)
            return {}

        # The raw output of the test
        self.logger.debug(raw)

        # The output parameters (dict) of the test
        return res

    def parse_benchmark_result(self, out, duration):
        m = self.result_list_re.search(out)
        if m is None:
            raise BenchmarkError("Can't find result list wrapper in output")

        raw = (m.group(1) + m.group(3)).strip()

        actual_res = ast.literal_eval(m.group(2))[0]

        all_results = actual_res.pop("all-results", [])
        all_results_start_times = actual_res.pop("all-results-start-times", None)
        all_results_duration = actual_res.pop("all-results-duration", None)

        results_count = len(all_results)

        res_list = []

        if results_count > 0:
            if all_results_start_times is None:
                all_results_start_times = np.linspace(0, duration, results_count + 1)[:-1]

            t0 = all_results_start_times[0]

            if all_results_duration is None:
                all_results_duration = [all_results_start_times[i + 1] - all_results_start_times[i] for i in
                                        range(results_count - 1)]
                all_results_duration.append(duration - (all_results_start_times[-1] - t0))

            for r, s, d in zip(all_results, all_results_start_times, all_results_duration):
                test_time = s - t0
                res_list.append(dict(actual_res, performance=r, test_time=test_time))
                res_list.append(dict(actual_res, performance=r, test_time=test_time + d))

        return res_list, raw

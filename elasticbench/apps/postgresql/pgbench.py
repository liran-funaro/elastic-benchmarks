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

from cloudexp.guest.application.benchmark import BenchmarkError, BenchmarkLoad, BenchmarkDuration, BenchmarkResults, \
    Benchmark
from elasticbench.apps.postgresql import postgres_bin_path, POSTGRES_PORT
from mom.util.parsers import parse_int, parse_string, parse_float, parameter_int, parameter_bool


BENCHMARK_FIELDS = {
    "scaling_factor": (parse_int, r"scaling factor: (\d+)"),
    "query_mode": (parse_string, "query mode: (.+)"),
    "number_of_clients": (parse_int, r"number of clients: (\d+)"),
    "number_of_threads": (parse_int, r"number of threads: (\d+)"),
    "duration": (parse_int, r"duration: (\d+) s"),
    "transactions_count": (parse_int, r"number of transactions actually processed: (\d+)"),
    "tps": (parse_float, r"tps = ([\d.]+) \(including connections establishing\)"),
    "tps_without_connections_time": (parse_float, r"tps = ([\d.]+) \(excluding connections establishing\)"),
    "query01_latency": (parse_float, r"([\d.]+)\s+\\set nbranches 1 \* :scale"),
    "query02_latency": (parse_float, r"([\d.]+)\s+\\set ntellers 10 \* :scale"),
    "query03_latency": (parse_float, r"([\d.]+)\s+\\set naccounts 100000 \* :scale"),
    "query04_latency": (parse_float, r"([\d.]+)\s+\\setrandom aid 1 :naccounts"),
    "query05_latency": (parse_float, r"([\d.]+)\s+\\setrandom bid 1 :nbranches"),
    "query06_latency": (parse_float, r"([\d.]+)\s+\\setrandom tid 1 :ntellers"),
    "query07_latency": (parse_float, r"([\d.]+)\s+\\setrandom delta -5000 5000"),
    "query08_latency": (parse_float, r"([\d.]+)\s+BEGIN;"),
    "query09_latency": (parse_float, r"([\d.]+)\s+"
                                     r"UPDATE pgbench_accounts SET abalance = abalance \+ :delta WHERE aid = :aid;"),
    "query10_latency": (parse_float, r"([\d.]+)\s+"
                                     r"SELECT abalance FROM pgbench_accounts WHERE aid = :aid;"),
    "query11_latency": (parse_float, r"([\d.]+)\s+"
                                     r"UPDATE pgbench_tellers SET tbalance = tbalance \+ :delta WHERE tid = :tid;"),
    "query12_latency": (parse_float, r"([\d.]+)\s+"
                                     r"UPDATE pgbench_branches SET bbalance = bbalance \+ :delta WHERE bid = :bid;"),
    "query13_latency": (parse_float, r"([\d.]+)\s+"
                                     r"INSERT INTO pgbench_history \(tid, bid, aid, delta, mtime\) "
                                     r"VALUES \(:tid, :bid, :aid, :delta, CURRENT_TIMESTAMP\);"),
    "query14_latency": (parse_float, r"([\d.]+)\s+END;"),
}


##################################################################################################################
# PostgreSQL benchmark sample output
##################################################################################################################
# starting vacuum...end.
# transaction type: TPC-B (sort of)
# scaling factor: 100
# query mode: simple
# number of clients: 60
# number of threads: 20
# duration: 120 s
# number of transactions actually processed: 17117
# tps = 142.297915 (including connections establishing)
# tps = 156.115797 (excluding connections establishing)
# statement latencies in milliseconds:
#     0.002530    \set nbranches 1 * :scale
#     0.000541    \set ntellers 10 * :scale
#     0.000563    \set naccounts 100000 * :scale
#     0.000639    \setrandom aid 1 :naccounts
#     0.000468    \setrandom bid 1 :nbranches
#     0.000512    \setrandom tid 1 :ntellers
#     0.000490    \setrandom delta -5000 5000
#     0.141871    BEGIN;
#     2.222018    UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;
#     0.230132    SELECT abalance FROM pgbench_accounts WHERE aid = :aid;
#     0.263907    UPDATE pgbench_tellers SET tbalance = tbalance + :delta WHERE tid = :tid;
#     0.251859    UPDATE pgbench_branches SET bbalance = bbalance + :delta WHERE bid = :bid;
#     0.219017    INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) \
#                   VALUES (:tid, :bid, :aid, :delta, CURRENT_TIMESTAMP);
#     2.457115    END;
##################################################################################################################
class PostgreSQLBenchmark(Benchmark):
    def __init__(self, threads_count=None, connection_per_transaction=None, output_log=None, scale_factor=None):
        Benchmark.__init__(self)
        self.threads_count = parameter_int(threads_count, 150)
        self.connection_per_transaction = parameter_bool(connection_per_transaction, False)
        self.output_log = parameter_bool(output_log, False)
        self.scale_factor = parameter_int(scale_factor, 200)
        self._initialized = False

    def get_required_cpus(self):
        return min(2, self.threads_count)

    def init_pgbench(self):
        if self._initialized:
            return
        args = [
            postgres_bin_path('pgbench'),
            "-i",                            # -i; --initialize: Required to invoke initialization mode.
            "-h", str(self.application_ip),  # -h hostname; --host=hostname: The database server's host name
            "-p", str(POSTGRES_PORT),        # -p port; --port=port: The database server's port number
            "-U", "postgres",                # -U login; --username=login: The user name to connect as
            "-s", str(self.scale_factor),    # -s scale_factor; --scale=scale_factor
            # Multiply the number of rows generated by the scale factor. For example, -s 100 will create 10,000,000 rows
            # in the pgbench_accounts table. Default is 1. When the scale is 20,000 or larger, the columns used to hold
            # account identifiers (aid columns) will switch to using larger integers (bigint), in order to be big enough
            # to hold the range of account identifiers.
        ]

        out, err = self.popen(args, raise_stderr=False)  # blocking
        self.logger.info("Initialized pgbench: %s", out)
        self._initialized = True

    def consume(self, load: BenchmarkLoad, duration: BenchmarkDuration) -> BenchmarkResults:
        # "The number of clients must be a multiple of the number of threads"
        cur_threads_count = self.threads_count
        if cur_threads_count > load:
            cur_threads_count = load
        if load % cur_threads_count != 0:
            cur_threads_count -= load % cur_threads_count

        args = [
            postgres_bin_path('pgbench'),
            "-h", str(self.application_ip),  # -h hostname; --host=hostname: The database server's host name
            "-p", str(POSTGRES_PORT),        # -p port; --port=port: The database server's port number
            "-U", "postgres",                # -U login; --username=login: The user name to connect as
            "-r",                            # -r; --report-latencies
            # Report the average per-statement latency (execution time from the perspective of the client) of each
            # command after the benchmark finishes. See below for details.
            "-T", str(duration),             # -T seconds; --time=seconds
            # Run the test for this many seconds, rather than a fixed number of transactions per client.
            # -t and -T are mutually exclusive.
            "-c", str(load),                 # -c clients; --client=clients
            # Number of clients simulated, that is, number of concurrent database sessions. Default is 1.
            "-j", str(cur_threads_count),    # -j threads; --jobs=threads
            # Number of worker threads within pgbench. Using more than one thread can be helpful on multi-CPU machines.
            # Clients are distributed as evenly as possible among available threads. Default is 1.
        ]

        if self.connection_per_transaction:
            args.append("--connect")                 # Reconnect for each transaction
        if self.output_log:
            args.append("--log")                     # Output log to working directory

        ms_output, ms_error = self.popen(args, raise_stderr=False)  # blocking

        # check for error:
        if ms_error and not re.search(r"starting vacuum[.]{3}.*end\.", ms_error, re.DOTALL | re.I | re.M):
            self.logger.warning("pgbench ended with error: %s", ms_error)
            raise BenchmarkError(f"pgbench error: {ms_error}")

        return self.parse_output(ms_output)

    @staticmethod
    def parse_output(output):
        res = {}
        for field_key in BENCHMARK_FIELDS:
            function, regexp = BENCHMARK_FIELDS[field_key]
            res[field_key] = function(regexp, output)

        return res

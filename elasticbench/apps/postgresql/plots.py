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
from cloudexp.results.analyze import fetch_data
from cloudexp.results.data import ExpData
from cloudexp.results.plots import PLOT_PERIOD

psql_fields = ['perf:duration', 'perf:number_of_clients', 'perf:number_of_threads', 'perf:query01_latency',
               'perf:query02_latency', 'perf:query03_latency', 'perf:query04_latency', 'perf:query05_latency',
               'perf:query06_latency', 'perf:query07_latency', 'perf:query08_latency', 'perf:query09_latency',
               'perf:query10_latency', 'perf:query11_latency', 'perf:query12_latency', 'perf:query13_latency',
               'perf:query14_latency', 'perf:scaling_factor', 'perf:tps',
               'perf:tps_without_connections_time', 'perf:transactions_count']
# 'perf:query_mode'


TPS = 'Transactions/second'


def tps(d: ExpData):
    # With connection time
    return fetch_data(d, y='perf:tps', y_name=TPS, **PLOT_PERIOD)


def tps_no_conn(d: ExpData):
    return fetch_data(d, y='perf:tps_without_connections_time', y_name=TPS, **PLOT_PERIOD)


def psql_perf(key: str):
    def fetch_psql_perf(d: ExpData):
        return fetch_data(d, y=key, y_name=key, **PLOT_PERIOD)
    return fetch_psql_perf


all_psql_perf = [psql_perf(f) for f in psql_fields]

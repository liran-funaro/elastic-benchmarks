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
from cloudexp.results.analyze import fetch_data
from cloudexp.results.data import ExpData
from cloudexp.results.plots import RATE, MEMORY, PLOT_PERIOD, PLOT_FUTURE_MOMENT


def perf_hits(d: ExpData):
    return fetch_data(d, y='perf:hit-rate', y_name=RATE, **PLOT_PERIOD)


def perf_throughput(d: ExpData):
    return fetch_data(d, y='perf:throughput', y_name=RATE, **PLOT_PERIOD)


def mem_alloc(d: ExpData):
    return fetch_data(d, y='perf:memory', y_name=MEMORY, **PLOT_FUTURE_MOMENT)


# Used by ExpAnalyzer
perf = perf_hits
memory = mem_alloc
perf_window = 30

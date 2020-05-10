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
from cloudexp.results.data import ExpData
from cloudexp.results.analyze import fetch_data
from cloudexp.results.plots import PLOT_PERIOD, RATE


def ops(d: ExpData):
    return fetch_data(d, y='perf:ops', y_name=RATE, **PLOT_PERIOD)


def time_ratio(d: ExpData):
    return fetch_data(d, y=('perf:real-time', 'perf:user-time', 'perf:sys-time'),
                      out_func=lambda df: df['perf:real-time'] / (df['perf:user-time'] + df['perf:sys-time']),
                      out='time-ratio',
                      y_name='Time Ratio (real/vm time)', **PLOT_PERIOD)


def ops_per_second_real_time(d: ExpData):
    return fetch_data(d, y='perf:real-time-ops/s', y_name=RATE, **PLOT_PERIOD)


def ops_per_second_vm_time(d: ExpData):
    return fetch_data(d, y='perf:user+sys-time-ops/s', y_name=RATE, **PLOT_PERIOD)

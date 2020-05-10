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
from cloudexp.results.plots import MEMORY, PLOT_PERIOD, MB_RATE


def perf_bandwidth(d: ExpData):
    return fetch_data(d, y='perf:bandwidth', y_name=MB_RATE, **PLOT_PERIOD)


def perf_bandwidth_resample(d: ExpData):
    return fetch_data(d, y='perf:bandwidth', y_name=MB_RATE, resample=(60, 1), **PLOT_PERIOD)


def perf_transfer(d: ExpData):
    return fetch_data(d, y='perf:transfer', y_name=MEMORY, **PLOT_PERIOD)

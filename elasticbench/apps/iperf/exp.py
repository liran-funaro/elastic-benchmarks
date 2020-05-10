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
from typing import Union

from cloudexp.util.function_of_time import FunctionOfTime
from elasticbench.apps.iperf import IPerf, IPerfBenchmark

from elasticbench.exp import default_single_vm_args, default_static_test


IPERF_EXP_NAME = 'iperf'


def default_args(mem_func: FunctionOfTime, load: Union[int, FunctionOfTime] = 1, **extra_info):
    return default_single_vm_args(
        application=IPerf(),
        benchmark=IPerfBenchmark(mode='remote'),
        mem_func=mem_func,
        load=load,
        load_interval=30,
        **extra_info
    )


def static_test(load: Union[int, FunctionOfTime] = 1, memory_alloc=4096, duration=(1, 'h'), **kwargs):
    return default_static_test(IPERF_EXP_NAME, default_args, load, memory_alloc, duration, **kwargs)

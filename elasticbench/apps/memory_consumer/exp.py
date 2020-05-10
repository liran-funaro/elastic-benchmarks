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
from typing import Union, Tuple

from cloudexp.util.function_of_time import FunctionOfTime
from elasticbench.apps.memory_consumer import MemoryConsumer
from elasticbench.apps.memory_consumer.benchmark import MemoryConsumerBenchmark
from elasticbench.exp import default_single_vm_args, default_decrease_increase_test, default_step_test,\
    default_make_all_drop_tests, default_make_all_validation

MC_EXP_NAME = 'mc'


def default_args(mem_func: FunctionOfTime, load: Union[int, FunctionOfTime] = 10,
                 load_interval: int = 5, **extra_info):
    max_mem = mem_func.get_max_value()
    return default_single_vm_args(
        application=MemoryConsumer(spare_mem=100,
                                   saturation_mem=2*1024,
                                   wait_timeout=20,
                                   sleep_after_write=0.1,
                                   max_mem=max_mem,
                                   backend='cpp'),
        benchmark=MemoryConsumerBenchmark(),
        mem_func=mem_func,
        load=load,
        load_interval=load_interval,
        **extra_info
    )


def decrease_increase_test(low_mem: int = 896, high_mem: int = 3072, load: Union[int, FunctionOfTime] = 10,
                           step_time: Tuple[int, str] = (2, 'm'),
                           warmup_time: Tuple[int, str] = (1, 'm'), down_step_time: Tuple[int, str] = None,
                           up_step_time: Tuple[int, str] = None, ** kwargs):
    return default_decrease_increase_test(MC_EXP_NAME, default_args, low_mem, high_mem, load,
                                          step_time, warmup_time, down_step_time, up_step_time, **kwargs)


def step_test(base_value, step_value, step_times: (), load: Union[int, FunctionOfTime] = 10):
    return default_step_test(MC_EXP_NAME, default_args, base_value, step_value, step_times, load)


def make_all_drop_test(low_mem: int = 896, high_mem: int = 2048, load: Union[int, FunctionOfTime] = 10,
                       base_step_time: Tuple[int, str] = (2, 'm'),
                       drop_step_time: Tuple[int, str] = (2, 'm'),
                       warmup_time: Tuple[int, str] = (1, 'm'),
                       repeat: int = 20, **kwargs):
    return default_make_all_drop_tests(MC_EXP_NAME, default_args, low_mem, high_mem, load, base_step_time,
                                       drop_step_time, warmup_time, repeat, **kwargs)


def make_all_validation(trace_group: str, low_mem: int = 896, high_mem: int = 2048,
                        load: Union[int, FunctionOfTime] = 10, warmup_time: Tuple[int, str] = (1, 'm'), **kwargs):
    return default_make_all_validation(MC_EXP_NAME, default_args, trace_group, low_mem, high_mem, load, warmup_time,
                                       **kwargs)


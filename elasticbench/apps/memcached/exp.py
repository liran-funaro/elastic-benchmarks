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
from typing import Union, Tuple

from cloudexp.util.function_of_time import FunctionOfTime
from elasticbench.apps.memcached.app_controller import Memcached
from elasticbench.apps.memcached.collector import MemcachedCollector
from elasticbench.apps.memcached.memslap import MemSlap
from elasticbench.exp import default_single_vm_args, default_decrease_increase_test, default_static_test, \
    default_step_test, default_make_all_drop_tests, default_make_all_validation

MEMCACHED_EXP_NAME = 'memcached'
MEMCACHED_STATIC_EXP_NAME = 'memcached-static'


def default_args(mem_func: FunctionOfTime, load: Union[int, FunctionOfTime] = 20,
                 win_size='100k', seed=1, dynamic=True, elastic=True, mode='local', init_mem_size=64,
                 spare_mem=100, cmd_get_percent=0.3, **extra_info):
    return default_single_vm_args(
        application=Memcached(spare_mem=spare_mem,
                              init_mem_size=init_mem_size,
                              wait_timeout=20,
                              dynamic=dynamic,
                              elastic=elastic),
        benchmark=MemSlap(cmd_get_percent=cmd_get_percent,
                          keys_dist=[(249, 249, 1.0)],
                          vals_dist=[(1024, 1024, 1.0)],
                          win_size=win_size,
                          seed=seed,
                          mode=mode),
        mem_func=mem_func,
        load=load,
        load_interval=mem_func.get_max_time(),
        guest_collectors=(MemcachedCollector,),
        **extra_info
    )


def get_exp_name(load: Union[int, FunctionOfTime] = 20, cmd_get_percent=0.3, elastic=True,
                 **_kwargs):
    if elastic:
        exp_name = MEMCACHED_EXP_NAME
    else:
        exp_name = MEMCACHED_STATIC_EXP_NAME
    return os.path.join(exp_name, f'l{load}-g{cmd_get_percent:.2f}')


def decrease_increase_test(low_mem: int = 896, high_mem: int = 4096, load: Union[int, FunctionOfTime] = 20,
                           step_time: Tuple[int, str] = (5, 'm'),
                           warmup_time: Tuple[int, str] = (5, 'm'), down_step_time: Tuple[int, str] = (4, 'm'),
                           up_step_time: Tuple[int, str] = (5, 'm'), ** kwargs):
    return default_decrease_increase_test(get_exp_name(load, **kwargs), default_args, low_mem, high_mem, load,
                                          step_time, warmup_time, down_step_time, up_step_time, **kwargs)


def static_test(load: Union[int, FunctionOfTime] = 20, memory_alloc=4096, duration=(1, 'h'), **kwargs):
    return default_static_test(get_exp_name(load, **kwargs), default_args, load, memory_alloc, duration, **kwargs)


def step_test(base_value, step_value, step_times: (), load: Union[int, FunctionOfTime] = 20, **kwargs):
    return default_step_test(get_exp_name(load, **kwargs), default_args, base_value, step_value, step_times, load,
                             **kwargs)


def make_all_drop_test(low_mem: int = 896, high_mem: int = 3584, load: Union[int, FunctionOfTime] = 20,
                       base_step_time: Tuple[int, str] = (12, 'm'),
                       drop_step_time: Tuple[int, str] = (3, 'm'),
                       warmup_time: Tuple[int, str] = (10, 'm'),
                       repeat: int = 20, **kwargs):
    return default_make_all_drop_tests(get_exp_name(load, **kwargs), default_args, low_mem, high_mem, load,
                                       base_step_time, drop_step_time, warmup_time, repeat, **kwargs)


def make_all_validation(trace_group: str, low_mem: int = 896, high_mem: int = 3584,
                        load: Union[int, FunctionOfTime] = 20, warmup_time: Tuple[int, str] = (10, 'm'), **kwargs):
    return default_make_all_validation(get_exp_name(load, **kwargs), default_args, trace_group, low_mem, high_mem, load,
                                       warmup_time, **kwargs)

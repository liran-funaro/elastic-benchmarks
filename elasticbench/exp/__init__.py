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
import logging
import os
from typing import Union, Tuple

from cloudexp.exp import save_experiment_args
from cloudexp.guest.allocations import memory, loads
from cloudexp.util.function_of_time import FunctionOfTime

from elasticbench import settings
from elasticbench.exp.mem_trace import iterate_over_stored_mem_trace

TEST_EXP_TYPE = 'test'
STATIC_EXP_TYPE = 'static'
STEP_EXP_TYPE = 'step'
DEC_INC_TYPE = 'dec-inc'
DROP_EXP_TYPE = 'drop'
VALID_EXP_TYPE = 'validation'

MAX_MEM_DIFF = 512


def on_finish(logger, output_path, *_args, **_kwargs):
    mpl_logger = logging.getLogger('matplotlib')
    mpl_logger.setLevel(logging.WARNING)
    from cloudexp.results.data import ExpData

    try:
        ExpData(output_path)
    except Exception as e:
        logger.exception("Failed converting data log to DB format: %s", e)


def default_single_vm_args(application, benchmark,
                           mem_func: memory.memory_function, load: Union[int, FunctionOfTime] = 1,
                           load_interval: int = 5, guest_collectors=(), **extra_info):
    if type(load) in (int, float):
        load = loads.constant_load(load)

    return dict(
        exp_config=None,  # default
        host_mom_config={
            ('guest-monitor', 'interval'): 3,
            ('host-monitor', 'collectors'): ['MemoryStatistics', 'CpuUsage', 'NetworkStatistics', 'ApplicationsStats'],
            ('host-monitor', 'applications'): ['python', 'ssh', 'pgbench', 'memaslap'],
        },
        vms_desc={
            'vm-1': dict(
                application=application,
                benchmark=benchmark,
                guest_mom_config={
                    ('policy', 'response-scripts'): {
                        'memory': mem_func
                    },
                    ('monitor', 'collectors'): ['MemoryStatistics', 'CpuUsage', 'NetworkStatistics',
                                                'ApplicationsStats', *guest_collectors],
                    ('monitor', 'applications'): ['memcached', 'memaslap', 'python', 'iperf', 'stress-ng', 'postgres',
                                                  'java'],
                },
                load_func=load,
                max_mem=mem_func.get_max_value() + MAX_MEM_DIFF,
                base_mem=mem_func.get_value(0),
                max_vcpus=4,
                base_vcpus=4,
                load_interval=load_interval,
            ),
        },
        duration=mem_func.get_max_time(),
        on_finish=on_finish,
        extra_info=extra_info
    )


def default_decrease_increase_test(exp_name, args_func, low_mem: int, high_mem: int,
                                   load: Union[int, FunctionOfTime] = 1, step_time: Tuple[int, str] = (1, 'm'),
                                   warmup_time: Tuple[int, str] = None, down_step_time: Tuple[int, str] = None,
                                   up_step_time: Tuple[int, str] = None, **kwargs):
    exp_sub_name = f'l{load}-m{low_mem}-{high_mem}'
    rel_path, output_path = settings.get_output_path_and_relative(DEC_INC_TYPE, exp_name, exp_sub_name)
    mem_func = memory.mem_decrease_increase(low_mem, high_mem, step_time, warmup_time, down_step_time, up_step_time)
    exp_kwargs = args_func(mem_func=mem_func, load=load, type=DEC_INC_TYPE,
                           exp_name=exp_name, exp_sub_name=exp_sub_name, **kwargs)
    save_experiment_args(output_path=output_path, **exp_kwargs)
    return settings.linkify_to_monitor(rel_path)


def default_static_test(exp_name, args_func, load: Union[int, FunctionOfTime] = 1,
                        memory_alloc=4096, duration=(1, 'h'), **kwargs):
    exp_sub_name = f'l{load}-m{memory_alloc}-{duration[0]}{duration[1]}'
    rel_path, output_path = settings.get_output_path_and_relative(STATIC_EXP_TYPE, exp_name, exp_sub_name)
    mem_func = memory.memory_static_function(memory_alloc, duration)
    exp_kwargs = args_func(mem_func=mem_func, load=load, type=STATIC_EXP_TYPE, exp_name=exp_name,
                           exp_sub_name=exp_sub_name, **kwargs)
    save_experiment_args(output_path=output_path, **exp_kwargs)
    return settings.linkify_to_monitor(rel_path)


def default_step_test(exp_name, args_func, base, step, step_times: (), load: Union[int, FunctionOfTime] = 1, **kwargs):
    exp_sub_name = f'l{load}-b{base}-s{step}-x{len(step_times)}'
    rel_path, output_path = settings.get_output_path_and_relative(STEP_EXP_TYPE, exp_name, exp_sub_name)
    mem_func = memory.step_function(base, step, step_times)
    exp_kwargs = args_func(mem_func=mem_func, load=load, type=STEP_EXP_TYPE, exp_name=exp_name,
                           exp_sub_name=exp_sub_name, **kwargs)
    save_experiment_args(output_path=output_path, **exp_kwargs)
    return settings.linkify_to_monitor(rel_path)


def default_drop_test(exp_name, args_func, base, drop, base_step_time: Tuple[int, str] = (1, 'm'),
                      drop_step_time: Tuple[int, str] = (1, 'm'), warmup_time: Tuple[int, str] = (1, 'm'),
                      repeat: int = 100, load: Union[int, FunctionOfTime] = 1, exp_type=DROP_EXP_TYPE, **kwargs):
    exp_sub_name = f'l{load}-b{base}-d{drop}-x{repeat}'
    rel_path, output_path = settings.get_output_path_and_relative(exp_type, exp_name, exp_sub_name)
    step_times = [base_step_time, drop_step_time] * repeat
    if warmup_time is not None:
        base_time, base_unit = base_step_time
        warmup_time, warmup_unit = warmup_time
        assert base_unit == warmup_unit, "Warmup time units must be the same as the base step time units."
        step_times[0] = (base_time + warmup_time, base_unit)

    mem_func = memory.step_function(base, drop, step_times)
    exp_kwargs = args_func(mem_func=mem_func, load=load, type=exp_type, exp_name=exp_name,
                           exp_sub_name=exp_sub_name, **kwargs)
    save_experiment_args(output_path=output_path, **exp_kwargs)
    return settings.linkify_to_monitor(rel_path)


def default_make_all_drop_tests(exp_name, args_func, low_mem: int, high_mem: int, load: Union[int, FunctionOfTime] = 1,
                                base_step_time: Tuple[int, str] = (1, 'm'), drop_step_time: Tuple[int, str] = (1, 'm'),
                                warmup_time: Tuple[int, str] = (1, 'm'), repeat: int = 20, exp_type=DROP_EXP_TYPE,
                                **kwargs):
    mem_range = memory.mem_series(low_mem, high_mem)
    for i, base_value in enumerate(mem_range):
        for drop_value in mem_range[:i]:
            default_drop_test(exp_name, args_func, base_value, drop_value,
                              base_step_time=base_step_time, drop_step_time=drop_step_time,
                              warmup_time=warmup_time, repeat=repeat, load=load, exp_type=exp_type, **kwargs)
    rel_path = settings.relative_output_path(exp_type, exp_name)
    return settings.linkify_to_monitor(rel_path)


def default_make_all_validation(exp_name, args_func, trace_group: str, low_mem: int, high_mem: int,
                                load: Union[int, FunctionOfTime] = 1,
                                warmup_time: Tuple[int, str] = (1, 'm'), exp_type=VALID_EXP_TYPE, overwrite=False,
                                parameters_set=None, **kwargs):
    for (a, c, i), mem_func in iterate_over_stored_mem_trace(trace_group):
        if parameters_set is not None and (a, c) not in parameters_set:
            continue
        exp_sub_name = f'l{load}-a{a}-c{c}'
        exp_idx = f'i{i}'
        rel_path, output_path = settings.get_output_path_and_relative(exp_type, exp_name, trace_group,
                                                                      exp_sub_name, exp_idx)
        if not overwrite and os.path.exists(output_path):
            continue
        mem_func.clip(low_mem, high_mem, inplace=True)
        mem_func_with_warmup = memory.memory_function([(high_mem, *warmup_time)])
        mem_func_with_warmup.concat(mem_func, inplace=True)
        exp_kwargs = args_func(mem_func=mem_func_with_warmup, load=load, type=exp_type,
                               exp_name=exp_name, exp_sub_name=exp_sub_name, exp_idx=exp_idx, **kwargs)
        save_experiment_args(output_path=output_path, **exp_kwargs)

    rel_path = settings.relative_output_path(exp_type, exp_name, trace_group)
    return settings.linkify_to_monitor(rel_path)

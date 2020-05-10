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
from typing import Tuple

from cloudexp.util.function_of_time import FunctionOfTime, static_function


def memory_function(*args, format_value='{:,}MB', **kwargs):
    return FunctionOfTime(*args, format_value=format_value, **kwargs)


def memory_static_function(val, duration=(1, 'm'), format_value='{:,}MB', **kwargs):
    return static_function((val, *duration), format_value=format_value, **kwargs)


INCREASING_REPR = '%(duration)s: Increasing from %(min_value)s to %(max_value)s'
DECREASING_REPR = '%(duration)s: Decreasing from %(min_value)s to %(max_value)s'
DEC_INC_REPR = '%(duration)s: Decreasing/increasing from %(min_value)s to %(max_value)s'
DROP_REPR = '%(duration)s: Drop from %(max_value)s to %(min_value)s'
STEP_REPR = '%(duration)s: Step from %(max_value)s to %(min_value)s'


def drop_function(base, drop, format_value='{:,}MB', **kwargs):
    args = (base, drop, base)
    return FunctionOfTime(args, format_value=format_value, default_representation=DROP_REPR, **kwargs)


def step_function(base_value, step_value, step_times: (), format_value='{:,}MB', **kwargs):
    ret = FunctionOfTime((), format_value=format_value, default_representation=STEP_REPR, **kwargs)

    for i, duration in enumerate(step_times):
        if type(duration) in (tuple, list):
            duration, unit = duration
        else:
            unit = None
        ret.add_value(base_value if i % 2 == 0 else step_value, duration, unit)

    return ret


# For testing
mem_6_5G_long = memory_static_function(6144 + 512, (15, 'h')),
mem_4G_short = memory_static_function(4096, (1, 's')),

# Static memory
mem_12G = memory_static_function(12288, (30, 'm'))
mem_9G = memory_static_function(9216, (30, 'm'))
mem_8G = memory_static_function(8192, (30, 'm'))
mem_7_5G = memory_static_function(7168 + 512, (30, 'm'))
mem_7G = memory_static_function(7168, (30, 'm'))
mem_6_5G = memory_static_function(6144 + 512, (30, 'm'))
mem_6G = memory_static_function(6144, (30, 'm'))
mem_5_5G = memory_static_function(5120 + 512, (30, 'm'))
mem_5G = memory_static_function(5120, (30, 'm'))
mem_4_5G = memory_static_function(4096 + 512, (30, 'm'))
mem_4G = memory_static_function(4096, (30, 'm'))
mem_3_5G = memory_static_function(3072 + 512, (30, 'm'))
mem_3G = memory_static_function(3072, (30, 'm'), )
mem_2_5G = memory_static_function(2048 + 512, (30, 'm'))
mem_2G = memory_static_function(2048, (30, 'm'), )
mem_1_5G = memory_static_function(1024 + 512, (30, 'm'))
mem_1G = memory_static_function(1024, (30, 'm'))
mem_900M = memory_static_function(896, (30, 'm'))
mem_700M = memory_static_function(768, (30, 'm'))
mem_600M = memory_static_function(640, (30, 'm'))
mem_500M = memory_static_function(512, (30, 'm'))

mem_step_8G = step_function(512, 8192, [(30, 's'), (1, 'm')])

mem_increase_quick = memory_function(
    [(512, 1, 'm'), *((m, 1, 'm') for m in range(1024, 1024*4+1, 1024))],
    default_representation=INCREASING_REPR)
mem_increase = memory_function(
    [(2048 + 512, 90, 'm'), *((m, 60, 'm') for m in range(512, 1024, 128)),
     *((m, 60, 'm') for m in range(1024, 6500, 512))],
    default_representation=INCREASING_REPR)
mem_increase_slow = memory_function(
    [(4096, 90, 'm'), *((m, 20, 'm') for m in range(512, 1024, 128)),
     *((m, 20, 'm') for m in range(1024, 6500, 512))],
    default_representation=INCREASING_REPR)
mem_decrease = memory_function(
    [(6144 + 512, 90, 'm'), *((m, 60, 'm') for m in range(6144, 1024, -512)),
     *((m, 60, 'm') for m in range(1024, 500, -128))],
    default_representation=DECREASING_REPR)


def mem_series(low_mem: int, high_mem: int):
    if low_mem < 512 or high_mem < 512:
        raise ValueError("Memory is lower than 512.")

    # This given the next below high_mem rounded up to 512MB jumps
    # Uses the round-up trick: ((n + round_number - 1) // round_number) * round_number
    # Here: n = high_mem-512, round_number = 512 and we want a space of at least 128 MB (replace 1 with 128)

    # ==> round_high_mem = (((high_mem-512) + 512 - 128) // 512) * 512
    round_below_high_mem = ((high_mem - 128) // 512) * 512

    if low_mem < 1024:
        # ==> round_low_mem = (((low_mem+128) + 128 - 64) // 128) * 128
        round_above_low_mem = ((low_mem + 192) // 128) * 128
    else:
        # ==> round_low_mem = (((low_mem+512) + 512 - 128*3) // 512) * 512
        round_above_low_mem = ((low_mem + 640) // 512) * 512

    step_512_low_limit = max(round_above_low_mem, 1024)

    return (low_mem,
            *range(round_above_low_mem, step_512_low_limit, 128),
            *range(step_512_low_limit, round_below_high_mem+1, 512),
            high_mem)


def mem_decrease_increase(low_mem: int, high_mem: int, step_time: Tuple[int, str] = None,
                          warmup_time: Tuple[int, str] = None, down_step_time: Tuple[int, str] = None,
                          up_step_time: Tuple[int, str] = None) -> FunctionOfTime:
    if up_step_time is None:
        up_step_time = step_time
    if down_step_time is None:
        down_step_time = step_time

    if any(s is None for s in (down_step_time, up_step_time)):
        raise ValueError("`down/up_step_time` must not be None, or `step_time` should not be None.")

    mem_range = mem_series(low_mem, high_mem)

    s = [(high_mem, *warmup_time),
         *((m, *down_step_time) for m in reversed(mem_range)),
         *((m, *up_step_time) for m in mem_range[1:])]

    return FunctionOfTime(s, default_representation=DEC_INC_REPR)

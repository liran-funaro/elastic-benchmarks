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
from typing import Iterator, Tuple

import nesteddict
import scipy.stats
import numpy as np

from cloudexp.util.function_of_time import FunctionOfTime
from cloudexp.util.timeformat import convert_to_seconds
from cloudexp.guest.allocations import memory
from elasticbench import settings


MEM_TRACE_NAME = 'mem-trace'


def beta_mean_max(mean, density=1, max_val=1, size=None):
    # E = a / (a+b) => E*(a+b) = a => E*a + E*b = a => E*b = a - E*a => E*b = a*(1-E)
    # => a = b * E/(1-E)
    # => b = a * (1-E)/E
    eps = np.finfo(np.float32).eps
    mean = np.divide(mean, max_val)
    mean = np.clip(mean, eps, 1 - eps)
    try:
        size = len(mean)
    except:
        if size is None:
            mean = np.repeat(mean, 1)
        else:
            mean = np.repeat(mean, size)
    u = mean < 0.5
    beta_a = np.repeat(density, size).astype(float)
    beta_b = np.repeat(density, size).astype(float)
    beta_a[u] = np.maximum(density * mean[u] / (1 - mean[u]), eps)
    beta_b[~u] = np.maximum(density * (1 - mean[~u]) / mean[~u], eps)
    ret = scipy.stats.beta.rvs(beta_a, beta_b, size=size, scale=max_val)
    if size is None:
        return ret[0]
    else:
        return ret


def beta_min_max_mean(min_val, max_val, mean, density=1, size=None):
    return beta_mean_max(mean - min_val, density, max_val - min_val, size=size) + min_val


def generate_mem_changes(max_amplitude: int = 4096, min_amplitude: int = 128, changes_count: int = 6,
                         density=1):
    changes = beta_min_max_mean(-max_amplitude, max_amplitude, 128, density=density, size=changes_count)
    changes = ((np.abs(changes) + min_amplitude - 1) // min_amplitude) * min_amplitude * np.sign(changes)
    return changes.astype(int)


def generate_mem_trace_clip(max_amplitude: int = 4096, min_amplitude: int = 128, changes_per_hour: int = 6,
                            density=1, duration=(1, 'h'), limits=(896, 4096), init=2048):
    duration = convert_to_seconds(*duration)
    changes_count = int(changes_per_hour * (duration / (60 * 60)))

    values = [init]
    while len(values)-1 < changes_count:
        changes = generate_mem_changes(max_amplitude, min_amplitude, changes_count, density)
        for c in changes:
            if len(values) > changes_count:
                break
            last_val = values[-1]
            val = np.clip(last_val + c, *limits)
            if np.abs(last_val - val) >= min_amplitude:
                values.append(val)

    intervals = len(values) + 1
    change_times = np.linspace(0, duration, intervals)
    changes_duration = np.diff(change_times)
    return memory.memory_function(zip(values, changes_duration))


def generate_multiple_mem_trace(trace_group: str, max_amplitude_range, change_per_hour_range, repeat=3,
                                min_amplitude=128, density=1, duration=(1, 'h'), trace_type='clip', overwrite=False,
                                **kwargs):
    traces = {
        'clip': generate_mem_trace_clip,
    }
    trace_func = traces[trace_type]

    store = nesteddict.NestedDictFS(settings.output_path(MEM_TRACE_NAME, trace_group), mode='c', store_engine='pickle')
    for a in max_amplitude_range:
        for c in change_per_hour_range:
            for i in range(repeat):
                if not overwrite and (a, c, i) in store:
                    continue
                t = trace_func(a, min_amplitude, c, density, duration, **kwargs)
                store[a, c, i] = t


def get_stored_mem_trace(trace_group: str, max_amplitude: int = 4096, changes_per_hour: int = 6,
                         index=0) -> FunctionOfTime:
    store = nesteddict.NestedDictFS(settings.output_path(MEM_TRACE_NAME, trace_group), mode='c', store_engine='pickle')
    return store[max_amplitude, changes_per_hour, index]


def iterate_over_stored_mem_trace(trace_group: str) -> Iterator[Tuple[Tuple[int, int, int], FunctionOfTime]]:
    store = nesteddict.NestedDictFS(settings.output_path(MEM_TRACE_NAME, trace_group), mode='r', store_engine='pickle')
    for k, mem_func in store.walk():
        if isinstance(mem_func, nesteddict.NestedDictFS):
            continue
        m, c, i = map(int, k)
        yield (m, c, i), mem_func

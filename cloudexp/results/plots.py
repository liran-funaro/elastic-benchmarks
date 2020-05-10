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


RATE = 'Items/second'
MB_RATE = 'MB/second'
PERCENT = 'Percent (%)'
MEMORY = 'Memory (MB)'
CPU = 'CPU Usage'
CPU_PERCENT = 'CPU (%)'
TEMPERATURE = 'Temperature (°C)'
FREQUENCY = "Frequency"

ONLY_VMS = {'host'}

PLOT_PERIOD = dict(x=('sample_start', 'sample_end'))
PLOT_FUTURE_MOMENT = dict(x='sample_end')
PLOT_FUTURE_CONST = dict(PLOT_FUTURE_MOMENT, plot_func='step', mode='after')
PLOT_PAST_MOMENT = dict(x='sample_start')
PLOT_PAST_CONST = dict(PLOT_PAST_MOMENT, plot_func='step', mode='before')


def mem_libvirt(d: ExpData):
    return fetch_data(d, y='libvirt:curmem', y_name=MEMORY, **PLOT_FUTURE_MOMENT)


def mem_control(d: ExpData):
    return fetch_data(d, y='controls:memory', y_name=MEMORY, **PLOT_FUTURE_CONST)


def mem_notify(d: ExpData):
    return fetch_data(d, y='notify:memory', y_name=MEMORY, **PLOT_FUTURE_CONST)


def mem_available(d: ExpData):
    return fetch_data(d, y='memory:available', y_name=MEMORY, **PLOT_FUTURE_MOMENT)


def mem_used(d: ExpData):
    return fetch_data(d, y=('memory:available', 'memory:unused'),
                      out='memory:used',
                      out_func=lambda df: df['memory:available'].subtract(df['memory:unused']),
                      y_name=MEMORY, **PLOT_FUTURE_MOMENT)


def mem_app_target(d: ExpData):
    return fetch_data(d, y='app-target:memory', y_name=MEMORY, **PLOT_FUTURE_MOMENT)


def mem_major_fault(d: ExpData):
    return fetch_data(d, y='memory:major_fault', y_name='Faults', diff=True, **PLOT_FUTURE_MOMENT)


def mem_minor_fault(d: ExpData):
    return fetch_data(d, y='memory:minor_fault', y_name='Faults', diff=True, **PLOT_FUTURE_MOMENT)


def mem_swap_out(d: ExpData):
    return fetch_data(d, y='memory:swap_out', y_name='Pages', diff=True, **PLOT_FUTURE_MOMENT)


def mem_unused(d: ExpData):
    return fetch_data(d, y='memory:unused', y_name=MEMORY, filter_groups=ONLY_VMS, **PLOT_FUTURE_MOMENT)


def mem_cache_and_buff(d: ExpData):
    return fetch_data(d, y='memory:cache_and_buff', y_name=MEMORY, filter_groups=ONLY_VMS, **PLOT_FUTURE_MOMENT)


def cpu_qemu(d: ExpData):
    return fetch_data(d, y=('qemu:cpu-total:usage', 'qemu:time'),
                      out='qemu-usage',
                      out_func=lambda df: df['qemu:cpu-total:usage'].diff().div(df['qemu:time'].diff()),
                      y_name=CPU, **PLOT_PAST_CONST)


def cpu_usage_percentage(d: ExpData):
    return fetch_data(d, y=('cpu:cpu-total:total', 'cpu:cpu-total:idle-all', 'cpu:time'),
                      out='cpu-usage-percent',
                      out_func=lambda df: 100 * df['cpu:cpu-total:total'].subtract(
                          df['cpu:cpu-total:idle-all']).div(
                          df['cpu:cpu-total:total']),
                      y_name=CPU_PERCENT, diff=True, **PLOT_PAST_CONST)


def cpu_usage_time(d: ExpData):
    return fetch_data(d, y=('cpu:cpu-total:total', 'cpu:cpu-total:idle-all', 'cpu:time'),
                      out='cpu-usage',
                      out_func=lambda df: df['cpu:cpu-total:total'].subtract(
                          df['cpu:cpu-total:idle-all']).div(
                          df['cpu:time']),
                      y_name=CPU, diff=True, **PLOT_PAST_CONST)


def cpu_usage_time_resample(d: ExpData):
    return fetch_data(d, y=('cpu:cpu-total:total', 'cpu:cpu-total:idle-all', 'cpu:time'),
                      out='cpu-usage',
                      out_func=lambda df: df['cpu:cpu-total:total'].subtract(
                          df['cpu:cpu-total:idle-all']).div(
                          df['cpu:time']),
                      y_name=CPU, diff=True, resample=(60, 1), **PLOT_PAST_CONST)


def cpu_usage_time_i(i: int):
    def ret_func(d: ExpData):
        return fetch_data(d, y=(f'cpu:cpu-{i}:total', f'cpu:cpu-{i}:idle-all', 'cpu:time'),
                          out=f'cpu-usage-{i}',
                          out_func=lambda df: df[f'cpu:cpu-{i}:total'].subtract(
                              df[f'cpu:cpu-{i}:idle-all']).div(
                              df['cpu:time']),
                          y_name=CPU, diff=True, **PLOT_PAST_CONST)
    return ret_func


def app_cpu_time(app_name: str):
    def ret_func(d: ExpData):
        return fetch_data(d, y=(f'{app_name}-stats:cpu:total', f'{app_name}-stats:time'),
                          out=f'{app_name}-cpu-usage',
                          out_func=lambda df: df[f'{app_name}-stats:cpu:total'].div(df[f'{app_name}-stats:time']),
                          y_name=CPU, diff=True, resample=(60, 1), **PLOT_PAST_CONST)
    return ret_func


NET_KEYS = [('receive', 'bytes'), ('receive', 'packets'), ('receive', 'errs'), ('receive', 'drop'), ('receive', 'fifo'),
            ('receive', 'frame'), ('receive', 'compressed'), ('receive', 'multicast'), ('transmit', 'bytes'),
            ('transmit', 'packets'), ('transmit', 'errs'), ('transmit', 'drop'), ('transmit', 'fifo'),
            ('transmit', 'colls'), ('transmit', 'carrier'), ('transmit', 'compressed')]


def net_stat(interface: str, direction: str, col: str):
    def fetch_net_stat(d: ExpData):
        return fetch_data(d, y=f'net:{interface}:{direction}:{col}', y_name=col, diff=True, **PLOT_PAST_CONST)
    return fetch_net_stat


def sensor(socket: int, core: int):
    if core is None:
        key = f'sensors:{socket}:avg'
    else:
        key = f'sensors:{socket}:core{core}:avg'

    def fetch_sensor(d: ExpData):
        return fetch_data(d, y=key, y_name=TEMPERATURE, **PLOT_PAST_MOMENT)

    return fetch_sensor


def cpu_freq(core_id: int):
    def fetch_freq(d: ExpData):
        return fetch_data(d, y=f'cpufreq:{core_id}', y_name=FREQUENCY, **PLOT_PAST_MOMENT)

    return fetch_freq


def ksm_parameter(parameter: str):
    def fetch_ksm_key(d: ExpData):
        return fetch_data(d, y=f'ksm:{parameter}', y_name=parameter, **PLOT_PAST_MOMENT)
    return fetch_ksm_key


def ksm_parameters():
    for p in ['full_scans', 'merge_across_nodes', 'pages_shared', 'pages_sharing', 'pages_to_scan', 'pages_unshared',
              'pages_volatile', 'run', 'sleep_millisecs']:
        yield ksm_parameter(p)

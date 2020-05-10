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
import re
import copy
import bisect
import numpy as np
import pandas as pd
import seaborn as sns
from typing import Dict, List
from collections import namedtuple
import matplotlib.pylab as plt

import cloudexp.results.analyze
from cloudexp.exp import find_all_sub_experiments
from cloudexp.results import line_plot
from cloudexp.results.data import ExpData
from cloudexp.results.vecplot import visualize_2d_vector, visualize_2d_vector_wire, visualize_2d_vector_multiline
from cloudexp.util.timeformat import time_delta
from cloudexp.results.plots import mem_available, mem_control, mem_notify, mem_used, mem_app_target

from elasticbench.exp.memory_profile import MemoryProfile


def read_df(d: ExpData, func, for_group) -> cloudexp.results.analyze.HdfData:
    """ Read DataFrame and metadata """
    return cloudexp.results.analyze.HdfData(func(d), for_group)


def find_mem_changes(mem_info: cloudexp.results.analyze.HdfData):
    mem_eps = 16
    df: pd.DataFrame = mem_info.df
    diff_df: pd.DataFrame = df[mem_info.y_value].diff().fillna(0)
    diff_df = (diff_df.abs() // mem_eps).astype('int32')
    return df.loc[diff_df != 0].index


def find_stable_mem_range(df: pd.DataFrame, x_value, y_value):
    """ Find the longest consecutive rows without memory changes """
    mem_eps = 16
    df = df.reset_index()
    group_by_key = (df[y_value].diff().fillna(0).cumsum() // mem_eps).astype('int32')
    df_groups = df.groupby(group_by_key).aggregate(['count', 'min', 'max'])

    best_group = df_groups[y_value, 'count'] == df_groups[y_value, 'count'].max()
    row = df_groups[best_group].reset_index(drop=True)
    return row[x_value, 'min'][0], row[x_value, 'max'][0], int(row[y_value, 'max'][0])


def _find_stable_perf_ranges_old(df: pd.DataFrame, col: str):
    """ Finds a stable area and return the bounding indexes """
    perf_eps = 1e-6
    past_mean, future_low, future_high = df[f'{col}-past-mean'], df[f'{col}-future-low'], df[f'{col}-future-high']

    left_predicate = (past_mean + perf_eps > future_low) & (past_mean - perf_eps < future_high)
    left_index = df.loc[left_predicate].head(1).index[0]

    future_mean, past_low, past_high = df[f'{col}-future-mean'], df[f'{col}-past-low'], df[f'{col}-past-high']
    right_predicate = (future_mean + perf_eps > past_low) & (future_mean - perf_eps < past_high)
    right_index = df.loc[right_predicate].tail(1).index[0]
    return left_index, right_index


def _find_stable_perf_ranges_second_old(df: pd.DataFrame, col: str, window_sec: int):
    """ Finds a stable area and return the bounding indexes """
    perf_eps = 1e-6
    low_q, high_q = 0.25, 0.75
    low, high = df[col].quantile(low_q), df[col].quantile(high_q)

    while np.isclose(low, high, atol=perf_eps):
        low_q, high_q = low_q - 0.05, high_q + 0.05
        if low_q - perf_eps > 0 and high_q + perf_eps < 1:
            low, high = df[col].quantile(low_q), df[col].quantile(high_q)
        else:
            low = df[col].min()
            high = df[col].max()
            break

    past_mean, future_mean = (df[f'{col}-{s}'] for s in ('past-mean', 'future-mean'))

    left_predicate = (past_mean + perf_eps > low) & (past_mean - perf_eps < high)
    if np.count_nonzero(left_predicate) == 0:
        return None, None
    left_index = df.loc[left_predicate].index[0] #- pd.to_timedelta(window_sec/2, unit='s')

    right_predicate = (future_mean + perf_eps > low) & (future_mean - perf_eps < high)
    if np.count_nonzero(right_predicate) == 0:
        return None, None
    right_index = df.loc[right_predicate].index[-1] #+ pd.to_timedelta(window_sec/2, unit='s')
    return max(df.index[0], left_index), min(df.index[-1], right_index)


def find_stable_perf_ranges(df: pd.DataFrame, col: str, window_sec: int):
    """ Finds a stable area and return the bounding indexes """
    # perf_rel_eps = 1e-6
    # perf_eps = (df[col].max() - df[col].min()) * perf_rel_eps
    l, r = _find_stable_perf_ranges_second_old(df, col, window_sec)
    return l, r
    df = df[l:r]

    past_mean, future_mean = (df[f'{col}-{s}'] for s in ('past-mean', 'future-mean'))
    diff = (past_mean - future_mean).abs().fillna(0)
    diff_threshold = diff.quantile(0.25)
    pass_threshold = diff > diff_threshold

    # pass_threshold = pass_threshold.reset_index(drop=True)
    group_by_key = (pass_threshold.cumsum() // 2).astype('int32')
    # pass_threshold = pass_threshold.reset_index()
    wdf = df.copy()
    x_value = 'idx'
    wdf[x_value] = wdf.index.copy()
    df_groups = wdf.groupby(group_by_key).aggregate(['count', 'min', 'max'])

    row = df_groups[df_groups[col, 'count'] == df_groups[col, 'count'].max()].reset_index(drop=True)
    return row[x_value, 'min'][0], row[x_value, 'max'][0]


def find_overhead_integral_range(df: pd.Series, wdf: pd.Series, lower_mean, upper_mean, upper_std, kind,
                                 sample_rate_sec=0.1, window_sec=15):
    drop = (kind == 'drop')
    perf_rel_eps = 1e-6
    perf_eps = (upper_mean - lower_mean) * perf_rel_eps
    # Find the places the performance is more than upper mean
    # noinspection PyTypeChecker
    # v = wdf.loc[wdf > (upper_mean - (upper_mean-lower_mean)*perf_rel_eps - upper_std*perf_std_eps - perf_eps)]

    # If drop: use the last place that the performance is still within the upper mean range
    # If rise: use the first place that the performance reached the upper mean range
    # i = v.index[-1 if drop else 0]
    if not drop:
        sample_rate_str = f'{int(sample_rate_sec * 1000)}ms'
        window = int(window_sec / sample_rate_sec)
        rdf = df.resample(sample_rate_str).pad()
        df_rolling = rdf.rolling(window, min_periods=1, closed='both')
        wdf = df_rolling.mean()
        # v1 = df.loc[df > upper_mean - perf_eps - upper_std]
        v1 = wdf.loc[wdf > upper_mean - perf_eps - upper_std]
        # v2 = wdf.loc[(wdf.diff() < perf_eps) | (df > wdf)]
        i1 = v1.index[0] if len(v1) > 0 else None
        # i2 = v2.index[0] if len(v2) > 0 else None
        # i = i1 if i2 is None else i2 if i1 is None else min(i1, i2)
        i = i1
        return slice(None, i)
    else:
        v = df.loc[df > upper_mean]
        i = v.index[-1] if len(v) > 0 else None
        return slice(i, None)


def get_overhead_integral(df: pd.Series, lower_mean, upper_mean, sample_rate_sec=0.1):
    """ Calculate the integral of the overhead: EffTmem(t) = integrate(1 - p(x)/p(t)) from 0 to t """
    perf_rel_eps = 1e-6
    perf_eps = (upper_mean - lower_mean) * perf_rel_eps

    t_mem = (df.index[-1] - df.index[0]).total_seconds()
    perf_ref = upper_mean - df + perf_eps
    perf_overhead = perf_ref.sum() * sample_rate_sec
    e_mem = perf_overhead / (upper_mean - lower_mean)

    return df, t_mem, perf_overhead, e_mem


MemStats = namedtuple('MemStats', ['mean', 'std', 'bottom', 'top', 'min', 'max'])


def get_memory_profile(mem_stats: Dict[int, MemStats]):
    mem, stats = zip(*sorted(mem_stats.items()))
    mean, std = zip(*((s.mean, s.std) for s in stats))
    return mem, mean, std


def plot_memory_profile(mem, mean, std, y_label: str, ax=None, **kwargs):
    if ax is None:
        ax = plt.gca()
    ax.errorbar(mem, mean, yerr=std, **kwargs)
    ax.set_xlabel("Memory (MB)")
    ax.set_ylabel(y_label)
    ax.grid(True, linewidth=1, linestyle=":", alpha=0.8)


class ExpAnalyzer:
    def __init__(self, application: str, output_path, fetch_module, for_group, window_sec=15, sample_rate_sec=0.1,
                 warmup_time=None, after_drop=False):
        self.application = application
        self.d = ExpData(output_path)

        self.window_sec = window_sec
        self.sample_rate_sec = sample_rate_sec
        if type(warmup_time) in (list, tuple):
            self.warmup_time = pd.to_timedelta(warmup_time[0], unit=warmup_time[1])
        else:
            self.warmup_time = None

        self.cont = read_df(self.d, mem_control, for_group)
        self.notify = read_df(self.d, mem_notify, for_group)
        self.mem = read_df(self.d, mem_available, for_group)
        self.mem_used = read_df(self.d, mem_used, for_group)
        self.app_target = read_df(self.d, mem_app_target, for_group)
        self.perf = read_df(self.d, getattr(fetch_module, 'perf'), for_group)
        self.mem_alloc = read_df(self.d, getattr(fetch_module, 'memory'), for_group)
        if warmup_time is not None:
            for hdf in (self.cont, self.notify, self.mem, self.mem_used, self.app_target, self.perf, self.mem_alloc):
                hdf.df = hdf.df[self.warmup_time:]

        min_time = self.cont.df.index[0]
        max_time = self.cont.df.index[-1]
        for hdf in (self.cont, self.notify, self.mem, self.mem_used, self.app_target, self.perf, self.mem_alloc):
            min_time = min(min_time, hdf.df.index[0])
            max_time = max(max_time, hdf.df.index[-1])

        self.perf_resample_df = self.perf.resample(window_sec, sample_rate_sec)

        self.mem_change = find_mem_changes(self.cont)
        self.notify_mem_change = find_mem_changes(self.notify)
        self.app_mem_changes = find_mem_changes(self.app_target)

        self.notify_mem_ranges = []
        for n in self.notify_mem_change:
            j = bisect.bisect_right(self.mem_change, n)
            self.notify_mem_ranges.append((n, self.mem_change[j]))

        self.mem_markers = min_time, *self.mem_change, max_time

        self.mem_ranges = list(zip(self.mem_markers, self.mem_markers[1:]))

        self.stable_mem_ranges = [find_stable_mem_range(self.mem.df[l:r], self.mem.x_value, self.mem.y_value)
                                  for l, r in self.mem_ranges]
        self.stable_used_mem_ranges = [find_stable_mem_range(self.mem_used.df[l:r],
                                                             self.mem_used.x_value, self.mem_used.y_value)
                                       for l, r, _ in self.stable_mem_ranges]
        self.expected_stable_perf_ranges = []
        for l, r, m in self.stable_used_mem_ranges:
            j = bisect.bisect_right(self.notify_mem_change, l)
            if j is not None and j < len(self.notify_mem_change):
                r = min(r, self.notify_mem_change[j])
            self.expected_stable_perf_ranges.append((l, r))

        self.stable_transient_app_mem_ranges = [find_stable_mem_range(self.mem_alloc.df[l:r],
                                                                      self.mem_alloc.x_value, self.mem_alloc.y_value)
                                                for l, r in self.notify_mem_ranges]
        self.stable_transient_used_mem_ranges = [find_stable_mem_range(self.mem_used.df[l:r],
                                                                       self.mem_used.x_value, self.mem_used.y_value)
                                                 for l, r in self.notify_mem_ranges]
        self.safe_retreat_time = [(min(r, al, ul) - l).total_seconds() for (l, r), (al, ar, _), (ul, ur, _) in
                                  zip(self.notify_mem_ranges, self.stable_transient_app_mem_ranges,
                                      self.stable_transient_used_mem_ranges)]

        self.mem_ranges_alloc = [m for l, r, m in self.stable_mem_ranges]
        # self.stable_perf_ranges = [
        #     (*find_stable_perf_ranges(self.perf_resample_df[l:r], self.perf.y_value, window_sec), m)
        #     for (_, _, m), (l, r, _) in zip(self.stable_mem_ranges, self.stable_used_mem_ranges)]
        self.stable_perf_ranges = [
            (l, r, m) for (_, _, m), (l, r) in zip(self.stable_mem_ranges, self.expected_stable_perf_ranges)]
        self.mem_to_stable_ranges = {}
        for l, r, m in self.stable_perf_ranges:
            if l is None and r is None:
                continue
            self.mem_to_stable_ranges.setdefault(m, []).append((l, r))

        self.mem_to_stable_slices = {}
        for mem, ranges in self.mem_to_stable_ranges.items():
            self.mem_to_stable_slices[mem] = [self.perf_resample_df[l:r] for l, r in ranges]

        self.slice_perf_stats = [self.perf_slices_stats([self.perf_resample_df[l:r]]) for l, r, _ in
                                 self.stable_perf_ranges]

        self.mem_stats: Dict[int, MemStats] = {}
        for mem, slices in self.mem_to_stable_slices.items():
            self.mem_stats[mem] = self.perf_slices_stats(slices)

        self.overheads = []
        self.apparent_overheads = []
        df = self.perf_resample_df[self.perf.y_value]
        start_stable_used_mem = [l for l, r, m in self.stable_used_mem_ranges]
        for left_range, right_range, left_mem, right_mem in zip(self.mem_ranges, self.mem_ranges[1:],
                                                                self.mem_ranges_alloc,
                                                                self.mem_ranges_alloc[1:]):
            if left_mem not in self.mem_stats or right_mem not in self.mem_stats:
                continue
            lower_stats, upper_stats = [self.mem_stats[m] for m in sorted([left_mem, right_mem])]
            lower_mean, upper_mean, upper_std = lower_stats.mean, upper_stats.mean, upper_stats.std
            if left_mem > right_mem:
                kind = 'drop'
                l, r = left_range
                i = bisect.bisect_right(self.app_mem_changes, r) - 1
                l = self.app_mem_changes[i]
            else:
                kind = 'rise'
                l, r = right_range
                i = bisect.bisect_right(start_stable_used_mem, r) - 1
                r = start_stable_used_mem[i]
            # k = 'past' if kind == 'drop' else 'future'
            # wdf = self.perf_resample_df[f'{self.perf.y_value}-{k}-mean']
            ret = get_overhead_integral(df[l:r], lower_mean, upper_mean, sample_rate_sec)
            self.apparent_overheads.append((*ret, left_mem, right_mem, lower_mean, upper_mean, kind))

            cdf, t_mem, perf_overhead, e_mem = ret
            self.overheads.append([t_mem, perf_overhead, e_mem, left_mem, right_mem, lower_mean, upper_mean, kind])

            if after_drop and kind == 'drop':
                kind = 'after-drop'
                l, r = right_range
                i = bisect.bisect_right(start_stable_used_mem, r) - 1
                r = start_stable_used_mem[i]
                ret = get_overhead_integral(df[l:r], 0, lower_mean, sample_rate_sec)
                self.apparent_overheads.append((*ret, left_mem, right_mem, 0, lower_mean, kind))

                cdf, t_mem, perf_overhead, e_mem = ret
                self.overheads[-1][0] += t_mem
                self.overheads[-1][1] += perf_overhead
                self.overheads[-1][2] = self.overheads[-1][1] / (upper_mean - lower_mean)

        self.overheads_dict = {}
        for (t_mem, _, e_mem, lm, rm, _, _, kind), sr_time in zip(self.overheads, self.safe_retreat_time):
            self.overheads_dict.setdefault((lm, rm), []).append((t_mem, e_mem, sr_time if kind == 'drop' else 0))

        self.overheads_dict_avg = {(lm, rm): tuple(np.mean(list(zip(*v)), axis=1))
                                   for (lm, rm), v in self.overheads_dict.items()}

        self.window_str = time_delta(window_sec, time_annotations="short")

    ##############################################################################################################
    # Data helpers
    ##############################################################################################################

    def perf_range_stats(self, ranges):
        df_slices = []
        for l, r in ranges:
            df_slices.append(self.perf_resample_df[l:r])
        return self.perf_slices_stats(df_slices)

    def perf_slices_stats(self, slices: List[pd.DataFrame]):
        sample_rate_str = f'{int(self.window_sec)}s'
        df = pd.concat(slices)[self.perf.y_value].resample(sample_rate_str).mean()
        mean = df.mean()
        std = df.std()
        bottom = df.quantile(0.25)
        top = df.quantile(0.75)
        # bottom = mean - std
        # top = mean + std
        return MemStats(mean, std, bottom, top, df.min(), df.max())

    def get_perf_ratio(self, left_limit=None, right_limit=None):
        profile = MemoryProfile(self.application)

        mem_ranges = self.mem_ranges
        allocs = self.mem_ranges_alloc
        ranges_time = [(r - l).total_seconds() for l, r in mem_ranges]
        # s, e = self.cont.df.index[0], self.cont.df.index[-1]
        ranges_mean = [self.perf_resample_df[l:r][self.perf.y_value].mean() for l, r in mem_ranges]
        ranges_expected = profile(allocs)

        actual_mean = np.average(ranges_mean[left_limit:right_limit], weights=ranges_time[left_limit:right_limit])
        static_mean = np.average(ranges_expected[left_limit:right_limit], weights=ranges_time[left_limit:right_limit])

        effective_ranges_time = copy.deepcopy(ranges_time)
        for i, (lm, rm) in enumerate(zip(allocs, allocs[1:])):
            if lm == rm:
                continue

            e_mem = profile.e_mem(lm, rm)

            if rm < lm:
                e_mem = -e_mem

            effective_ranges_time[i] += e_mem
            effective_ranges_time[i + 1] -= e_mem

        elastic_mean = np.average(ranges_expected[left_limit:right_limit],
                                  weights=effective_ranges_time[left_limit:right_limit])

        return static_mean/actual_mean, elastic_mean/actual_mean, actual_mean, static_mean, elastic_mean

    ##############################################################################################################
    # Plot functions
    ##############################################################################################################

    def plot_memory_profile(self, ax=None, **kwargs):
        mem, mean, std = get_memory_profile(self.mem_stats)
        plot_memory_profile(mem, mean, std, self.perf.y_name, ax=ax, **kwargs)

    def plot_window_band(self, fig, df: pd.DataFrame, x_value: str, col: str, kind, color, muted=True):
        """ Perf window band plot """
        label = f'Perf. {kind} window ({self.window_str})'
        line = fig.line(source=df, x=x_value, y=f'{col}-{kind}-mean',
                        color=color, legend=label, y_range_name=self.perf.y_name)
        if muted:
            line.visible = False
        area = fig.varea(source=df, x=x_value, y1=f'{col}-{kind}-low', y2=f'{col}-{kind}-high',
                         color=color, alpha=0.1, legend=label, y_range_name=self.perf.y_name)
        # area = fig.varea(x=df.index,
        #                  y1=df[f'{col}-{kind}-mean'] - df[f'{col}-{kind}-std'],
        #                  y2=df[f'{col}-{kind}-mean'] + df[f'{col}-{kind}-std'],
        #                  color=color, alpha=0.1, legend=label, y_range_name=self.perf.y_name)
        if muted:
            area.visible = False

    def plot(self):
        p = line_plot.LinePlot()

        #########################################################################################
        # Raw Data
        #########################################################################################
        p.plot(self.perf, legend='Raw performance', color="#f4428c", auto_line=False)
        p.plot(self.cont, legend='Memory allocation', color="purple", auto_line=False)
        p.plot(self.mem, legend='Memory in VM', color="green", line='dotted', alpha=0.8)
        p.plot(self.mem_used, legend='Used Memory in VM', color="grey", line='dashed', alpha=0.5)
        p.plot(self.mem_alloc, legend='Application Memory', color="blue", line='dashed', alpha=0.5)

        #########################################################################################
        # Perf moving window
        #########################################################################################
        self.plot_window_band(p.fig, self.perf_resample_df, self.perf.x_value, self.perf.y_value, 'past', 'blue')
        self.plot_window_band(p.fig, self.perf_resample_df, self.perf.x_value, self.perf.y_value, 'future', 'orange')

        #########################################################################################
        # Mem Y range
        #########################################################################################
        y_min, y_max = p.ranges.y[self.mem.y_name].as_tuple_extra(0.05)

        def get_segment_y(sz):
            return dict(y0=[y_min] * sz, y1=[y_max] * sz, y_range_name=self.mem.y_name)

        # def get_area_y(sz):
        #     return dict(y1=[y_min] * sz, y2=[y_max] * sz, y_range_name=self.mem.y_name)

        #########################################################################################
        # Mem Segments
        #########################################################################################
        p.fig.segment(x0=self.mem_change, x1=self.mem_change, legend='Memory update',
                      color='purple', line_width=2, line_dash='dotted', **get_segment_y(len(self.mem_change)))

        p.fig.segment(x0=self.notify_mem_change, x1=self.notify_mem_change, legend='Memory notify',
                      color='red', line_width=2, line_dash='dotted', **get_segment_y(len(self.notify_mem_change)))

        p.fig.segment(x0=self.app_mem_changes, x1=self.app_mem_changes, legend='App Memory update',
                      color='blue', line_width=2, line_dash='dotted', **get_segment_y(len(self.app_mem_changes)))

        #########################################################################################
        # Stable memory regions
        #########################################################################################
        mem_extra_height = (y_max - y_min) * 0.02
        for l, r, m in self.stable_mem_ranges:
            label = 'Stable memory region'
            p.fig.varea(x=[l, r], y1=[m - mem_extra_height] * 2, y2=[m + mem_extra_height] * 2,
                        legend=label, y_range_name=self.mem.y_name,
                        color='green', alpha=0.1)

        for l, r, m in self.stable_transient_app_mem_ranges:
            label = 'Stable app memory region'
            p.fig.varea(x=[l, r], y1=[m - mem_extra_height] * 2, y2=[m + mem_extra_height] * 2,
                        legend=label, y_range_name=self.mem.y_name,
                        color='grey', alpha=0.1)

        #########################################################################################
        # Stable performance regions
        #########################################################################################
        try:
            profile = MemoryProfile(self.application)
        except:
            profile = None
        for (l, r, m), range_stats in zip(self.stable_perf_ranges, self.slice_perf_stats):
            if l is None and r is None:
                continue
            label = 'Stable perf. region mean'
            stats = self.mem_stats[m]
            p.fig.segment(x0=[l, r], x1=[l, r], legend=label,
                          y0=[stats.min] * 2, y1=[stats.max] * 2, y_range_name=self.perf.y_name,
                          color='black', line_width=2, line_dash='solid', alpha=0.8)
            p.fig.segment(x0=[l], x1=[r], legend=label,
                          y0=[stats.mean], y1=[stats.mean], y_range_name=self.perf.y_name,
                          color='black', line_width=2, line_dash='solid', alpha=0.8)

            p.fig.segment(x0=[l], x1=[r], legend='Range mean',
                          y0=[range_stats.mean], y1=[range_stats.mean], y_range_name=self.perf.y_name,
                          color='black', line_width=2, line_dash='dotted', alpha=0.8)
            # self.slice_perf_stats
            if profile:
                profile_mean = profile(m)
                p.fig.segment(x0=[l], x1=[r], legend='Expected mean',
                              y0=[profile_mean], y1=[profile_mean], y_range_name=self.perf.y_name,
                              color='black', line_width=2, line_dash='dashed', alpha=0.8)
            p.fig.varea(x=[l, r], legend=label,
                        y1=[stats.min] * 2, y2=[stats.max] * 2, y_range_name=self.perf.y_name,
                        color='grey', alpha=0.1)

        #########################################################################################
        # Memory statistics
        #########################################################################################
        for (ml, mr), m in zip(self.mem_ranges, self.mem_ranges_alloc):
            label = 'Percentile perf. band'
            try:
                stats = self.mem_stats[m]
            except KeyError:
                continue
            p.fig.varea(x=[ml, mr], legend=label,
                        y1=[stats.bottom] * 2, y2=[stats.top] * 2, y_range_name=self.perf.y_name,
                        color='#f4e842', alpha=0.3)

            label = 'Mean perf. band'
            p.fig.varea(x=[ml, mr], legend=label,
                        y1=[stats.mean - stats.std] * 2, y2=[stats.mean + stats.std] * 2,
                        y_range_name=self.perf.y_name,
                        color='#34a1eb', alpha=0.3)

        #########################################################################################
        # Overhead
        #########################################################################################
        for t, t_mem, perf_overhead, e_mem, lm, rm, lower_mean, upper_mean, kind in self.apparent_overheads:
            print(f"{kind}: {lm:>6}->{rm:<6} [Tmem] {t_mem:<6.5g} "
                  f"[Emem] {e_mem:<6.5g} [Perf lost] {perf_overhead:<6.5g}")
            p.fig.varea(x=t.index, y1=t, y2=[upper_mean] * len(t),
                        color="red", alpha=0.3, legend='Overhead', y_range_name=self.perf.y_name)

        for (lm, rm), (t_mem, e_mem, sr_time) in self.overheads_dict_avg.items():
            print(f"{lm:>6}->{rm:<6} [Tmem] {t_mem:<6.5g} [Emem] {e_mem:<6.5g} [Safe Retreat Time] {sr_time:<6.5g}")

        p.show()


class MultiExpAnalyzer:
    def __init__(self, application: str, output_path: str, fetch_module, for_group='vm-1', window_sec=None,
                 sample_rate_sec=0.1, **kwargs):
        self.application = application
        if window_sec is None:
            window_sec = fetch_module.perf_window

        exp_path, exp_sub_path, _ = find_all_sub_experiments(output_path, ignore_backup=True, only_finished=True)
        if len(exp_path) == 0:
            raise ValueError("No experiment results found.")

        self.p = {}
        for p, k in zip(exp_path, exp_sub_path):
            try:
                self.p[k] = ExpAnalyzer(application, p, fetch_module, for_group, window_sec, sample_rate_sec, **kwargs)
            except Exception as e:
                print(k, e)
        self.main = next(iter(self.p.values()))

        self.mem_to_stable_slices = {}
        for ea in self.p.values():
            for mem, slices in ea.mem_to_stable_slices.items():
                self.mem_to_stable_slices.setdefault(mem, []).extend(slices)

        self.mem_stats: Dict[int, MemStats] = {}
        for mem, slices in self.mem_to_stable_slices.items():
            self.mem_stats[mem] = self.main.perf_slices_stats(slices)

        self.overheads_dict = {}
        for ea in self.p.values():
            for (t_mem, _, e_mem, lm, rm, _, _, kind), sr_time in zip(ea.overheads, ea.safe_retreat_time):
                self.overheads_dict.setdefault((lm, rm), []).append(
                    (t_mem, e_mem, sr_time if kind == 'drop' else np.nan))

        self.overheads_dict_avg = {(lm, rm): tuple(np.mean(list(zip(*v)), axis=1))
                                   for (lm, rm), v in self.overheads_dict.items()}

        self.from_mem_values = sorted(self.mem_stats)
        self.to_mem_values = self.from_mem_values[::-1]
        mem_n = len(self.from_mem_values)

        self.t_mem = np.full((mem_n, mem_n), np.nan)
        self.e_mem = np.full((mem_n, mem_n), np.nan)
        self.t_t_mem = np.full((mem_n, mem_n), np.nan)

        self.from_mem = []
        self.diff_mem = []
        self.val_t_mem = []
        self.val_e_mem = []

        from_mem_indexes = {m: i for i, m in enumerate(self.from_mem_values)}
        to_mem_indexes = {m: i for i, m in enumerate(self.to_mem_values)}

        for (lm, rm), (t_mem, e_mem, t_t_mem) in self.overheads_dict_avg.items():
            i = from_mem_indexes[lm]
            j = to_mem_indexes[rm]
            self.t_mem[i, j] = t_mem
            self.e_mem[i, j] = e_mem
            self.t_t_mem[i, j] = t_t_mem

            self.from_mem.append(lm)
            self.diff_mem.append(rm - lm)
            self.val_t_mem.append(t_mem)
            self.val_e_mem.append(e_mem)

        self.diff_mem_values = sorted(set(self.diff_mem))[::-1]

    def __getitem__(self, item):
        return self.p[item]

    def keys(self):
        return self.p.keys()

    def generate_profile(self, perf_value=None):
        MemoryProfile.generate_profile(self.application, self, perf_value=perf_value)
        return MemoryProfile(self.application)

    def get_memory_profile(self):
        mem, mean, std = get_memory_profile(self.mem_stats)
        return mem, mean, std

    def get_memory_perf(self, new_mem):
        mem, mean, std = get_memory_profile(self.mem_stats)
        return np.interp(new_mem, mem, mean)

    def plot_memory_profile(self, ax=None, **kwargs):
        mem, mean, std = get_memory_profile(self.mem_stats)
        plot_memory_profile(mem, mean, std, self.main.perf.y_name, ax=ax, **kwargs)

    def plot_transient_vec(self, vec: np.array, label: str, kind='heatmap', **kwargs):
        if kind == 'heatmap':
            func = visualize_2d_vector
        elif kind == 'wire':
            func = visualize_2d_vector_wire
        else:
            func = visualize_2d_vector_multiline
        return func(vec, d_keys=('From Memory (MB)', 'To Memory (MB)'),
                    val_key=label, d_ticks=(self.from_mem_values, self.to_mem_values), **kwargs)

    def plot_d_transient_vec(self, vec: np.array, label: str, kind='heatmap', **kwargs):
        if kind == 'heatmap':
            func = visualize_2d_vector
        elif kind == 'wire':
            func = visualize_2d_vector_wire
        else:
            func = visualize_2d_vector_multiline
        return func(np.transpose(vec), d_keys=('Change Amplitude (MB)', 'From Memory (MB)'),
                    val_key=label, d_ticks=(self.diff_mem_values, self.from_mem_values), **kwargs)

    def plot_t_mem(self, kind='heatmap', **kwargs):
        return self.plot_transient_vec(self.t_mem, "$T_{mem}$ (Seconds)", kind, **kwargs)

    def plot_t_t_mem(self, kind='heatmap', **kwargs):
        return self.plot_transient_vec(self.t_t_mem, "True $T_{mem}$ (Seconds)", kind, **kwargs)

    def plot_e_mem(self, kind='heatmap', **kwargs):
        return self.plot_transient_vec(self.e_mem, "Effective $T_{mem}$ (Seconds)", kind, **kwargs)

    def plot_d_e_mem(self, kind='heatmap', **kwargs):
        from scipy.interpolate import Rbf
        d_e_mem = Rbf(self.from_mem, self.diff_mem, self.val_e_mem, function='linear', smooth=0)
        mesh = np.meshgrid(self.from_mem_values, self.diff_mem_values, sparse=False, indexing='ij')
        e_mem = d_e_mem(*mesh)
        return self.plot_d_transient_vec(e_mem, "Effective $T_{mem}$ (Seconds)", kind, **kwargs)

    def plot_d_e_mem_multi(self, kind='e-mem', rise=True, **kwargs):
        val = self.val_e_mem if kind == 'e-mem' else self.val_t_mem
        all_from = {}
        for f, d, e in zip(self.from_mem, self.diff_mem, val):
            if not rise:
                d = -d
            if d < 0:
                continue
            all_from.setdefault(f, []).append((d, e))
        all_from = {f: sorted(l) for f, l in all_from.items()}

        for f in sorted(all_from):
            plt.plot(*zip(*all_from[f]), label=f'{f} MB', **kwargs)

        plt.legend(title='From Memory')
        if rise:
            plt.xlabel('Memory Increase (MB)')
        else:
            plt.xlabel('Memory Drop (MB)')
        if kind == 'e-mem':
            plt.ylabel('Effective $T_{mem}$ (Seconds)')
        else:
            plt.ylabel('$T_{mem}$ (Seconds)')

    def iter_split_perf_ratio(self, key, splits=1):
        p = self.p[key]
        range_count = len(p.mem_ranges)
        ranges_in_split = int(range_count // splits)
        n_ranges = [ranges_in_split] * splits
        for i in range(splits):
            if sum(n_ranges) == range_count:
                break
            n_ranges[i] += 1

        n_ranges = np.cumsum([0, *n_ranges])
        for l, r in zip(n_ranges, n_ranges[1:]):
            yield p.get_perf_ratio(l, r)

    def iter_perf_ratio(self, keys=None, splits=1):
        if keys is None:
            keys = list(self.p.keys())
        d = (res for k in keys for res in self.iter_split_perf_ratio(k, splits))
        yield from (np.abs(np.subtract(p[:2], 1) * 100) for p in d)

    def plot_perf_ratio(self, ax1=None, ax2=None, bins=None, nbins=15, keys=None, splits=1, **kwargs):
        static_profiler, elastic_profiler = zip(*self.iter_perf_ratio(keys, splits))

        if ax1 is None:
            ax1 = plt.gca()
        if ax2 is None:
            ax2 = plt.gca()

        if bins is None:
            bins = np.linspace(0, max(max(static_profiler), max(elastic_profiler)), nbins)

        print("Average/Median static  profiler:", np.mean(static_profiler),  np.median(static_profiler))
        print("Average/Median elastic profiler:", np.mean(elastic_profiler), np.median(elastic_profiler))

        # s_hist, bins = np.histogram(static_profiler, bins=bins)
        # e_hist, bins = np.histogram(elastic_profiler, bins=bins)
        #
        # bin_middle = np.array([(l + r) / 2 for l, r in zip(bins, bins[1:])])
        #
        # ax1.bar(bin_middle-0.5, s_hist, label=r'Static profiler',
        #        color='#a8ffcb55', width=2, **kwargs)
        # ax2.bar(bin_middle+0.5, e_hist, label=r'Elastic profiler',
        #        color='#45106bA0', width=2, **kwargs)
        s_hist, s_bins, s_patches = ax1.hist(static_profiler, label=r'Static profiler', bins=bins,
                                             color='#a8ffcb85', **kwargs)
        e_hist, e_bins, e_patches = ax2.hist(elastic_profiler, label=r'Elastic profiler', bins=bins,
                                             color='#45106b85', **kwargs)
        for ax in (ax1, ax2):
            ax.set_xlabel(r'Performance Difference (\%)')
            ax.set_ylabel('Experiment Count')
            # ax.set_yticks(np.arange(max(s_hist.max(), e_hist.max()) + 1))
            ax.legend(frameon=False)
            ax.set_xlim((bins[0], bins[-1]))
        # ax.set_xticks(bin_middle)

    def plot_perf_ratio_violin(self, keys=None, splits=1, figsize=(4, 6), **kwargs):
        if keys is None:
            keys = list(self.p.keys())
        # static_profiler, elastic_profiler = zip(*self.iter_perf_ratio(keys, splits))
        #
        # print("Average/Median static  profiler:", np.mean(static_profiler),  np.median(static_profiler))
        # print("Average/Median elastic profiler:", np.mean(elastic_profiler), np.median(elastic_profiler))

        import re
        pat = re.compile(r'([ac])(\d+)', re.I)

        static = {}
        elastic = {}

        for k, (s, d) in zip(keys, self.iter_perf_ratio(keys, splits)):
            r = {kk: int(dd) for kk, dd in pat.findall(k)}
            a, c = r.get('a', None), r.get('c', None)
            static.setdefault(a, {}).setdefault(c, []).append(s)
            elastic.setdefault(a, {}).setdefault(c, []).append(d)

        amps = sorted(elastic.keys())
        n_amp = len(amps)

        fig, axes = plt.subplots(n_amp+1, 1, figsize=figsize, sharex=True, sharey=True)

        for i, (a, ax) in enumerate(zip(amps, axes)):
            s = static[a]
            d = elastic[a]
            rates = sorted(s.keys())
            parts = ax.violinplot([d[r] for r in rates], **kwargs)
            for pc in parts['bodies']:
                pc.set_facecolor('#D43F3A')
                pc.set_edgecolor('black')
                pc.set_alpha(0.5)
            parts = ax.violinplot([s[r] for r in rates], **kwargs)
            for pc in parts['bodies']:
                pc.set_facecolor('blue')
                pc.set_edgecolor('black')
                pc.set_alpha(0.5)

        # s_hist, bins = np.histogram(static_profiler, bins=bins)
        # e_hist, bins = np.histogram(elastic_profiler, bins=bins)
        #
        # bin_middle = np.array([(l + r) / 2 for l, r in zip(bins, bins[1:])])
        #
        # ax1.bar(bin_middle-0.5, s_hist, label=r'Static profiler',
        #        color='#a8ffcb55', width=2, **kwargs)
        # ax2.bar(bin_middle+0.5, e_hist, label=r'Elastic profiler',
        #        color='#45106bA0', width=2, **kwargs)
        # s_hist, s_bins, s_patches = ax1.hist(static_profiler, label=r'Static profiler', bins=bins,
        #                                      color='#a8ffcb85', **kwargs)
        # e_hist, e_bins, e_patches = ax2.hist(elastic_profiler, label=r'Elastic profiler', bins=bins,
        #                                      color='#45106b85', **kwargs)
        # for ax in (ax1, ax2):
        #     ax.set_xlabel(r'Performance Difference (\%)')
        #     ax.set_ylabel('Experiment Count')
        #     # ax.set_yticks(np.arange(max(s_hist.max(), e_hist.max()) + 1))
        #     ax.legend(frameon=False)
        #     ax.set_xlim((bins[0], bins[-1]))
        # # ax.set_xticks(bin_middle)

    def plot_perf_ratio_violin_sns(self, keys=None, splits=1, figsize=(3, 6), lgd_rise=0.95,
                                   exclude_rate=(), exclude_amp=(), show_joined=True, transpose=False,
                                   nbins=6, **kwargs):
        if keys is None:
            keys = list(self.p.keys())

        import re
        pat = re.compile(r'([ac])(\d+)', re.I)
        df = []
        for k, (s, d) in zip(keys, self.iter_perf_ratio(keys, splits)):
            r = {kk: int(dd) for kk, dd in pat.findall(k)}
            a, c = r.get('a', None), r.get('c', None)
            if c in exclude_rate or a in exclude_amp:
                continue
            df.append([k, s, 'Static', a, c])
            df.append([k, d, 'Elastic', a, c])
            if show_joined:
                df.append([k, s, 'Static', a, 0])
                df.append([k, d, 'Elastic', a, 0])
                df.append([k, s, 'Static', 0, c])
                df.append([k, d, 'Elastic', 0, c])
                df.append([k, s, 'Static', 0, 0])
                df.append([k, d, 'Elastic', 0, 0])
        df = pd.DataFrame(df, columns=['key', 'Perf', 'Profiler', 'Amplitude', 'Rate'])

        amps = list(reversed(sorted(set(df['Amplitude']))))
        rates = sorted(set(df['Rate']))
        if show_joined:
            rates.pop(0)
            rates.append(0)
        n_amp = len(amps)
        n_rate = len(rates)

        if not transpose:
            n_rows, n_cols = n_amp, n_rate
        else:
            figsize = figsize[1], figsize[0]
            n_rows, n_cols = n_rate, n_amp

        fig, axes_grid = plt.subplots(n_rows, n_cols, figsize=figsize, sharex=True, sharey=True, squeeze=False)
        plt.subplots_adjust(left=None, bottom=None, right=None, top=None, wspace=0, hspace=0)

        palette = ['#a0ffc9', '#413699']

        def right_label(label):
            ax.text(1.03, 0.5, label, transform=ax.transAxes, ha='left', va='center', rotation=90)

        def bottom_label(label):
            ax.text(0.5, -0.03, label, transform=ax.transAxes, ha='center', va='top')

        def rate_label(rate):
            return f'{rate}' if rate != 0 else 'Joined'

        def amp_label(amp):
            return f'{amp}MB' if amp != 0 else 'Joined'

        for i, a in enumerate(amps):
            for j, c in enumerate(rates):
                if not transpose:
                    row, col = i, j
                else:
                    row, col = j, i
                ax = axes_grid[row, col]
                sns.violinplot(x='Rate', y='Perf', hue='Profiler',
                               data=df.loc[(df['Amplitude'] == a) & (df['Rate'] == c)], split=True, ax=ax,
                               palette=palette, linewidth=1, bw=0.5, inner=None, **kwargs)
                ax.legend_.remove()
                if a == 0 and c == 0:
                    ax.set_facecolor('#AAAAAA')
                elif a == 0 or c == 0:
                    ax.set_facecolor('#DDDDDD')

                ax.xaxis.set_visible(False)

                if col == 0:
                    ax.set_ylabel('')
                else:
                    ax.yaxis.set_visible(False)
                if n_cols > 1 and col == n_cols - 1:
                    right_label(amp_label(a) if not transpose else rate_label(c))
                if n_rows > 1 and row == n_rows - 1:
                    bottom_label(rate_label(c) if not transpose else amp_label(a))

                if n_rows > 1 and row == n_rows - 1 and col == 0:
                    t = 0.08 if not transpose else 0.04
                    label = 'Memory Changes per Hour' if not transpose else 'Memory Amplitude'
                    ax.text(0.5, t, label, transform=fig.transFigure, ha='center', va='top')
                if n_cols > 1 and col == n_cols - 1 and row == 0:
                    t = 1.0 if not transpose else 0.95
                    label = 'Memory Amplitude' if not transpose else 'Memory Changes per Hour'
                    ax.text(t, 0.5, label, transform=fig.transFigure, ha='left', va='center', rotation=90)
                if row == 0 and col == 0:
                    t = 0 if not transpose else 0.06
                    ax.text(t, 0.5, r'Performance Difference (\%)', transform=fig.transFigure,
                            ha='right', va='center', rotation=90)

        lgd = axes_grid[0, 0].legend(loc='upper center', bbox_to_anchor=(0.5, lgd_rise), ncol=3,
                                     frameon=False, bbox_transform=plt.gcf().transFigure, columnspacing=0.5)
        plt.locator_params(axis='y', nbins=nbins)
        return lgd

    def plot_perf_ratio_diff(self, ax=None, bins=None, nbins=15,  keys=None, splits=1, **kwargs):
        static_profiler, elastic_profiler = zip(*self.iter_perf_ratio(keys, splits))
        diff = np.subtract(static_profiler, elastic_profiler)

        if ax is None:
            ax = plt.gca()

        if bins is None:
            bins = nbins

        print("Average improvement:", np.mean(diff))
        print(" Median improvement:", np.median(diff))

        n, bins, patches = ax.hist(diff, alpha=0.2, label=r'Profiler improvement (\%)', bins=bins, **kwargs)
        ax.set_xlabel(r'Elastic profiler improvement difference (\%)')
        ax.set_ylabel('Experiment Count')

        ax.set_yticks(np.arange(max(n)))

        # ax.legend(frameon=False)

    def get_perf_ratio_data(self):
        keys = list(self.p.keys())
        pat = re.compile(r'a(\d+)-c(\d+)', re.I)
        params = [tuple(map(int, pat.search(k).groups())) for k in keys]
        d = [(a, c, p1, p2) for (a, c), (p1, p2) in zip(params, self.iter_perf_ratio(keys=keys))]
        return pd.DataFrame(d, columns=['Amplitude', 'Frequency', 'Static', 'Elastic'])

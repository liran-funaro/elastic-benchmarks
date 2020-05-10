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
import itertools
from typing import Callable

import pandas as pd
from bokeh import plotting
from bokeh.io import save, show
from collections import defaultdict
from bokeh.models import Range1d, LinearAxis, DatetimeTickFormatter
# noinspection PyUnresolvedReferences
from bokeh.palettes import Set1_9

from cloudexp.results.analyze import HdfData, get_hdf_groups
from cloudexp.results.data import ExpData

COLORS = Set1_9
MARKERS = 'asterisk', 'circle', 'diamond', 'cross', 'square', 'triangle', 'inverted_triangle'
LINES = 'solid', 'dashed', 'dotted'


class AxisRange:
    def __init__(self, force_min=None, force_max=None):
        self.min = force_min
        self.max = force_max

    def update(self, new_min, new_max):
        if self.min is None:
            self.min = new_min
        else:
            self.min = min(self.min, new_min)

        if self.max is None:
            self.max = new_max
        else:
            self.max = max(self.max, new_max)

    def update_data_frame(self, df: pd.DataFrame, col_name):
        if df.index.name == col_name:
            self.update(df.index[0], df.index[-1])
        else:
            self.update(df[col_name].min(), df[col_name].max())

    def as_tuple(self):
        return self.min, self.max

    def as_range(self):
        return Range1d(*self.as_tuple())

    def as_tuple_extra(self, extra=0.1):
        return self.min * (1-extra), self.max * (1+extra)

    def as_range_extra(self, extra=0.1):
        return Range1d(*self.as_tuple_extra(extra))


class PlotRanges:
    def __init__(self):
        self.x_range = AxisRange()
        self.y_range = defaultdict(lambda: AxisRange(force_min=0))

    def update_data_frame(self, df: pd.DataFrame, x_col_name, y_col_name, y_axis_name):
        self.x_range.update_data_frame(df, x_col_name)
        self.y_range[y_axis_name].update_data_frame(df, y_col_name)

    @property
    def x(self):
        return self.x_range

    @property
    def y(self):
        return self.y_range

    def finalize_fig(self, fig, main_y_axis):
        fig.legend.click_policy = "hide"
        fig.legend.location = "bottom_right"

        fig.x_range = self.x.as_range()
        fig.xaxis.formatter = DatetimeTickFormatter()
        fig.xaxis[0].ticker.desired_num_ticks = 14

        fig.y_range = self.y[main_y_axis].as_range_extra(0.1)
        fig.extra_y_ranges = {y_name: y_range.as_range_extra(0.05) for y_name, y_range in self.y.items()}
        fig.yaxis.axis_label = main_y_axis
        for y_name in self.y:
            if y_name == main_y_axis:
                continue
            fig.add_layout(LinearAxis(y_range_name=y_name, axis_label=y_name), 'left')


class CycleGroup:
    def __init__(self, items):
        self.cycle = itertools.cycle(items)
        self.index = {}

    def __getitem__(self, item):
        if item is None:
            return next(self.cycle)

        cur_item = self.index.get(item, None)
        if cur_item is None:
            cur_item = next(self.cycle)
            self.index[item] = cur_item
        return cur_item


class LinePlot:
    def __init__(self, color_group=None, marker_group=None, line_group=None):
        self.fig = new_fig()
        # possible_group: y_value, y_name, group
        self.color_cycle = CycleGroup(COLORS)
        self.marker_cycle = CycleGroup(MARKERS)
        self.line_cycle = CycleGroup(LINES)

        self.color_group = color_group
        self.marker_group = marker_group
        self.line_group = line_group

        self.ranges = PlotRanges()
        self.main_y_axis = None

    def color(self, hdf: HdfData):
        return self.color_cycle[hdf.metadata.get(self.color_group, None)]

    def marker(self, hdf: HdfData):
        if hdf.is_resample is not None:
            return None
        else:
            return self.marker_cycle[hdf.metadata.get(self.marker_group, None)]

    def line(self, hdf: HdfData):
        return self.line_cycle[hdf.metadata.get(self.line_group, None)]

    def get_line_properties(self, hdf: HdfData):
        return self.color(hdf), self.marker(hdf), self.line(hdf)

    def plot(self, hdf: HdfData, legend=None, color=None, marker=None, line=None,
             auto_color=True, auto_marker=True, auto_line=True, **kwargs):
        if self.main_y_axis is None:
            self.main_y_axis = hdf.y_name

        if color is None and auto_color:
            color = self.color(hdf)
        if marker is None and auto_marker:
            marker = self.marker(hdf)
        if line is None and auto_line:
            line = self.line(hdf)

        if color is not None:
            kwargs['line_color'] = color
        if line is not None:
            kwargs['line_dash'] = line

        y_value = hdf.y_value
        if hdf.is_resample:
            y_value = f'{hdf.y_value}-mean'
            self.fig.varea(source=hdf.df, x=hdf.x_value, y1=f'{hdf.y_value}-low', y2=f'{hdf.y_value}-high',
                           color=color, alpha=0.1, legend=legend, y_range_name=hdf.y_name)

        cur_plot_func = getattr(self.fig, hdf.plot_func_name)
        cur_plot_func(source=hdf.df, x=hdf.x_value, y=y_value, line_width=2, y_range_name=hdf.y_name,
                      legend=legend, muted_alpha=0.2, **hdf.plot_kwargs, **kwargs)

        if marker is not None:
            cur_marker_func = getattr(self.fig, marker)
            cur_marker_func(source=hdf.df, x=hdf.x_value, y=hdf.y_value, y_range_name=hdf.y_name,
                            legend=legend, fill_color="white", color=color, size=8)

        self.update_ranges(hdf)

    def plot_df(self, df: pd.DataFrame, units=None):
        cols = df.columns
        if units is None:
            units = cols
        for c, u in zip(cols, units):
            if self.main_y_axis is None:
                self.main_y_axis = u
            color = self.color_cycle[c]
            line = self.line_cycle[c]

            self.fig.line(x=df.index, y=df[c], line_width=2, y_range_name=u,
                          legend=c, muted_alpha=0.2, line_color=color, line_dash=line)
            self.ranges.update_data_frame(df, df.index.name, c, u)

    def update_ranges(self, hdf: HdfData):
        self.ranges.update_data_frame(hdf.df, hdf.x_value, hdf.y_value, hdf.y_name)

    def show(self):
        self.ranges.finalize_fig(self.fig, self.main_y_axis)
        show(self.fig)


def new_fig():
    return plotting.figure(sizing_mode='stretch_both',
                           tools=['hover', 'crosshair', 'wheel_zoom', 'box_zoom', 'pan', 'save', 'reset'],
                           active_scroll='wheel_zoom')


def store_fig(html_file_path, fig, title=None):
    plotting.output_file(html_file_path, title=title, mode='inline', root_dir=None)
    save(fig)


def plot_funcs(data: ExpData, *funcs: Callable[[ExpData], str], output_filename=None, **kwargs):
    if output_filename is not None:
        plotting.output_file(data.export_file_path(output_filename, ext='html'), title=output_filename)
    hdf_files = [f(data) for f in funcs]
    return plot(*hdf_files, **kwargs)


def plot(*hdf_files, color_group=None, marker_group=None, line_group=None, for_group=None, **kwargs):
    p = LinePlot(color_group, marker_group, line_group)

    for f_path in hdf_files:
        if for_group is None:
            hdf_groups = get_hdf_groups(f_path)
        else:
            hdf_groups = [for_group]
        for group in hdf_groups:
            hdf = HdfData(f_path, group)

            if for_group is None:
                legend = f"{group}: {hdf.y_value}"
            else:
                legend = f"{hdf.y_value} "

            p.plot(hdf, legend=legend, **kwargs)

    p.show()

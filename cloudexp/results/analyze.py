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
import os
import warnings
from typing import Optional, Set, Dict, Union, Iterable, Callable, Tuple

import numpy as np
import pandas as pd
from tables import NaturalNameWarning

from cloudexp.results.data import ExpData


def get_sql_query(*read_keys, group_by=None, filter_groups=None):
    read_keys_str = ', '.join((f"`{k}`" for k in read_keys))
    read_keys_condition_str = ' and '.join((f"`{k}` != ''" for k in read_keys))
    if group_by is not None and filter_groups is not None:
        filter_groups_condition_str = " and ".join((f"`{group_by}` != '{k}'" for k in filter_groups))
        read_keys_condition_str = f'{read_keys_condition_str} and {filter_groups_condition_str}'
    return f"select {read_keys_str} from data where {read_keys_condition_str} order by `{read_keys[0]}` asc"


def remove_file_unsafe(file_path):
    try:
        os.remove(file_path)
    except:
        pass


def group_frame_by(df: pd.DataFrame, group_by: str, filter_values: Optional[Set] = None):
    ret = {}
    for group, sub_df in df.groupby(group_by):
        if filter_values and group in filter_values:
            continue
        del sub_df[group_by]
        ret[group] = sub_df
    return ret


def store_frames(hdf_file_path, df_dict: Dict[str, pd.DataFrame], **plot_kwargs):
    remove_file_unsafe(hdf_file_path)

    with pd.HDFStore(hdf_file_path) as store:
        try:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore', category=NaturalNameWarning)
                for group, sub_df in df_dict.items():
                    key = str(group)
                    store.put(key, sub_df, append=False)
                    store.get_storer(key).attrs.metadata = plot_kwargs
        except:
            remove_file_unsafe(hdf_file_path)
            raise


def remove_na_rows(s: pd.Series, col):
    return s.loc[s[col].notna()]


def adjust_index_time(s: pd.Series, sec: Union[int, float]):
    s.index = s.index - pd.to_timedelta(sec, unit='s')
    return s


def resample_df(df: pd.DataFrame, col: str, window_sec=15, sample_rate_sec=0.1) -> pd.DataFrame:
    """ Resample a DataFrame and add   future-window and past-window columns """
    sample_rate_str = f'{int(sample_rate_sec * 1000)}ms'
    window = int(window_sec/sample_rate_sec)
    fake_entry = df.loc[df.index[-1]].copy()
    fake_entry[col] = np.NaN
    fake_entry.name = df.index[-1] + pd.to_timedelta(window_sec, unit='s')
    df = df.append(fake_entry)
    rdf = df.resample(sample_rate_str).pad()
    df_rolling = rdf.rolling(window, min_periods=1, closed='both')

    # Get past rolling stats (default)
    series = {
        'past': {
            'mean': df_rolling.mean(),
            'std': df_rolling.std(),
            'low': df_rolling.quantile(0.25),
            'high': df_rolling.quantile(0.75),
        }
    }

    # Remove NaN values
    series['past'] = {k: remove_na_rows(s, col) for k, s in series['past'].items()}

    # Create future rolling stats by shifting the index
    series['future'] = {k: adjust_index_time(s.copy(), window_sec) for k, s in series['past'].items()}

    # Create center rolling stats by shifting the index
    series['center'] = {k: adjust_index_time(s.copy(), window_sec / 2) for k, s in series['past'].items()}

    # Clip data such that future and past will have identical index
    left_index, right_index = series['past']['mean'].index[0], series['future']['mean'].index[-1]
    for t in ('past', 'future'):
        series[t] = {k: s[left_index:right_index] for k, s in series[t].items()}

    # Store data in the re-sampled DataFrame
    for t, cur_series in series.items():
        for k, s in cur_series.items():
            rdf[f'{col}-{t}-{k}'] = s

    return rdf


def fetch_data(data: ExpData, x: Union[str, Tuple[str, str]], y: Union[Iterable[str], str], group_by: str = 'source',
               out: Optional[str] = None, out_func: Optional[Callable] = None, resample=None,
               filter_groups: Optional[Set] = None, x_end_value: Optional[str] = None,
               x_as_time=True, diff=False, **plot_kwargs):
    if isinstance(y, str):
        y = (y,)
    else:
        y = tuple(y)

    if isinstance(x, tuple):
        x, x_end_value = x

    all_values = [x, *y, group_by]
    if x_end_value:
        all_values.append(x_end_value)

    with data.db_connection() as conn:
        full_df = pd.read_sql_query(get_sql_query(*all_values, group_by=group_by, filter_groups=filter_groups), conn)

    if x_end_value:
        x = f'{x}-{x_end_value}'
        full_df = pd.melt(full_df, id_vars=[*y, group_by],
                          value_name=x).drop('variable', axis=1).sort_values(x)

    if x_as_time:
        full_df[x] = pd.to_timedelta(full_df[x], unit='seconds')
        full_df.set_index(x, inplace=True)

    dfs = group_frame_by(full_df, group_by, filter_groups)

    if diff:
        for sub_df in dfs.values():
            for col in y:
                sub_df[col] = sub_df[col].diff()

    if out_func and out:
        for sub_df in dfs.values():
            sub_df[out] = out_func(sub_df)
    else:
        out = y[-1]

    for sub_df in dfs.values():
        sub_df.drop(set(sub_df.columns) - {x, out}, axis=1, inplace=True)

    if resample is not None:
        window_sec, sample_rate_sec = resample
        for k, df in list(dfs.items()):
            dfs[k] = resample_df(df, out, window_sec, sample_rate_sec)

    hdf_file_path = data.export_file_path(x, out, ext='h5')
    store_frames(hdf_file_path, dfs, x_value=x, y_value=out, resample=resample, **plot_kwargs)
    return hdf_file_path


class HdfData:
    def __init__(self, hdf_file, for_group):
        """ Read DataFrame and metadata """
        self.hdf_file = hdf_file
        self.for_group = for_group

        with pd.HDFStore(hdf_file) as store:
            self.df = store[for_group]
            self.metadata = store.get_storer(for_group).attrs.metadata

        self.plot_kwargs = self.metadata.copy()
        self.x_value = self.plot_kwargs.pop('x_value')
        self.y_value = self.plot_kwargs.pop('y_value')
        self.y_name = self.plot_kwargs.pop('y_name', self.y_value)
        self.plot_func_name = self.plot_kwargs.pop('plot_func', 'line')
        self.is_resample = self.plot_kwargs.pop('resample', False)

    @property
    def x_column(self):
        return np.array(self.df.index)

    @property
    def y_column(self):
        return np.array(self.df[self.y_value])

    def resample(self, window_sec=15, sample_rate_sec=0.1) -> pd.DataFrame:
        return resample_df(self.df, self.y_value, window_sec, sample_rate_sec)


def get_hdf_groups(hdf_file):
    with pd.HDFStore(hdf_file) as store:
        return list(map(lambda k: k[1:], store))


def multiple_hdf_data(data: ExpData, for_group, *funcs: Callable[[ExpData], str]):
    hdf_files = [f(data) for f in funcs]
    return {func.__name__: HdfData(f_path, for_group) for func, f_path in zip(funcs, hdf_files)}


def join_multiple_data(data: ExpData, for_group, *funcs: Callable[[ExpData], str], sample_rate_sec=0.1):
    hdf_files = [f(data) for f in funcs]
    hdf_data = [HdfData(f, for_group) for f in hdf_files]

    main_df = hdf_data[0]
    sample_rate_str = f'{int(sample_rate_sec * 1000)}ms'
    main_df_resample = main_df.df.resample(sample_rate_str).pad()
    main_index = main_df_resample.index
    hdf_resample = [h.df.reindex(main_index, method='pad') for h in hdf_data]

    df = pd.DataFrame({h.y_value: r[h.y_value] for h, r in zip(hdf_data, hdf_resample)}, index=main_index)
    units = [h.y_name for h in hdf_data]
    return df, units

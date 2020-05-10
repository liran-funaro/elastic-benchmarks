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
import numpy as np
import matplotlib.pylab as plt

from nesteddict import NestedDictFS

from cloudexp.results.vecplot import visualize_2d_vector

from elasticbench import settings


PROFILE_NAME = 'mem-profile'


class MemoryProfile:
    required_fields = 'mem', 'perf', 'std', 'from-mem', 'to-mem', 't-mem', 'e-mem'

    def __init__(self, application):
        self.application = application
        self.store = NestedDictFS(settings.output_path(PROFILE_NAME, application), mode='r')
        missing_fields = {f for f in self.required_fields if f not in self.store}
        if missing_fields:
            raise ValueError(f"Missing fields: {missing_fields}")

        from scipy.interpolate import Rbf
        from_mem, to_mem = np.meshgrid(self.store['from-mem'], self.store['to-mem'], sparse=False, indexing='ij')
        self.e_mem_func = Rbf(from_mem, to_mem, np.nan_to_num(self.store['e-mem']), function='linear', smooth=0)
        self.t_mem_func = Rbf(from_mem, to_mem, np.nan_to_num(self.store['t-mem']), function='linear', smooth=0)

    def __repr__(self):
        return f'{self.__class__.__name__}({self.application})'

    @staticmethod
    def generate_profile(application: str, analyzer, perf_value=None):
        store = NestedDictFS(settings.output_path(PROFILE_NAME, application), mode='c')
        mem, mean, std = analyzer.get_memory_profile()
        store['mem'] = mem
        store['perf'] = mean
        store['std'] = std
        if perf_value is None:
            perf_value = analyzer.main.perf.y_name
        store['value'] = perf_value

        store['from-mem'] = analyzer.from_mem_values
        store['to-mem'] = analyzer.to_mem_values
        store['t-mem'] = analyzer.t_mem
        store['e-mem'] = analyzer.e_mem

    def plot_profile(self, ax=None, perf_transform=None, perf_fmt=None, annotate=True, **kwargs):
        if ax is None:
            ax = plt.gca()

        mem = self.store['mem']
        perf = self.store['perf']
        std = self.store['std']

        print("IFGB (perf per GB) =", (perf[-1]/perf[0]) / ((mem[-1] - mem[0])/1024))

        if perf_transform is not None:
            perf = perf_transform(np.array(perf))
            std = perf_transform(np.array(std))
        ax.errorbar(mem, perf, yerr=std, **kwargs)
        ax.set_xlabel("Memory (MB)")
        ax.set_ylabel(self.store['value'])
        ax.set_ylim(0, perf[-1]*1.1)
        plt.locator_params(axis='y', nbins=6)

        mid_perf = (perf[0] + perf[-1]) / 2
        if annotate:
            ax.annotate(f'$mem_L$', (mem[0], perf[0]), (mem[0], mid_perf),
                        arrowprops=dict(facecolor='black', shrink=0.1, width=2),
                        horizontalalignment='center', verticalalignment='center'
                        )
            ax.annotate(f'$mem_H$', (mem[-1], perf[-1]), (mem[-1], mid_perf),
                        arrowprops=dict(facecolor='black', shrink=0.1, width=2),
                        horizontalalignment='center', verticalalignment='center'
                        )

        ax.set_xticks(mem[1:])
        ax.set_xticks(mem, minor=True)
        # ax.set_xticklabels(list(map(str, mem)), rotation=90)

        if perf_fmt is not None:
            y_ticks = ax.get_yticks()
            ax.set_yticklabels(list(map(perf_fmt, y_ticks)))
        ax.grid(True, linewidth=1, linestyle=":", alpha=0.8)
        ax.grid(True, linewidth=1, linestyle=":", alpha=0.8, which='minor')

        return ax

    def plot_transient_vec(self, vec: np.array, label: str, **kwargs):
        return visualize_2d_vector(vec, d_keys=('From Memory (MB)', 'To Memory (MB)'),
                                   val_key=label, d_ticks=(self.store['from-mem'], self.store['to-mem']), **kwargs)

    def plot_t_mem(self, **kwargs):
        return self.plot_transient_vec(self.store['t-mem'], "$T_{mem}$ (Seconds)", **kwargs)

    def plot_e_mem(self, **kwargs):
        return self.plot_transient_vec(self.store['e-mem'], "$E_{mem}$ (Seconds)", **kwargs)

    def plot_e_mem_reduction(self, other_profile: 'MemoryProfile', **kwargs):
        e_mem_reduce = (1 - other_profile.store['e-mem'] / self.store['e-mem']) * 100
        return self.plot_transient_vec(e_mem_reduce, r"$E_{mem}$ Reduction (\%)", **kwargs)

    def __call__(self, mem):
        return np.interp(mem, self.store['mem'], self.store['perf'])

    def e_mem(self, from_mem, to_mem):
        return self.e_mem_func(from_mem, to_mem)

    def t_mem(self, from_mem, to_mem):
        return self.t_mem_func(from_mem, to_mem)

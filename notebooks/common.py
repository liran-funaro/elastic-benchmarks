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
# Generic imports
import os
import sys
import time

# Scientific imports
import scipy
import numpy as np
import pandas as pd

# Plot imports
import bokeh.io
from bokeh.io import show
import seaborn as sns
import matplotlib as mpl
import matplotlib.pyplot as plt

import mom
import cloudexp
import elasticbench

from cloudexp.results import plots
from cloudexp.results import analyze
from cloudexp.results.data import ExpData
from cloudexp.exp.batch import batch_progress
from cloudexp.results.exp_analyzer import ExpAnalyzer, MultiExpAnalyzer
from cloudexp.results.line_plot import new_fig, plot_funcs, plot, LinePlot

from elasticbench import settings
from elasticbench.exp import test as test_exp

from elasticbench.apps.memory_consumer import exp as mc_exp, plots as mc_plots
from elasticbench.apps.memcached import exp as mcd_exp, plots as mcd_plots
from elasticbench.apps.iperf import exp as iperf_exp, plots as iperf_plots
from elasticbench.apps.stress import exp as stress_exp, plots as stress_plots
from elasticbench.apps.postgresql import exp as psql_exp, plots as psql_plots


def __fake_usage__():
    return (os, sys, time, scipy, np, pd, show, sns, mpl, plt, mom, cloudexp, elasticbench, batch_progress,
            ExpData, ExpAnalyzer, MultiExpAnalyzer, analyze, new_fig,
            plot_funcs, plot, LinePlot, plots, settings, test_exp, mc_exp, mc_plots, mcd_exp, mcd_plots, iperf_exp,
            iperf_plots, stress_exp, stress_plots, psql_exp, psql_plots)


def init_plot(factor=1.3):
    default_dpi = mpl.rcParamsDefault['figure.dpi']
    mpl.rcParams['figure.dpi'] = default_dpi * factor


# Init bokeh to output to the notebook
bokeh.io.reset_output()
bokeh.io.output_notebook()

# Init matplotlib
init_plot(1.5)

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
from cloudexp.exp import save_experiment_args
from cloudexp.guest.allocations import memory, loads
from cloudexp.guest.application import NoApplication
from cloudexp.guest.application.benchmark import NoBenchmark

from elasticbench import settings
from elasticbench.exp import TEST_EXP_TYPE, MAX_MEM_DIFF


def make_test(exp_name='fast'):
    rel_path, output_path = settings.get_output_path_and_relative(TEST_EXP_TYPE, exp_name)
    mem_func = memory.mem_6G

    exp_kwargs = dict(
        exp_config=None,  # default
        host_mom_config={('guest-monitor', 'interval'): 3},
        output_path=output_path,
        vms_desc={
            'vm-1': dict(
                application=NoApplication(),
                benchmark=NoBenchmark(),
                guest_mom_config={('policy', 'response-scripts'): {
                    'memory': mem_func
                }},
                load_func=loads.constant_load(1),
                max_mem=mem_func.get_max_value() + MAX_MEM_DIFF,
                base_mem=mem_func.get_min_value(),
                load_interval=10,
            ),
        },
        duration=30,
        extra_info=dict(
            type=TEST_EXP_TYPE,
            exp_name=exp_name,
        )
    )

    save_experiment_args(**exp_kwargs)
    return settings.linkify_to_monitor(rel_path)

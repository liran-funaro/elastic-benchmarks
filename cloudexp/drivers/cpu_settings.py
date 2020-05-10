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
import copy
import itertools
import os
import re
import threading
from typing import List, Dict, Union

from cloudexp.util import shell
from mom.logged_object import LoggedObject

# lscpu -p : CPU,Core,Socket,Node,,L1d,L1i,L2,L3
lscpu_structure = re.compile(r"^(?P<CPU>\d+),(?P<Core>\d*),(?P<Socket>\d*),(?P<Node>\d*),,"
                             r"(?P<L1d>\d*),(?P<L1i>\d*),(?P<L2>\d*),(?P<L3>\d*)",
                             re.IGNORECASE | re.MULTILINE)


def fetch_cpu_data(offline=False):
    """
    Retrieve the CPUs in the host machine with the following info:
        CPU, Core, Socket, Node, L1d, L1i, L2, L3 (arranged by CPU id)
    """
    cmd = "lscpu -p"
    if offline:
        cmd += " -c"
    lscpu_raw = os.popen(cmd).read()
    cpus_info: List[Dict[str, Union[str, int, None]]] = [m.groupdict() for m in lscpu_structure.finditer(lscpu_raw)]

    # Convert all the data to integer
    for cpu in cpus_info:
        for key, value in cpu.items():
            if value:
                cpu[key] = int(value)
            else:
                cpu[key] = None

    return sorted(cpus_info, key=lambda x: x["CPU"])


class CpuData(LoggedObject):
    """ Manage the the machine's CPUs """

    def __init__(self):
        """ Constructor is called only once (singleton) """
        # Allows reentrant by the same thread
        LoggedObject.__init__(self)
        self._lock = threading.RLock()

        self._cpu_data = fetch_cpu_data()

        self._cpu_hierarchy = self.subset("Node", "Socket", "Core", "CPU")

        self._machine_core_subset = {0: self.subset("Core")}
        self._node_core_subset = self.subset("Node", "Core")
        self._socket_core_subset = self.subset("Socket", "Core")
        self._core_core_subset = self.subset("Core", "Core")

        self._core_cpu_subset = self.subset("Core", "CPU")

        self._machine_availability = {0: len(self._cpu_data)}
        self._node_availability = self.subset_count("Node")
        self._socket_availability = self.subset_count("Socket")
        self._core_availability = self.subset_count("Core")
        self._cpu_availability = [True] * len(self._cpu_data)

    @classmethod
    def _sort_by(cls, dct_list, attr):
        return sorted(dct_list, key=lambda x: x[attr])

    @classmethod
    def _group_by(cls, dct_list, attr):
        return itertools.groupby(cls._sort_by(dct_list, attr), lambda x: x[attr])

    @classmethod
    def _group_by_cpus(cls, cpu_group, attr):
        ret = cls._group_by(cpu_group, attr)

        groups = {}

        for key, cpu_group in ret:
            groups[key] = list(cpu_group)

        return groups

    def group_by_cpus(self, attr):
        return self._group_by_cpus(self._cpu_data, attr)

    @classmethod
    def _multi_group_by(cls, dct_list, *keys, **kwargs):
        if len(keys) == 0:
            list_func = kwargs.setdefault("list_func", list)
            return list_func(dct_list)

        cpus_group = cls._group_by_cpus(dct_list, keys[0])

        return dict([(key, cls._multi_group_by(cpus, *keys[1:], **kwargs)) for key, cpus in cpus_group.items()])

    @classmethod
    def _subset(cls, dct_list, *keys):
        subset_of = keys[-1]
        group_by_keys = keys[:-1]
        return cls._multi_group_by(dct_list, *group_by_keys,
                                   list_func=lambda lst: sorted(set([dct[subset_of] for dct in lst])))

    @classmethod
    def _subset_count(cls, dct_list, *keys):
        return cls._multi_group_by(dct_list, *keys, list_func=len)

    def _hold_cpu(self, cpu):
        if self._cpu_availability[cpu]:
            cpu_info = self._cpu_data[cpu]
            machine_key = 0
            node_key = cpu_info["Node"]
            socket_key = cpu_info["Socket"]
            core_key = cpu_info["Core"]

            self._machine_availability[machine_key] -= 1
            self._core_availability[core_key] -= 1
            self._socket_availability[socket_key] -= 1
            self._node_availability[node_key] -= 1
            self._cpu_availability[cpu] = False

    def _release_cpu(self, cpu):
        if not self._cpu_availability[cpu]:
            cpu_info = self._cpu_data[cpu]
            machine_key = 0
            node_key = cpu_info["Node"]
            socket_key = cpu_info["Socket"]
            core_key = cpu_info["Core"]

            self._machine_availability[machine_key] += 1
            self._core_availability[core_key] += 1
            self._socket_availability[socket_key] += 1
            self._node_availability[node_key] += 1
            self._cpu_availability[cpu] = True

    ##############################################################################################################
    # Interface
    ##############################################################################################################

    def multi_group_by(self, *keys, **kwargs):
        return self._multi_group_by(self._cpu_data, *keys, **kwargs)

    def subset_count(self, *keys):
        return self._subset_count(self._cpu_data, *keys)

    def subset(self, *keys):
        return self._subset(self._cpu_data, *keys)

    def filter_non_available_cpus(self, cpus):
        return [cpu for cpu in cpus if self._cpu_availability[cpu]]

    def release_cpus(self, cpus):
        """ Release the given CPUs back the available CPUs poll. """
        for cpu in cpus:
            self._release_cpu(cpu)

    def select_cpus(self, cpus):
        """
        Select a list of CPUs.
        This function will return the subset of CPUs that are available for use.
        The returned CPUs will not be available for the following calls.
        When an experiment ends, the CPUs expected to be return using release_cpus()
        """
        available_cpus_subset = self.filter_non_available_cpus(cpus)

        if len(available_cpus_subset) < len(cpus):
            self.logger.warning("Could not select all of the requested CPUs. Only %s are available.",
                                available_cpus_subset)

        for cpu in available_cpus_subset:
            self._hold_cpu(cpu)

        return available_cpus_subset

    def auto_select_cpus(self, cpu_count):
        """
        Select CPUs from the available CPUs poll.
        The returned CPUs will not be available for the following calls.
        When an experiment ends, the CPUs expected to be return using release_cpus()

        Output: a list of CPUs (e.g. [0,2,...,8])
        """
        select_hierarchy = [
            (self._core_availability, self._core_core_subset),
            (self._socket_availability, self._socket_core_subset),
            (self._node_availability, self._node_core_subset),
            (self._machine_availability, self._machine_core_subset),
        ]

        available_cores = None

        for subset_availability, subset_cores in select_hierarchy:
            if available_cores is not None:
                break

            for subset, availability in subset_availability.items():
                if availability >= cpu_count:
                    available_cores = subset_cores[subset]
                    break

        if available_cores is None:
            self.logger.warning("Could not select CPUs. No cores available.")
            return []

        cpus = []

        # We will always select only full cores
        cores = []
        selected_cores_cpu_count = 0

        # Arrange cores from the most available cpus to the least
        available_cores = sorted(copy.deepcopy(available_cores),
                                 key=lambda core_key: -(self._core_availability[core_key]))

        for core in available_cores:
            if selected_cores_cpu_count >= cpu_count:
                # We already have what we need
                break

            if self._core_availability[core] == 0:
                continue

            cores.append(self.filter_non_available_cpus(self._core_cpu_subset[core]))
            selected_cores_cpu_count += self._core_availability[core]

        # CPUs from the same core should be far apart
        while len(cpus) < cpu_count and len(cores) > 0:
            new_cores = []

            for core in cores:
                if core and len(cpus) < cpu_count:
                    cpus.append(core.pop(0))

                if core:
                    new_cores.append(core)

            cores = new_cores

        if len(cpus) < cpu_count:
            self.logger.warning("Could not select %s CPUs. Only %s available.", cpu_count, len(cpus))

        return self.select_cpus(cpus)

    @property
    def cpu_hierarchy(self):
        """
        Retrieve the CPUs hierarchy in the host machine arranged by nodes then cores

        Example output: {
                            0: {                 # Node 0
                                0: {             # Socket 0
                                    0: [1, 2],   # Core 0
                                    1: [3, 4]    # Core 1
                                }
                            },
                            1: {                 # Node 1
                                1: {             # Socket 1
                                    2: [5, 6],   # Core 2
                                    3: [7, 8]    # Core 3
                                }
                            }
                        }
        """
        return copy.deepcopy(self._cpu_hierarchy)

    @property
    def hyper_threads(self):
        return [s for s in self._core_cpu_subset.values() if len(s) > 1]


def write_to_system_file(file_path, data):
    out, err = shell.append_to_file(file_path, data, as_root=True)
    if err:
        raise Exception(f"Failed writing '{data}' to {file_path}: {err}")


def set_cpu_online(online, *cpu):
    online_bit = 1 if online else 0
    for c in cpu:
        if int(c) == 0:
            continue
        write_to_system_file(f'/sys/devices/system/cpu/cpu{c}/online', online_bit)


def is_cpu_online(cpu):
    if int(cpu) == 0:
        return True
    with open(f'/sys/devices/system/cpu/cpu{cpu}/online', 'r') as f:
        return int(f.read()) == 1


def disable_hyper_threading():
    c = CpuData()
    secondary_hyper_thread = itertools.chain(*[set(s) - {min(s)} for s in c.hyper_threads])
    set_cpu_online(False, *secondary_hyper_thread)


def enable_hyper_threading():
    cpu_data = fetch_cpu_data(offline=True)
    offline_threads = [d['CPU'] for d in cpu_data]
    set_cpu_online(True, *offline_threads)


cpu_pattern = re.compile(r'cpu(\d+)', re.I)
policy_pattern = re.compile(r'policy(\d+)', re.I)


def set_cpu_governor(governor='performance'):
    main_path = '/sys/devices/system/cpu/cpufreq'
    for c in os.listdir(main_path):
        m = policy_pattern.match(c)
        if not m or not is_cpu_online(m.group(1)):
            continue
        governor_path = os.path.join(main_path, c, 'scaling_governor')
        write_to_system_file(governor_path, governor)

    main_path = '/sys/devices/system/cpu'
    for c in os.listdir(main_path):
        m = cpu_pattern.match(c)
        if not m or not is_cpu_online(m.group(1)):
            continue
        governor_path = os.path.join(main_path, c, 'cpufreq/scaling_governor')
        write_to_system_file(governor_path, governor)


def set_cpu_freq_limits(min_freq, max_freq):
    # tapuz24: 1200000, 2400000
    # sudo cat /sys/devices/system/cpu/cpufreq/policy*/cpuinfo_cur_freq
    # sudo cpupower -c 0-23 frequency-set -g performance -d 2400000 -u 2400000
    main_path = '/sys/devices/system/cpu/cpufreq'
    for c in os.listdir(main_path):
        if not policy_pattern.match(c):
            continue
        min_freq_path = os.path.join(main_path, c, 'scaling_min_freq')
        write_to_system_file(min_freq_path, min_freq)
        min_freq_path = os.path.join(main_path, c, 'scaling_max_freq')
        write_to_system_file(min_freq_path, max_freq)

    main_path = '/sys/devices/system/cpu'
    for c in os.listdir(main_path):
        if not cpu_pattern.match(c):
            continue
        min_freq_path = os.path.join(main_path, c, 'cpufreq/scaling_min_freq')
        write_to_system_file(min_freq_path, min_freq)
        min_freq_path = os.path.join(main_path, c, 'cpufreq/scaling_max_freq')
        write_to_system_file(min_freq_path, max_freq)

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
import re
import os
import subprocess

from mom.monitor.collectors import Collector, CollectionError

main_path = '/sys/devices/system/cpu'
cpu_pattern = re.compile(r'cpu(\d+)', re.I)


def read_root_file(file_path):
    p = subprocess.Popen(["sudo", "-k", "-n", "cat", file_path], encoding='utf-8',
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    out = out.strip()
    err = err.strip()
    if err:
        raise CollectionError(f"Could not read file: {err}")

    return out


class CpuFreq(Collector):

    @staticmethod
    def iter_cpu_freq():
        for c in os.listdir(main_path):
            m = cpu_pattern.match(c)
            if not m:
                continue
            cpu_id = int(m.group(1))
            freq_path = os.path.join(main_path, c, 'cpufreq', 'cpuinfo_cur_freq')
            freq = int(read_root_file(freq_path))
            yield cpu_id, freq

    @classmethod
    def collect(cls):
        return {'cpufreq': dict(cls.iter_cpu_freq())}

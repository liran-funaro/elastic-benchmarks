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
import subprocess

from mom.monitor.collectors import Collector, CollectionError

physical_pattern = re.compile(r'\s*Physical\s*id\s*(\d+)\s*:\s*([+\-]?[\d.]+)\s*°C'
                              r'\s*\(\s*high\s*=\s*([+\-]?[\d.]+)\s*°C\s*,\s*crit\s*=\s*([+\-]?[\d.]+)\s*°C\s*\)\s*$',
                              re.I | re.M)

core_pattern = re.compile(r'\s*Core\s*(\d+)\s*:\s*([+\-]?[\d.]+)\s*°C'
                          r'\s*\(\s*high\s*=\s*([+\-]?[\d.]+)\s*°C\s*,\s*crit\s*=\s*([+\-]?[\d.]+)\s*°C\s*\)\s*$',
                          re.I | re.M)


def parse_groups(groups):
    group_id = int(groups[0])
    avg_temp, high_temp, critical_temp = map(float, groups[1:4])
    return group_id, {
        'avg': avg_temp,
        'high': high_temp,
        'critical': critical_temp,
    }


def parse_sensors(out):
    lines = physical_pattern.split(out)
    ret = {}
    for i, l in enumerate(zip(lines[1::5], lines[2::5], lines[3::5], lines[4::5], lines[5::5])):
        physical_id, data = parse_groups(l[:4])
        core_data = (parse_groups(m.groups()) for m in core_pattern.finditer(l[4]))
        data.update({f'core{i}': d for i, d in core_data})
        ret[physical_id] = data
    return ret


class Sensors(Collector):

    @staticmethod
    def read_sensors():
        p = subprocess.Popen(["sensors"], encoding='utf-8',
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        out = out.strip()
        err = err.strip()
        if err:
            raise CollectionError(f"'sensors' error output: {err}")

        return parse_sensors(out)

    @classmethod
    def collect(cls):
        return {'sensors': cls.read_sensors()}

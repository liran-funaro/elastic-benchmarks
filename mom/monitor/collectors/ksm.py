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
import os

from mom.monitor.collectors import Collector

ksm_path = '/sys/kernel/mm/ksm'


def read_ksm_file(ksm_file_name):
    with open(os.path.join(ksm_path, ksm_file_name), 'r') as f:
        return int(f.read())


def read_ksm_data():
    return {f: read_ksm_file(f) for f in os.listdir(ksm_path)}


class KsmStats(Collector):
    @classmethod
    def collect(cls):
        return {'ksm': read_ksm_data()}

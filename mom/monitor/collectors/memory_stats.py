# Memory Overcommitment Manager
# Copyright (C) 2010 Adam Litke, IBM Corporation
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 2 as
# published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public
# License along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
import re

from mom.monitor.collectors import Collector, CollectionError
from mom.util.parsers import parse_int, parameter_int

mem_info_pattern = re.compile(r'\s*([a-z_\d\-()\[\]]+)\s*:\s*(\d+)\s*([a-z]*)', re.I | re.M)


class MemoryStatistics(Collector):
    """
    This Collctor returns memory statistics about the host by examining
    /proc/meminfo and /proc/vmstat.  The fields provided are:
        available     - The total amount of available memory (MB)
        unused        - The amount of memory that is not being used for any purpose (MB)
        free          - The amount of free memory including some caches (MB)
        anon_pages    - The amount of memory used for anonymous memory areas (MB)
        swap_in       - The amount of memory swapped in since boot (pages)
        swap_out      - The amount of memory swapped out since boot (pages)
        page_in       - The amount of memory paged in since boot (pages)
        page_out      - The amount of memory paged out since boot (pages)
        major_fault   - The amount of major page faults since boot (pages)
        minor_fault   - The amount of minor page faults since boot (pages)
    """
    def __init__(self, _properties=None, meminfo=True, vmstat=True):
        Collector.__init__(self)
        self.meminfo = meminfo
        self.vmstat = vmstat

    @staticmethod
    def parse_int_def_zero(regex, string):
        val = parse_int(regex, string)
        return val if val is not None else 0

    @staticmethod
    def get_mem_info():
        with open("/proc/meminfo", 'r') as f:
            mem_info = f.read()

        match_iter = mem_info_pattern.finditer(mem_info)
        if not match_iter:
            raise CollectionError(f"meminfo output count not be parsed: {mem_info}")
        ret = {}
        for m in match_iter:
            key = m.group(1).lower()
            value = parameter_int(m.group(2))
            unit = m.group(3).lower()
            if unit == 'kb':
                value /= (1 << 10)
            ret[key] = value

        avail = ret['memtotal']
        unused = ret['memfree']
        buffers = ret['buffers']
        cached = ret['cached']
        if None not in (unused, buffers, cached):
            free = unused + buffers + cached
            cache_and_buff = cached + buffers
        else:
            free = cache_and_buff = None

        return dict(ret, available=avail, unused=unused, free=free, cache_and_buff=cache_and_buff)

    def get_vm_stat(self):
        with open("/proc/vmstat", 'r') as f:
            vm_stat = f.read()
        # /proc/vmstat reports cumulative statistics so we must subtract the
        # previous values to get the difference since the last collection.
        swap_in = self.parse_int_def_zero("^pswpin (.*)", vm_stat)
        swap_out = self.parse_int_def_zero("^pswpout (.*)", vm_stat)
        minflt = self.parse_int_def_zero("^pgfault (.*)", vm_stat)
        majflt = self.parse_int_def_zero("^pgmajfault (.*)", vm_stat)

        page_in = self.parse_int_def_zero("^pgpgin (.*)", vm_stat)
        page_out = self.parse_int_def_zero("^pgpgout (.*)", vm_stat)

        return dict(swap_in=swap_in, swap_out=swap_out, page_in=page_in, page_out=page_out,
                    major_fault=majflt, minor_fault=minflt)

    def collect(self):
        ret = {}
        if self.meminfo:
            ret.update(self.get_mem_info())

        if self.vmstat:
            ret.update(self.get_vm_stat())

        return {'memory': ret}

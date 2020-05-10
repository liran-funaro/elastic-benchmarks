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
import time

from mom.monitor.collectors import GuestCollector


def cast_nanoseconds_to_seconds(input_string):
    return int(input_string) * 1e-9


def cast_list_of_nanoseconds(input_string):
    return list(map(cast_nanoseconds_to_seconds, input_string.split()))


def cast_key_value(input_string, value_cast_method=None):
    lines = filter(None, (l.strip() for l in input_string.split('\n')))
    ret = dict(l.split() for l in lines)
    if value_cast_method is not None:
        for k, v in ret.items():
            ret[k] = value_cast_method(v)
    return ret


def cast_key_value_int(input_string):
    return cast_key_value(input_string, int)


CGROUP_SUBSYSTEM_PATH = '/sys/fs/cgroup/%s/machine/%s.libvirt-qemu/%s'


class GuestQemuProcess(GuestCollector):

    def read_value(self, subsystem, file_name, cast_method=None):
        with open(CGROUP_SUBSYSTEM_PATH % (subsystem, self.owner_name, file_name)) as f:
            ret = f.read()
            if cast_method is None:
                return ret
            else:
                return cast_method(ret)

    def collect(self):
        ret = {
            'time': time.time(),
            'cpu-total': {
                'usage': self.read_value('cpu', 'cpuacct.usage', cast_nanoseconds_to_seconds),
                'usage-user': self.read_value('cpu', 'cpuacct.usage_user', cast_nanoseconds_to_seconds),
                'usage-system': self.read_value('cpu', 'cpuacct.usage_sys', cast_nanoseconds_to_seconds),
            }
        }

        usage_per_cpu = self.read_value('cpu', 'cpuacct.usage_percpu', cast_list_of_nanoseconds)
        usage_per_cpu_user = self.read_value('cpu', 'cpuacct.usage_percpu_user', cast_list_of_nanoseconds)
        usage_per_cpu_sys = self.read_value('cpu', 'cpuacct.usage_percpu_sys', cast_list_of_nanoseconds)
        for i, (u, uu, us) in enumerate(zip(usage_per_cpu, usage_per_cpu_user, usage_per_cpu_sys)):
            ret[f'cpu-{i}'] = {
                'usage': u,
                'usage-user': uu,
                'usage-system': us,
            }

        ret['memory'] = self.read_value('memory', 'memory.stat', cast_key_value_int)
        return {'qemu': ret}

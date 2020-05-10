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
import time

from mom.monitor.collectors import Collector

CPU_PROC_STAT_HEADERS = 'user', 'nice', 'system', 'idle', 'iowait', 'irq', 'softirq', 'steal', 'guest', 'guest_nice'


"""
Calculated of /proc/stat is based on 'top" code:
https://github.com/hishamhm/htop/blob/e0209da88faf3b390d71ff174065abd407abfdfd/ProcessList.c
--------------------------------------------------------------------------------------------
// Guest time is already accounted in usertime
usertime = usertime - guest;
nicetime = nicetime - guestnice;
// Fields existing on kernels >= 2.6
// (and RHEL's patched kernel 2.4...)
idlealltime = idletime + ioWait;
systemalltime = systemtime + irq + softIrq;
virtalltime = guest + guestnice;
totaltime = usertime + nicetime + systemalltime + idlealltime + steal + virtalltime;
--------------------------------------------------------------------------------------------

Also explained here:
https://stackoverflow.com/questions/23367857/accurate-calculation-of-cpu-usage-given-in-percentage-in-linux
"""


clock_ticks = os.sysconf("SC_CLK_TCK")


class CpuUsage(Collector):

    @staticmethod
    def read_total_proc_stat():
        with open("/proc/stat", 'r') as f:
            contents = f.read()

        cpu_lines = [line for line in contents.split("\n") if line.startswith("cpu")]
        ret = {}
        for line in cpu_lines:
            tokens = [s for s in line.split(" ") if s]
            name = tokens[0].strip()
            ret[name] = [int(s.strip()) for s in tokens[1:]]

        return ret

    @classmethod
    def collect(cls):
        dct = {'time': time.time()}
        for k, v in cls.read_total_proc_stat().items():
            user, nice, system, idle, iowait, irq, softirq, steal, guest, guest_nice = v
            #  Guest time is already accounted in 'user'
            user_time = user - guest
            nice_time = nice - guest_nice
            # Fields existing on kernels >= 2.6 (and RHEL's patched kernel 2.4...)
            idle_all_time = idle + iowait
            system_all_time = system + irq + softirq
            virtual_time = guest + guest_nice
            total_time = user_time + nice_time + system_all_time + idle_all_time + steal + virtual_time

            cpu_id = int(k[3:]) if len(k) > 3 else "total"
            stats = {
                'total': total_time,
                'virtual': virtual_time,
                'system-all': system_all_time,
                'idle-all': idle_all_time,
                'user-non-virtual': user_time,
                **dict(zip(CPU_PROC_STAT_HEADERS, v))
            }

            dct[f'cpu-{cpu_id}'] = {k: v/clock_ticks for k, v in stats.items()}

        return {'cpu': dct}

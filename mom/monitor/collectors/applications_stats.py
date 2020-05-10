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
import time
import subprocess
from typing import List, Dict, Any

from mom.monitor.collectors import Collector, CollectionError
from mom.monitor.collectors.cpu_usage import clock_ticks

CPU_PROC_STAT_HEADERS = 'minflt', 'majflt', 'utime', 'stime', 'vsize', 'rss'
MB = 2**20
PAGES_IN_MB = 2**8

"""
For the calculation of /proc/[pid]/stat we use information from `man proc`:
---------------------------------------------------------------------------
(10) minflt
    The number of minor faults the process has made which have not required loading a memory page from disk.
(12) majflt
    The number of major faults the process has made which have required loading a memory page from disk.
(14) utime
    Amount of time that this process has been scheduled in user mode, measured in clock ticks
    (divide by sysconf(_SC_CLK_TCK)). This includes guest time, guest_time (time spent running a virtual CPU, see
    below), so that applications that are not aware of the guest time field do not lose that time from their
    calculations.
(15) stime
    Amount of time that this process has been scheduled in kernel mode, measured in clock ticks
    (divide by sysconf(_SC_CLK_TCK)).
(23) vsize
    Virtual memory size in bytes.
(24) rss
    Resident Set Size: number of pages the process has in real memory.  This is just the pages which count toward text,
    data, or stack space.  This does not include pages which have not been demand-loaded in, or which are swapped out.
---------------------------------------------------------------------------

Also explained here:
https://stackoverflow.com/questions/16726779/how-do-i-get-the-total-cpu-usage-of-an-application-from-proc-pid-stat
"""


CPU_STATS = 'utime', 'stime', 'total'
MEM_STATS = 'rss', 'vsize'


def read_pid_stat(pid: int):
    cur_time = time.time()
    with open(f"/proc/{pid}/stat", 'r') as f:
        contents = f.read()

    data = list(filter(None, contents.split(" ")))
    stats = dict(zip(CPU_PROC_STAT_HEADERS, map(int, (data[9], data[11], *data[13:15], *data[22:24]))))
    stats['utime'] /= clock_ticks
    stats['stime'] /= clock_ticks
    stats['total'] = stats['utime'] + stats['stime']
    stats['time'] = cur_time
    stats['rss'] /= PAGES_IN_MB
    stats['vsize'] /= MB
    return stats


OPTIONAL_SECTIONS = 'monitor', 'host-monitor'


class ApplicationsStats(Collector):
    def __init__(self, properties):
        Collector.__init__(self, properties)
        self.app_names: List[str] = self.get_app_names()
        self.app_missing = {}
        self.app_timeout = 60

    def get_app_names(self) -> List[str]:
        for section in OPTIONAL_SECTIONS:
            app_names = self.config.get(section, 'applications', None)
            if app_names is not None:
                return app_names

    def mark_app_missing(self, app_name: str):
        if app_name not in self.app_missing:
            self.app_missing[app_name] = time.time()

    def mark_app_found(self, app_name: str):
        if app_name in self.app_missing:
            del self.app_missing[app_name]

    def clean_missing_apps(self):
        removed = []
        min_missing_time = float('inf')
        cur_time = time.time()
        for app, start_time in self.app_missing.items():
            missing_time = cur_time - start_time
            if missing_time > self.app_timeout:
                removed.append(app)
                min_missing_time = min(min_missing_time, missing_time)

        if not removed:
            return

        self.logger.info("Could not find the following applications after %.2f seconds: %s. Stop looking.",
                         min_missing_time, ", ".join(removed))
        for app in removed:
            self.app_names.remove(app)
            self.mark_app_found(app)

    def get_app_pid_list(self, app_name):
        p = subprocess.Popen(["pgrep", '-f', app_name], encoding='utf-8',
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        out = out.strip()
        err = err.strip()
        if err:
            raise CollectionError(f"Error looking for '{app_name}' pid: {err}")

        app_pids = []
        for line in out.split():
            line = line.strip()
            if not line:
                continue
            try:
                app_pids.append(int(line))
            except ValueError as e:
                self.logger.error("Failed to parse pgrep output line '%s' with error: %s", e)

        return app_pids

    def get_app_stat(self, app_name):
        app_pid = self.get_app_pid_list(app_name)

        if not app_pid:
            self.mark_app_missing(app_name)
            return None
        else:
            self.mark_app_found(app_name)

        stats_data = []
        for pid in app_pid:
            try:
                stats = read_pid_stat(pid)
            except (FileNotFoundError, ProcessLookupError):
                pass
            except Exception as e:
                self.logger.exception("Failed to read pid '%s' of app '%s': %s", pid, app_name, e)
            else:
                stats_data.append(stats)

        if not stats_data:
            return None

        ret = {
            'time': min(dct['time'] for dct in stats_data),
            'cpu': {k: sum(dct[k] for dct in stats_data) for k in CPU_STATS},
            'memory': {k: sum(dct[k] for dct in stats_data) for k in MEM_STATS},
        }
        return ret

    def collect(self) -> Dict[str, Any]:
        ret = {f'{p}-stats': self.get_app_stat(p) for p in list(self.app_names)}
        self.clean_missing_apps()
        return {k: v for k, v in ret.items() if v}

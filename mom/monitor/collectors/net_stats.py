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
from itertools import cycle, chain

from mom.monitor.collectors import Collector


def read_network_stat(pid: int = None):
    if pid is None:
        stats_file_path = "/proc/net/dev"
    else:
        stats_file_path = f"/proc/{pid}/net/dev"
    with open(stats_file_path, 'r') as f:
        content = f.readlines()

    # Output example:
    # --------------------------------------------------------------------------------------------------------------
    # Inter-|   Receive                                                |  Transmit
    #  face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
    #     lo:    8284     108    0    0    0     0          0         0     8284     108    0    0    0     0       0          0
    #   eth0: 7412951    5878    0    6    0     0          0         0   302140    3936    0    0    0     0       0          0

    # First line is the communication direction (receive/transmit) separated by |
    # Second line is the data columns. Each direction column's is separated by |
    # First column is the interface name. We don't need to use its name.

    headers = [h.strip().lower() for h in content[0].split('|')]
    sub_headers = [h.split() for h in content[1].split('|')]
    keys = list(chain(*[zip(cycle([h]), s) for h, s in zip(headers[1:], sub_headers[1:])]))

    ret = {}
    for line in content[2:]:
        line = line.split()
        interface_name = line[0].strip(": ")
        interface = ret.setdefault(interface_name, {})
        for (direction, key), d in zip(keys, line[1:]):
            interface.setdefault(direction, {})[key] = int(d)
    return ret


class NetworkStatistics(Collector):
    def __init__(self, properties=None):
        Collector.__init__(self, properties)

    @classmethod
    def collect(cls):
        return {'net': read_network_stat()}

"""
Author: Liran Funaro <liran.funaro@gmail.com>
Based on code by Eyal Posner

Copyright (C) 2006-2018 Liran Funaro

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
import socket
from cloudexp.util import process

from elasticbench.apps.memcached import memcached_port

from mom.communication.pickle_socket_server import SimpleTcpClient
from mom.monitor.collectors import Collector
from mom.util.parsers import parameter_int_or_float_or_str, parameter_int

MB = 2**20
PAGE_SIZE = 2**12
PAGES_IN_MB = 2**8

stats_regexp = re.compile(r"\s*STAT\s+([a-z_]+)\s+([\d.]+)", re.I)
stats_slabs_regexp = re.compile(r"\s*STAT\s+(\d+):([a-z_]+)\s+(\d+)", re.I)


def parse_stats(output: str):
    return {m.group(1): parameter_int_or_float_or_str(m.group(2)) for m in stats_regexp.finditer(output)}


def parse_slabs_stats(output: str):
    slabs = {}
    for m in stats_slabs_regexp.finditer(output):
        slab_id = parameter_int(m.group(1))
        key = m.group(2)
        value = parameter_int_or_float_or_str(m.group(3))
        slabs.setdefault(slab_id, {})[key] = value
    return slabs


class MemcachedCollector(Collector):
    def __init__(self, _properties=None, timeout=10):
        Collector.__init__(self)
        self.timeout = timeout
        self._memcached_client = None
        self._app_available = False

    @staticmethod
    def open_statm():
        pid = process.pid_of('memcached')
        return open(f"/proc/{pid}/statm", "r")

    @property
    def memcached_client(self):
        if self._memcached_client is None:
            self._memcached_client = SimpleTcpClient("localhost", memcached_port,
                                                     timeout=self.timeout,
                                                     base_name="memcached-collector-client")
        return self._memcached_client

    def close(self):
        if self._memcached_client is not None:
            self._memcached_client.close()

    def memcached_command(self, *cmd):
        cmd = " ".join(map(str, (*cmd, '\n')))
        try:
            return self.memcached_client.send_recv(cmd)
        except Exception as err:
            if self._app_available:
                self.logger.warn("Error communicating with memcached with command '%s': %s", cmd, err)
                self._app_available = False
            return None

    def max_bytes_stats(self):
        output = self.memcached_command('stats')
        if not output:
            return None
        d = parse_stats(output)

        # Adjust byte fields into MB
        for k, v in list(d.items()):
            if 'byte' in k:
                d[k] = v / MB

        return d

    def alloc_stats(self):
        output = self.memcached_command('stats', 'slabs')
        if not output:
            return None
        d = parse_stats(output)

        # Adjust total_malloced field into MB
        total_malloced = d.get('total_malloced', None)
        if total_malloced is not None:
            d['total_malloced'] = total_malloced / MB

        # Adjust mem_requested field into MB
        d['slabs'] = parse_slabs_stats(output)
        for s in list(d['slabs'].values()):
            mem_requested = s.get('mem_requested', None)
            if mem_requested is not None:
                s['mem_requested'] = mem_requested / MB

        return d

    def rss_stats(self):
        """ read the statm resident set size (measure in pages), and convert to MB. """
        try:
            with self.open_statm() as f:
                info = f.read()
            return int(info.split(" ")[1]) / PAGES_IN_MB
        except Exception as err:
            if self._app_available:
                self.logger.warn("Error reading memcached statm: %s", err)
                self._app_available = False
            return None

    def collect(self):
        ret = {}

        rss = self.rss_stats()
        if rss is not None:
            ret['rss'] = rss

        stats = self.max_bytes_stats()
        if stats is not None:
            ret.update(stats)

        alloc = self.alloc_stats()
        if alloc is not None:
            ret.update(alloc)

        return {'memcached': ret}

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
import socket

from cloudexp.guest.application import DynamicResourceControl, Application
from mom.util.memory_type import InvalidMemory, is_memory_close
from elasticbench.apps.memcached.collector import MemcachedCollector


class MemcachedMemControl(DynamicResourceControl):
    def __init__(self, app: Application, guest_server_port, wait_timeout, decrease_mem_time=8, spare_mem=0,
                 elastic=True, **kwargs):
        DynamicResourceControl.__init__(self, guest_server_port, wait_timeout, decrease_mem_time, spare_mem=spare_mem,
                                        **kwargs)
        self.app = app
        self._memcached_collector = None
        self._last_target = InvalidMemory
        self.min_update_diff = 15
        self.elastic = elastic

    @property
    def memcached_collector(self):
        if self._memcached_collector is None:
            self._memcached_collector = MemcachedCollector(timeout=self.wait_timeout)
        return self._memcached_collector

    def terminate(self):
        if self._memcached_collector is not None:
            self._memcached_collector.close()
        DynamicResourceControl.terminate(self)

    def memcached_command(self, *cmd):
        return self.memcached_collector.memcached_command(*cmd)

    def mem_stats(self):
        stats = self.memcached_collector.collect()
        stats = stats['memcached']
        max_bytes = stats.get('limit_maxbytes', 0)
        total_malloc = stats.get('total_malloced', 0)
        mem_rss = stats.get('rss', 0)
        return max_bytes, mem_rss, total_malloc

    def application_rss(self):
        max_bytes, mem_rss, total_malloc = self.mem_stats()
        return mem_rss

    def change_mem_func(self, mem_total, mem_usage, mem_cache_and_buff, app_rss):
        # * total_malloc == max_bytes (max_bytes is set by out target)
        #   [total_malloc follows max_bytes perfectly (diff==0) but with a delay]
        # * mem_usage - mem_cache_and_buff - mem_rss = about 200 MB (OS memory probably)
        # * mem_rss - total_malloc = between 20 to 60 MB
        # => mem_usage - mem_cache_and_buff - total_malloc = about 260 MB
        # * mem_cache_and_buff = between 120 to 180 MB
        # * mem_usage == mem_available (but with as delay)

        # Leave page cache out of the equation ( - mem_cache_and_buff)
        spare = mem_usage - app_rss + self.spare_mem
        if spare < 0:
            self.logger.error("Spare memory should be grater than 0. But spare=%s.", spare)
        target = int(max(1, mem_total - spare))

        if is_memory_close(target, self._last_target, self.min_update_diff):
            self.logger.debug("Insignificant memory target change: from %.2f to %.2f.", self._last_target, target)
            return

        self.logger.info("[memcached mem state] target=%.2f | spare=%.2f", target, spare)

        return self.apply_memory(target)

    def apply_memory(self, target):
        if self.elastic:
            try:
                ans = self.memcached_command('m', target)
                self.logger.debug("Response for command 'm %s': %s", target, ans)
                self._last_target = target
                return target
            except socket.error as err:
                self.logger.warn("Error in setting memcached memory to %i: %s", target, err)
        else:
            try:
                self.app.init_mem_size = target
                self.app.restart_application()
                self.logger.warn("Memcached restarted with memory: %s", target)
                self._last_target = target
                return target
            except Exception as err:
                self.logger.warn("Error restarting memcached to set memory to %s: %s", target, err)

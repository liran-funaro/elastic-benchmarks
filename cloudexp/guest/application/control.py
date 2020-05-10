"""
Author: Liran Funaro <liran.funaro@gmail.com>

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
import time
from typing import Optional, Union

from mom.util.terminable import Terminable
from mom.logged_object import LoggedThread
from mom.communication.guest_client import GuestClient
from mom.monitor.collectors.memory_stats import MemoryStatistics
from mom.util.memory_type import InvalidMemory, is_memory_close, is_valid_mem
from mom.communication.messages import MessageTargetAllocation, MessageUpdateResourceDiff, \
    MessageUpdateApplicationTarget


class DynamicResourceControl(LoggedThread, Terminable):
    STATS_POLL_TIMEOUT = 0.5

    def __init__(self, guest_server_port, wait_timeout, decrease_mem_time=1, change_mem_func=None,
                 application_rss_func=None, name=None,
                 spare_mem=0, shared_terminable: Terminable = None):
        LoggedThread.__init__(self, name=name, daemon=True)
        Terminable.__init__(self, shared_terminable=shared_terminable)
        self.stats_collector = MemoryStatistics(meminfo=True, vmstat=False)
        self.guest_server_port = guest_server_port
        self.wait_timeout = max(1, wait_timeout)
        self.decrease_mem_time = max(0, decrease_mem_time)
        self.spare_mem = max(0, spare_mem)
        self.guest_server_client: Optional[GuestClient] = None
        self.memory_diff = InvalidMemory
        self.available_memory = InvalidMemory

        if change_mem_func is not None:
            self.change_mem_func = change_mem_func
        if application_rss_func is not None:
            self.application_rss = application_rss_func

    def change_mem_func(self, mem_total, mem_usage, mem_cache_and_buff, app_rss):
        """
        This function should be overridden.
        Gets target_memory in MB, and should change the size of the memory
        of the program to that target memory.
        """
        raise NotImplementedError

    def application_rss(self):
        """
        This function should be overridden.
        Return the application's memory consumption in MB.
        """
        return float('nan')

    def get_server_client(self):
        if self.guest_server_client is None:
            self.guest_server_client = GuestClient("localhost", port=self.guest_server_port,
                                                   default_timeout=None, base_name=self.logger_name)
        return self.guest_server_client

    def terminate(self):
        Terminable.terminate(self)
        if self.guest_server_client is not None:
            self.guest_server_client.close()

    def get_stats(self):
        try:
            stats = self.stats_collector.collect()
            memory_stats = stats.get('memory', {})
            available, cache_and_buff, unused = map(memory_stats.get, ('available', 'cache_and_buff', 'unused'))
            memory_stats['used'] = available - unused
            return memory_stats
        except Exception as ex:
            self.logger.error("Failed getting data on available memory: %s", ex)
            return None

    def poll_stats(self, target, cur_value, interval=0.1, timeout: Union[int, float] = 1, field='available'):
        end_time = time.time() + timeout
        stable_time = time.time()
        while time.time() < end_time and not is_memory_close(target, cur_value):
            self.terminable_sleep(interval)
            stats = self.get_stats()
            new_value = stats[field]
            if not is_memory_close(new_value, cur_value):
                stable_time = time.time()
            cur_value = new_value
        return cur_value, time.time() - stable_time

    def request_target(self, timeout=None):
        guest_server_client = self.get_server_client()
        try:
            msg = MessageTargetAllocation(timeout=timeout)
            target = guest_server_client.send_receive_message(msg, timeout=max(1, timeout+1))
            grace_period = target.get('grace-period', None)
            if grace_period is not None:
                cur_time = time.time()
                update_time = target.get('update-time', cur_time)
                grace_period = grace_period - (cur_time - update_time)
                if grace_period < 1e-2:
                    grace_period = None
                target['grace-period'] = grace_period
            return target
        except Exception as ex:
            self.logger.warning("Failed getting hint on target allocation: %s", ex)
            return None

    def update_resource_diff(self):
        if not is_valid_mem(self.memory_diff):
            return
        guest_server_client = self.get_server_client()
        try:
            msg = MessageUpdateResourceDiff(memory=self.memory_diff)
            guest_server_client.send_receive_message(msg, timeout=self.wait_timeout)
        except Exception as ex:
            self.logger.warning("Failed update the policy about the memory diff: %s", ex)

    def update_application_target(self, app_target):
        if not is_valid_mem(app_target):
            return
        guest_server_client = self.get_server_client()
        try:
            msg = MessageUpdateApplicationTarget(memory=app_target)
            guest_server_client.send_receive_message(msg, timeout=self.wait_timeout)
        except Exception as ex:
            self.logger.warning("Failed update the policy about the memory diff: %s", ex)

    def _update_memory(self, target):
        """ :return: wait_time, target_memory """
        if target is None:
            return self.wait_timeout, None

        grace_period = target.get('grace-period', None)
        memory_alloc = target.get('alloc', {}).get('memory', None)
        if memory_alloc is None:
            self.logger.warning('Notification did not include memory allocation')
            return self.wait_timeout, None

        # First notification should indicate stable condition
        if not is_valid_mem(self.memory_diff) and grace_period is None:
            self.memory_diff = max(0, memory_alloc - self.available_memory)
            LoggedThread(target=self.update_resource_diff, name=f"{self.__log_name__}-resource-diff",
                         daemon=True, verbose=False).start()

        if not is_valid_mem(self.memory_diff):
            return self.wait_timeout, None

        memory_alloc -= self.memory_diff

        if is_memory_close(memory_alloc, self.available_memory):
            # memory_alloc = available ; continue
            return self.wait_timeout, None
        elif memory_alloc > self.available_memory and grace_period is not None:
            # memory_alloc > available ; still have grace time
            return grace_period, None
        elif memory_alloc > self.available_memory:
            # memory_alloc > available ; not more grace time
            self.available_memory, stable_time = self.poll_stats(memory_alloc, self.available_memory,
                                                                 timeout=self.STATS_POLL_TIMEOUT)
            is_ready = is_memory_close(memory_alloc, self.available_memory)
            if is_ready or stable_time + 1e-2 > self.STATS_POLL_TIMEOUT:
                # It is a good time to update the memory diff
                self.memory_diff = max(0, memory_alloc + self.memory_diff - self.available_memory)
                return self.wait_timeout, memory_alloc
            else:
                return 0.1, memory_alloc
        elif grace_period is not None and grace_period > self.decrease_mem_time:
            # memory_alloc < available ; still have grace time
            return grace_period - self.decrease_mem_time, None
        else:
            # memory_alloc < available ; not more grace time
            return self.wait_timeout, memory_alloc

    def logged_run(self) -> None:
        wait_time = self.wait_timeout
        while self.should_run:
            target = self.request_target(wait_time)
            if not self.should_run:
                return
            memory_stats = self.get_stats()
            if not self.should_run:
                return
            app_rss = self.application_rss()
            if not self.should_run:
                return

            self.available_memory, cache_and_buff, used = map(memory_stats.get, ('available', 'cache_and_buff', 'used'))

            wait_time, target_memory = self._update_memory(target)
            self.logger.debug("[state] next=%.2f | target=%s | available=%.2f | used=%.2f | unused=%.2f | rss=%.2f | "
                              "cache=%.2f | diff=%.2f",
                              wait_time, target_memory, self.available_memory, used, self.available_memory - used,
                              app_rss, cache_and_buff, self.memory_diff)
            if not self.should_run:
                return

            if target_memory is not None and target_memory > 0:
                # take the minimum from the host's hint and current allocation,
                # so we won't take more memory than available when memory is
                # growing, and we will free memory when shrinkage is about to
                # happen.
                target_memory = min(target_memory, self.available_memory)
            else:
                target_memory = self.available_memory

            try:
                ret_target = self.change_mem_func(target_memory, used, cache_and_buff, app_rss)
            except Exception as ex:
                self.logger.exception("Failed to update the application memory: %s", ex)
                ret_target = None

            if ret_target is not None:
                LoggedThread(target=self.update_application_target, name=f"{self.__log_name__}-app-target",
                             args=(ret_target,), daemon=True, verbose=False).start()

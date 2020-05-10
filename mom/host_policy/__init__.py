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
import socket
import threading
import time
from typing import List, Union
from concurrent.futures import ThreadPoolExecutor

from mom.config import DictConfig, ClassImporter
from mom.logged_object import LoggedThread
from mom.util.data_logger import DataLogger
from mom.host_policy import controllers
from mom.host_policy import allocators
from mom.communication.messages import MessageNotify, MessageInquiry
from mom.monitor import Monitor, MonitorDataEntity
from mom.guest_manager import GuestManager
from mom.util.terminable import Terminable


class HostPolicy(LoggedThread, Terminable):
    """
    At a regular interval, this thread triggers system reconfiguration by
    sampling host and guest data, evaluating the policy and reporting the
    results to all enabled Controller plugins.
    """
    def __init__(self, config: DictConfig, libvirt_iface, host_monitor: Monitor, guest_manager: GuestManager,
                 shared_terminable: Terminable = None):
        LoggedThread.__init__(self, daemon=True)
        Terminable.__init__(self, shared_terminable=shared_terminable)

        self.policy_lock = threading.RLock()
        self.host_monitor = host_monitor
        self.guest_manager = guest_manager
        self.config = config

        self.interval: float = max(self.config.get('policy', 'interval'), 1.)
        self.grace_period: float = min(self.config.get('policy', 'grace-period'), self.interval)
        self.inquiry_timeout: float = min(self.config.get('policy', 'inquiry-timeout'), self.interval)

        self.resources = [r.strip() for r in self.config.get('policy', 'resources')]
        self.policy = ClassImporter(allocators).get_class(config.get('policy', 'allocator'))(self.resources)
        self.controllers = []

        self.properties = {
            'libvirt_iface': libvirt_iface,
            'host_monitor': host_monitor,
            'guest_manager': guest_manager,
            'config': config
            }

        self.policy_data_loggers = {}
        self.client_executor = None

        self.get_controllers()

    def get_controllers(self):
        """ Initialize the Controllers called for in the config file. """
        controllers_importer = ClassImporter(controllers)
        for resource_name in self.resources:
            try:
                controller_name = self.config.get('policy', f'{resource_name}-controller')
                controller_class = controllers_importer.get_class(controller_name)
                self.logger.debug("Loaded %s controller for resource %s", controller_name, resource_name)
            except Exception as e:
                self.logger.exception("Unable to import controller for resource '%s': %s", resource_name, e)
                continue
            try:
                self.controllers.append(controller_class(resource_name, self.properties))
            except Exception as e:
                self.logger.exception("Unable to instantiate controller for resource '%s': %s", resource_name, e)

    def get_policy_data_logger(self, entity: MonitorDataEntity):
        source = entity.prop('source')
        data_logger = self.policy_data_loggers.get(source, None)
        if data_logger is None:
            data_logger = DataLogger('policy', source)
            self.policy_data_loggers[source] = data_logger
        return data_logger

    def notify_guest(self, entity: MonitorDataEntity, grace_period: Union[int, float, None] = None):
        alloc = {}
        for r in self.resources:
            alloc[r] = entity.get_control(r)
        try:
            msg_obj = MessageNotify({'alloc': alloc, 'grace-period': grace_period})
            timeout = grace_period
            if grace_period is None:
                timeout = self.inquiry_timeout
            entity.prop('guest-client').send_receive_message(msg_obj, timeout=timeout)
        except socket.timeout as e:
            if self.should_run:
                self.logger.error("Could not notify guest: %s", e)
        except Exception as ex:
            if self.should_run:
                self.logger.exception("Failed to notify guest %s: %s", entity.prop('source'), ex)

    def inquire_guest(self, entity: MonitorDataEntity, grace_period: Union[int, float, None] = None):
        last_alloc = entity.get_var('last_control', {})
        try:
            msg_obj = MessageInquiry({'last-alloc': last_alloc, 'grace-period': grace_period,
                                      'timeout': self.inquiry_timeout})
            return entity.prop('guest-client').send_receive_message(msg_obj, timeout=self.inquiry_timeout)
        except socket.timeout as e:
            if self.should_run:
                self.logger.error("Could not inquire guest: %s", e)
        except Exception as ex:
            if self.should_run:
                self.logger.exception("Failed to inquire guest: %s", ex)

    def parallel_for_each_guest(self, func, guest_list: List[MonitorDataEntity], results_var_key=None, **kwargs):
        jobs = []
        for g in guest_list:
            jobs.append(self.client_executor.submit(func, g, **kwargs))

        for g, j in zip(guest_list, jobs):
            res = j.result()
            if results_var_key:
                g.set_var(results_var_key, res)

    def do_controls(self):
        """
        Sample host and guest data, process the rule set and feed the results
        into each configured Controller.
        """
        if not self.should_run:
            return
        # collect data
        host: MonitorDataEntity = self.host_monitor.interrogate()
        if host is None:
            return
        guest_list: List[MonitorDataEntity] = list(self.guest_manager.interrogate().values())

        # send inquiry to the clients regarding the next allocation
        inquiry_time = time.time()
        self.parallel_for_each_guest(self.inquire_guest, guest_list, results_var_key='inquiry',
                                     grace_period=self.grace_period)
        if not self.should_run:
            return
        try:
            with self.policy_lock:
                self.policy.apply_policy(host, guest_list)
        except Exception as e:
            self.logger.exception("Exception while applying policy: %s", e)
            return

        if not self.should_run:
            return

        # send notification to the clients on the next allocation
        notify_start = time.time()
        remaining_grace = max(0., self.grace_period - (notify_start - inquiry_time))
        self.parallel_for_each_guest(self.notify_guest, guest_list, grace_period=remaining_grace)
        notify_end = time.time()

        if not self.should_run:
            return

        # log and store control and variable of host and guest
        for entity in (host, *guest_list):
            data_logger = self.get_policy_data_logger(entity)
            data_logger.append_data({'notify': entity.controls}, notify_start, notify_end)

        remaining_grace = max(0., self.grace_period - (time.time() - inquiry_time))
        self.terminable_sleep(remaining_grace)

        if not self.should_run:
            return

        policy_start = time.time()
        for c in self.controllers:
            c.apply_control(host, guest_list)
        policy_end = time.time()

        # log and store control and variable of host and guest
        for entity in (host, *guest_list):
            data_logger = self.get_policy_data_logger(entity)
            data_logger.append_data({'controls': entity.controls, 'variables': entity.variables}, policy_start,
                                    policy_end)
            entity.store_variables()

        if not self.should_run:
            return
        # send notification to the clients on the applied allocation
        self.parallel_for_each_guest(self.notify_guest, guest_list, grace_period=None)

    def logged_run(self) -> None:
        self.client_executor = ThreadPoolExecutor(thread_name_prefix=f"{self.logger_name}-dispatcher")
        try:
            while self.should_run:
                start = time.time()
                self.do_controls()
                self.terminable_sleep(self.interval - (time.time() - start))
        finally:
            self.client_executor.shutdown()

#! /usr/bin/env python
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
import logging
from typing import Union

from mom.config import DictConfig
from mom.LibvirtInterface import LibvirtInterface
from mom.monitor.host_monitor import HostMonitor
from mom.guest_manager import GuestManager
from mom.host_policy import HostPolicy
from mom.util.terminable import Terminable

DEFAULT_CONFIG = {
    'main': {
        'check-loop-interval': 10,
        'libvirt-hypervisor-uri': 'qemu:///system',
    },
    'guest-manager': {
        'interval': 5,
    },
    'monitor': {
        'sample-history-length': 10,
    },
    'host-monitor': {
        'interval': 10,
        'collectors': ['MemoryStatistics', 'CpuUsage', 'NetworkStatistics', 'Sensors', 'CpuFreq', 'KsmStats'],
    },
    'guest-monitor': {
        'interval': 10,
        'collectors': ['GuestStats', 'GuestQemuProcess', 'GuestLibvirt'],
        'check-readiness-interval': 5,
    },
    'guest-client': {
        'port': 2187,
        'timeout': 10,
    },
    'policy': {
        'resources': ['memory'],
        'memory-controller': 'Balloon',
        'allocator': 'InquiryAllocator',
        'interval': 30,
        'inquiry-timeout': 2,
        'grace-period': 20,
    },
    'logging': {
        'verbosity': 'debug',
    }
}


def threads_ok(threads):
    """ Check to make sure a list of expected threads are still alive """
    return all((isinstance(t, list) and threads_ok(t)) or (hasattr(t, "is_alive") and t.is_alive()) for t in threads)


class MomHostDaemon(Terminable):
    def __init__(self, config: Union[DictConfig, dict, None] = None, shared_terminable: Terminable = None):
        Terminable.__init__(self, shared_terminable=shared_terminable)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.config = DictConfig(DEFAULT_CONFIG, config)

        # Set up a shared libvirt connection
        libvirt_uri = self.config.get('main', 'libvirt-hypervisor-uri')
        self.libvirt_iface = LibvirtInterface(libvirt_uri)

        self.guest_manager = GuestManager(self.config, libvirt_iface=self.libvirt_iface,
                                          shared_terminable=shared_terminable)
        self.host_monitor = HostMonitor(self.config, libvirt_iface=self.libvirt_iface,
                                        shared_terminable=shared_terminable)
        self.host_policy = HostPolicy(self.config, self.libvirt_iface,
                                      self.host_monitor, self.guest_manager,
                                      shared_terminable=shared_terminable)

        self.threads = {self.guest_manager, self.host_policy, self.host_monitor}

    def run_mom(self):
        # Start threads
        self.logger.info("Starting")

        for t in self.threads:
            if not t.is_alive() and self.should_run:
                t.start()

        interval = self.config.get('main', 'check-loop-interval')
        while self.should_run:
            self.terminable_sleep(interval)
            if not threads_ok(self.threads):
                if self.should_run:
                    self.logger.warning("One of the threads ended before it should. Terminating.")
                break

        self.terminate()

        for t in self.threads:
            if t.is_alive():
                t.join()

        self.logger.info("Daemon ending")

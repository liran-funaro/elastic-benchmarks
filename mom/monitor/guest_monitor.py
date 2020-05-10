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

from mom.communication.messages import MessageNotify
from mom.config import DictConfig
from mom.monitor import Monitor
from mom.logged_object import LoggedThread
from mom.communication.guest_client import load_guest_client
from mom.util.terminable import Terminable


class GuestMonitor(Monitor, LoggedThread):
    """ A GuestMonitor thread collects and reports statistics about 1 running guest """
    def __init__(self, config: DictConfig, guest_id, libvirt_iface, shared_terminable: Terminable = None):
        self.config = config
        self.libvirt_iface = libvirt_iface
        self.id = guest_id
        self.logger = logging.getLogger(f"GuestMonitor-{guest_id}")
        self.dom = self.libvirt_iface.getDomainFromID(guest_id)
        self.interval = self.config.get('guest-monitor', 'interval')

        self.info = self.get_guest_info()
        if self.info is None:
            raise ValueError(f"GuestMonitor-id:{guest_id} - failed to get information")

        self.vm_name = self.info['name']

        LoggedThread.__init__(self, name=self.vm_name, daemon=True)
        Monitor.__init__(self, config, self.vm_name, self.vm_name, shared_terminable=shared_terminable)

        self.start()

    def _properties(self):
        properties = Monitor._properties(self)
        properties.update(**self.info)

        properties.update({
            'id': self.id,
            'libvirt_iface': self.libvirt_iface,
            'interval': self.interval,
        })

        self.ip = properties['ip']

        self.guest_client = load_guest_client(self.ip, self.vm_name, self.config)
        properties['guest-client'] = self.guest_client

        return properties

    def check_guest_readiness(self):
        self.logger.debug("checking readiness: sending welcome messages")

        interval = self.config.get('guest-monitor', 'check-readiness-interval')
        self.guest_client.wait_for_server(interval, shared_terminable=self)
        # Hack to notify guest on current memory allocation

        info = self.dom.info()
        cur_mem = int(info[2] / 2 ** 10)
        self.guest_client.send_receive_message(MessageNotify({'alloc': {'memory': cur_mem}, 'grace-period': None}))
        self._set_ready()

    def _collectors_list(self, config):
        return config.get('guest-monitor', 'collectors')

    def get_guest_info(self):
        """
        Collect some basic properties about this guest
        Returns: A dict of properties on success, None otherwise
        """
        if self.dom is None:
            return None
        data = {
            'uuid': self.libvirt_iface.domainGetUUID(self.dom),
            'name': self.libvirt_iface.domainGetName(self.dom)
        }

        # The IP address is optional
        data['ip'] = self.get_guest_ip(data['name'])
        return data

    def logged_run(self) -> None:
        self.check_guest_readiness()

        self.logger.debug("Monitor interval: %i sec", self.interval)
        while self.should_run:
            start = time.time()
            self.collect()
            need_to_wait = self.interval - (time.time()-start)
            self.terminable_sleep(need_to_wait)

        self.guest_client.close()

    @staticmethod
    def get_guest_ip(name):
        return name

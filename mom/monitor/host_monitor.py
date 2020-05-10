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

from mom.logged_object import LoggedThread
from mom.monitor import Monitor

from mom.util.terminable import Terminable


class HostMonitor(Monitor, LoggedThread):
    """ The Host Monitor thread collects and reports statistics about the host. """

    def __init__(self, config, libvirt_iface=None, shared_terminable: Terminable = None):
        # thread's name is important to Plotter, must be: Host-Monitor
        self.interval = config.get('host-monitor', 'interval')
        self.libvirt_iface = libvirt_iface

        LoggedThread.__init__(self, daemon=True)
        Monitor.__init__(self, config, monitor_source='host', shared_terminable=shared_terminable)
        self._set_ready()

    def _properties(self):
        properties = Monitor._properties(self)
        properties.update({'interval': self.interval,
                           'libvirt_iface': self.libvirt_iface})
        return properties

    def _collectors_list(self, config):
        return config.get('host-monitor', 'collectors')

    def logged_run(self) -> None:
        while self.should_run:
            start = time.time()
            self.collect()
            self.terminable_sleep(self.interval - (time.time() - start))

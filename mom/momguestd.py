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
from typing import Union

from mom import guest_policy
from mom.monitor import Monitor
from mom.logged_object import LoggedObject
from mom.communication.guest_server import GuestServer
from mom.config import DictConfig, ClassImporter


DEFAULT_CONFIG = {
    'monitor': {
        'sample-history-length': 10,
        'collectors': ['MemoryStatistics', 'CpuUsage', 'NetworkStatistics'],
    },
    'policy': {
        'policy': 'BaseGuestPolicy',
    },
    'server': {
        'host': '',
        'port': 2187,
        'timeout': 10,
    },
    'logging': {
        'verbosity': 'debug',
    }
}


class MomGuestDaemon(LoggedObject):
    def __init__(self, config: Union[DictConfig, dict, None] = None, guest_name=None):
        LoggedObject.__init__(self, guest_name)
        self.config = DictConfig(DEFAULT_CONFIG, config)

        self.monitor = Monitor(self.config, monitor_name=guest_name)  # for collecting data, not running as a thread
        self.policy = ClassImporter(guest_policy).get_class(self.config.get('policy', 'policy'))(self.config)
        self.server = GuestServer(self.config, self.monitor, self.policy, guest_name=guest_name)

    def start(self):
        self.logger.info("Starting guest server, listening on %s:%s", self.server.host, self.server.port)
        self.server.serve_forever()
        self.server.shutdown()
        self.logger.info("Ended")

    def terminate(self):
        self.server.shutdown()

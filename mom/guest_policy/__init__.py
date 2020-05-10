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
from threading import Event
from typing import Dict

from mom.logged_object import LoggedObject


class BaseGuestPolicy(LoggedObject):
    def __init__(self, config):
        LoggedObject.__init__(self)
        self.config = config
        self.init_time = None
        self.response_scripts = self.config.get('policy', 'response-scripts', {})
        self.resource_diff = {}
        self._notify_event = Event()
        self._notify_event.clear()

    def update_resource_diff(self, resource_diff: Dict):
        self.resource_diff.update(resource_diff)

    def inquiry(self, inquiry_data):
        if self.init_time is None:
            self.init_time = time.time()
            cur_time = 0.
        else:
            cur_time = time.time() - self.init_time

        if self.response_scripts is None:
            return {}
        else:
            ret = {}
            grace_period = inquiry_data.get('grace-period', None)
            if grace_period:
                cur_time += grace_period
            for resource, script in self.response_scripts.items():
                diff = self.resource_diff.get(resource, 0)
                ret[resource] = script(cur_time) + diff
            return ret

    def notify(self, _notify_data):
        self._notify_event.set()

    def wait_for_notify(self, timeout=None):
        is_new_notification = self._notify_event.wait(timeout)
        self._notify_event.clear()
        return is_new_notification

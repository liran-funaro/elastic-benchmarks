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
from typing import List
from mom.logged_object import LoggedObject
from mom.monitor import MonitorDataEntity


class InquiryAllocator(LoggedObject):
    def __init__(self, resources):
        LoggedObject.__init__(self)
        self.resources = set(resources)

    def apply_policy(self, _host: MonitorDataEntity, guest_list: List[MonitorDataEntity]):
        """ Set control values for each guest according to its inquiry """
        for g in guest_list:
            inquiry = g.get_var('inquiry')
            if inquiry is None:
                self.logger.warning("No inquiry results for guest: %s", g.prop('name'))
                continue
            for r in self.resources.intersection(inquiry.keys()):
                g.control(r, inquiry[r])

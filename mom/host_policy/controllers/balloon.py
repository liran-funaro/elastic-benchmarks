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
from mom.logged_object import LoggedObject

from mom.util.memory_type import is_memory_close


class Balloon(LoggedObject):
    """
    Simple Balloon Controller that uses the libvirt setMemory() API to resize
    a guest's memory balloon.  Output triggers are:
        - balloon_target - Set guest balloon to this size (kB)
    """
    def __init__(self, resource, properties):
        LoggedObject.__init__(self)
        self.resource = resource
        self.libvirt = properties['libvirt_iface']

    def get_guest_target(self, guest_entity):
        return guest_entity.get_control(self.resource)

    def set_guest_target(self, guest_entity, target):
        return guest_entity.control(self.resource, target)

    @staticmethod
    def get_guest_name(guest_entity):
        return guest_entity.prop('name')

    @staticmethod
    def get_guest_id(guest_entity):
        return guest_entity.prop('id')

    @staticmethod
    def get_guest_current_mem(guest_entity):
        return guest_entity.stat('libvirt')['curmem']

    def apply_guest_control(self, guest_entity):
        guest_id = self.get_guest_id(guest_entity)
        dom = self.libvirt.getDomainFromID(guest_id)
        target = self.get_guest_target(guest_entity)
        guest_name = self.get_guest_name(guest_entity)

        if dom is None or target is None:
            return

        info = dom.info()
        max_mem = float(info[1]) / 2 ** 10
        cur_mem = float(info[2]) / 2 ** 10

        # check maximum allowed memory (defined by libvirt's domain maxMemory)
        if target > max_mem:
            self.logger.warning("%s reached it's memory limit", guest_name)
            target = max_mem
            self.set_guest_target(guest_entity, target)

        if is_memory_close(target, cur_mem, 5):
            return

        self.logger.debug("Ballooning %s: from %i to %i MB", guest_name, cur_mem, target)

        target_kb = int(target * (2 ** 10))
        if dom.setMemory(target_kb):
            self.logger.warning("Error while ballooning: %s", guest_name)

    def apply_control(self, _host, guests):
        for guest in sorted(guests, key=self._guest_sort_key):
            self.apply_guest_control(guest)

    def _guest_sort_key(self, guest_entity):
        """ returns: target - current. If this is negative, we'll prefer this guest """
        try:
            target = self.get_guest_target(guest_entity)
            current = self.get_guest_current_mem(guest_entity)
            return target - current
        except Exception as e:
            self.logger.warning("Could not calculate sort-key for %s: %s", guest_entity.prop('name'), e)
            return 0

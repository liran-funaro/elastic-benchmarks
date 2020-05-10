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
import threading

from mom.logged_object import LoggedThread
from mom.monitor.guest_monitor import GuestMonitor
from mom.util.terminable import Terminable


class GuestManager(LoggedThread, Terminable):
    """
    The GuestManager thread maintains a list of currently active guests on the
    system.  When a new guest is discovered, a new GuestMonitor is spawned.
    When GuestMonitors stop running, they are removed from the list.
    """
    def __init__(self, config, libvirt_iface, shared_terminable: Terminable = None):
        LoggedThread.__init__(self, daemon=True)
        Terminable.__init__(self, shared_terminable=shared_terminable)
        self.config = config
        self.libvirt_iface = libvirt_iface
        self.guest_monitors = {}
        self.guest_monitors_lock = threading.RLock()

    def get_guests_readiness(self):
        with self.guest_monitors_lock:
            return {g.info['name']: g.is_ready for g in self.guest_monitors.values()}

    def spawn_guest_monitors(self, domain_list):
        """
        Get the list of running domains and spawn GuestMonitors for any guests
        we are not already tracking.  The GuestMonitor constructor might block
        so don't hold guests_sem while calling it.
        """
        with self.guest_monitors_lock:
            spawn_list = set(domain_list) - set(self.guest_monitors)

        for guest_id in spawn_list:
            guest = GuestMonitor(self.config, guest_id, self.libvirt_iface, shared_terminable=self)
            if guest.is_alive():
                with self.guest_monitors_lock:
                    if guest_id not in self.guest_monitors:
                        self.guest_monitors[guest_id] = guest
                    else:
                        del guest

    def wait_for_guest_monitors(self):
        """ Wait for GuestMonitors to exit """
        while self.guest_monitors:
            with self.guest_monitors_lock:
                if self.guest_monitors:
                    guest_id, thread = self.guest_monitors.popitem()
                else:
                    guest_id = None
            if guest_id is not None:
                thread.join(0)
            else:
                break

    def check_threads(self, domain_list):
        """
        Check for stale and/or deceased threads and remove them.
        """
        with self.guest_monitors_lock:
            for guest_id, thread in list(self.guest_monitors.items()):
                # Check if the thread has died
                if not thread.is_alive():
                    del self.guest_monitors[guest_id]
                # Check if the domain has ended according to libvirt
                elif guest_id not in domain_list:
                    thread.terminate()
                    del self.guest_monitors[guest_id]

    def interrogate(self):
        """
        Interrogate all active GuestMonitors
        Return: A dictionary of Entities, indexed by guest id
        """
        ret = {}
        with self.guest_monitors_lock:
            for guest_id, monitor in self.guest_monitors.items():
                entity = monitor.interrogate()
                if entity is not None:
                    ret[guest_id] = entity
        return ret

    def logged_run(self) -> None:
        interval = self.config.get('guest-manager', 'interval')
        while self.should_run:
            start = time.time()
            domain_list = self.libvirt_iface.listDomainsID()
            if domain_list is not None:
                self.spawn_guest_monitors(domain_list)
                self.check_threads(domain_list)
            need_to_wait = interval - (time.time() - start)
            self.terminable_sleep(need_to_wait)
        self.wait_for_guest_monitors()

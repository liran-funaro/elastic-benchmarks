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
from mom.monitor.collectors import GuestCollector, CollectionError


class GuestLibvirt(GuestCollector):
    """
    This Collector uses libvirt to return guest memory statistics
        libvirt_state - The domain state defined by libvirt as:
                VIR_DOMAIN_NOSTATE  = 0 : no state
                VIR_DOMAIN_RUNNING  = 1 : the domain is running
                VIR_DOMAIN_BLOCKED  = 2 : the domain is blocked on resource
                VIR_DOMAIN_PAUSED   = 3 : the domain is paused by user
                VIR_DOMAIN_SHUTDOWN = 4 : the domain is being shut down
                VIR_DOMAIN_SHUTOFF  = 5 : the domain is shut off
                VIR_DOMAIN_CRASHED  = 6 : the domain is crashed
        libvirt_maxmem - The maximum amount of memory the guest may use
        libvirt_curmem - The current memory limit (set by ballooning)

    The following additional statistics may be available depending on the
    libvirt version, qemu version, and guest operation system version:
        mem_available - The total amount of available memory (MB)
        mem_unused    - The amount of memory that is not being used for any purpose (MB)
        mem_free      - The amount of free memory including some caches (MB)
        anon_pages    - The amount of memory used for anonymous memory areas (MB)
        swap_in       - The amount of memory swapped in since boot (pages)
        swap_out      - The amount of memory swapped out since boot (pages)
        major_fault   - The amount of major page faults since boot (pages)
        minor_fault   - The amount of minor page faults since boot (pages)
    """

    def __init__(self, properties):
        GuestCollector.__init__(self, properties)
        self.memstats_available = True

    def stats_error(self, msg, *args):
        """
        Only log stats interface errors one time when we first discover a
        problem.  Otherwise the log will be overrun with noise.
        """
        if self.memstats_available:
            self.logger.debug(msg, *args)
        self.memstats_available = False

    def collect(self):
        info = self.libvirt_interface.domainGetInfo(self.guest_domain)
        if info is None:
            raise CollectionError('Failed to get domain info')

        ret = {
            'state': info[0],
            'maxmem': float(info[1]) / 2**10,
            'curmem': float(info[2]) / 2**10,
        }

        # Try to collect memory stats.  This function may not be available
        try:
            info = self.libvirt_interface.domainGetMemoryStats(self.guest_domain)
            if info:
                ret.update(info)
                self.memstats_available = True
        except self.libvirt_interface.error as e:
            self.stats_error('libvirt memoryStats() is not ready: %s', e)
        except AttributeError as e:
            self.stats_error('Memory stats API not available for guest: %s', e)
        except KeyError as e:
            self.stats_error("Missing key for guest: %s", e)
        return {'libvirt': ret}

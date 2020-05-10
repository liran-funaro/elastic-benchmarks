"""
Author: Liran Funaro <liran.funaro@gmail.com>
Based on code from Memory Overcommitment Manager by Adam Litke, IBM Corporation

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
from mom.monitor.collectors import GuestCollector


class GuestCpuCount(GuestCollector):
    def collect(self):
        try:
            vcpus = [x[3] for x in self.guest_domain.vcpus()[0]]
            ret = {'cpu-count': len(vcpus)}
        except:
            ret = {'cpu-count': 1}

        return {'cpu': ret}

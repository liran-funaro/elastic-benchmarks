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
from typing import Dict, Any

from mom.config import DictConfig
from mom.logged_object import LoggedObject


class CollectionError(Exception):
    """
    This exception should be raised if a Collector has a problem during its
    collect() operation and it cannot return a complete, coherent data set.
    """
    pass


class Collector(LoggedObject):
    def __init__(self, properties=None):
        if properties is None:
            properties = {}
        self.properties: dict = properties
        self.owner_name: str = self.properties.get('name', None)
        self.config: DictConfig = self.properties.get('config', None)
        LoggedObject.__init__(self, name=self.owner_name)

    @property
    def libvirt_interface(self):
        return self.properties.get('libvirt_iface', None)

    def collect(self) -> Dict[str, Any]:
        raise NotImplementedError


class GuestCollector(Collector):
    def __init__(self, properties):
        Collector.__init__(self, properties)
        self._guest_id = None
        self._domain = None
        self._guest_client = None

    @property
    def guest_client(self):
        if self._guest_client is None:
            self._guest_client = self.properties.get('guest-client', None)
        return self._guest_client

    @property
    def guest_id(self):
        if self._guest_id is None:
            self._guest_id = self.properties.get('id', None)
        return self._guest_id

    @property
    def guest_domain(self):
        if self._domain is None:
            self._domain = self.libvirt_interface.getDomainFromID(self.guest_id)
        return self._domain

    def collect(self) -> Dict[str, Any]:
        raise NotImplementedError

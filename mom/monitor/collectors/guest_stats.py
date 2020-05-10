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
import zlib
import copy
import msgpack

from mom.communication.messages import Message
from mom.monitor.collectors import GuestCollector
from mom.util.dict_itertools import dict_recursive_update


class MessageStats(Message):
    """
    Host request from guest memory statistics
    Guest response with his statistics
    """
    def process(self, data, monitor, _policy):
        mon_data = None
        try:
            mon_data = monitor.collect()
        except Exception as e:
            self.logger.error("Error collecting data: %s", e)
        if not mon_data:
            mon_data = {}

        ret_data = copy.deepcopy(data)
        dict_recursive_update(ret_data, mon_data)

        packed = msgpack.dumps(ret_data, use_bin_type=True)
        return {'monitor': zlib.compress(packed, level=9)}


STATS_MSG = MessageStats()


class GuestStats(GuestCollector):
    """ Runs on host, and collect stats from guest through GuestClient. """
    def __init__(self, properties):
        GuestCollector.__init__(self, properties)
        self._guest_ready = False

    def collect(self):
        try:
            data = self.guest_client.send_receive_message(STATS_MSG)
        except Exception as e:
            if self._guest_ready:
                raise e
            else:
                return {}

        self._guest_ready = True
        data = zlib.decompress(data['monitor'])
        return msgpack.loads(data, raw=False)

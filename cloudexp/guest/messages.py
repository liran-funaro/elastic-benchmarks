"""
Author: Liran Funaro <liran.funaro@gmail.com>
Original Author: Eyal

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
from typing import Dict

from mom.communication.messages import Message


class MessageExpHintLoad(Message):
    """
    Host Hint the guest on load for experiment use
    Guest respond with ack (default)
    """
    def process(self, data: Dict, monitor, policy) -> Dict:
        hint_data = data.setdefault('hint', {})
        hint_data.update(self.content)
        hint_data['update-time'] = time.time()
        return Message.process(data, monitor, policy)

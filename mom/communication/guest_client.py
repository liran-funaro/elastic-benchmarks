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
import time
import socket
import threading
from typing import Union
from socket import _GLOBAL_DEFAULT_TIMEOUT

from mom.logged_object import LoggedObject
from mom.communication.pickle_socket_server import PickleTcpClient
from mom.communication.messages import Message, EchoMessage, MessageError
from mom.util.terminable import Terminable


def load_guest_client(ip, name, config, port=None, default_timeout=None, base_name="GuestClient"):
    if port is None:
        port = config.get("guest-client", "port")
    if default_timeout is None:
        default_timeout = config.get("guest-client", "timeout")
    return GuestClient(ip, name, port, default_timeout, base_name=base_name)


class GuestClient(LoggedObject):
    seconds_to_retry = 3

    def __init__(self, ip, name=None, port=8888, default_timeout=60, base_name="GuestClient"):
        LoggedObject.__init__(self, name)

        self.lock = threading.RLock()
        self.ip = ip
        self.port = port
        self.default_timeout = default_timeout
        self.client = PickleTcpClient(ip, port, timeout=default_timeout, base_name=base_name)

    def __repr__(self):
        return f"{LoggedObject.__repr__(self)}-{self.ip}-{self.port}"

    def close(self):
        self.client.close()
        self.logger.info("Closed")

    def set_timeout(self, timeout: Union[int, float, None] = _GLOBAL_DEFAULT_TIMEOUT):
        use_timeout = self.default_timeout if timeout is _GLOBAL_DEFAULT_TIMEOUT else timeout
        self.client.set_timeout(use_timeout)

    def send_receive_message(self, msg: Message, timeout: Union[int, float, None] = _GLOBAL_DEFAULT_TIMEOUT):
        """
        Send a message and collect respond from client
        response must be a string representing a dict
        """
        with self.lock:
            self.set_timeout(timeout)

            response_message = self.client.send_recv(msg)

            if not isinstance(response_message, dict):
                raise MessageError(f"Response must be a dict. Got {response_message} instead.")
            if not response_message.get("ack", False):
                err = response_message.get('error', '<no error message>')
                raise MessageError(f"Other side failed to process message {msg}: {err}")

            data = response_message.get('response', None)
            if not isinstance(data, dict):
                raise MessageError(f"Response data must be a dict. Got {data} instead.")

        return data

    def wait_for_server(self, interval=5, timeout=3, max_retries=24, shared_terminable: Terminable = None):
        start_wait = time.time()
        msg = EchoMessage()
        self.logger.debug("Waiting for guest server.")

        for c in range(max_retries):
            if shared_terminable is not None and not shared_terminable.should_run:
                break
            start_attempt = time.time()
            try:
                self.send_receive_message(msg, timeout)
            except socket.error:
                pass
            else:
                self.logger.info("Guest server is ready...")
                return

            if c < max_retries-1:
                sleep_time = interval - (time.time() - start_attempt)
                if shared_terminable is not None:
                    shared_terminable.terminable_sleep(sleep_time)
                else:
                    time.sleep(max(0., sleep_time))

        if shared_terminable is None or shared_terminable.should_run:
            raise Exception(f"Guest server is not ready after {max_retries} attempts "
                            f"({time.time() - start_wait} sec.).")

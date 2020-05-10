"""
Author: Eyal Posener, Orna Agmon Ben Yehuda, Liran Funaro <liran.funaro@gmail.com>
Technion - Israel Institute of Technology

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
import socket
import logging
import sys
import threading
import pickle
from socketserver import StreamRequestHandler, TCPServer, ThreadingMixIn
from typing import Optional

from mom.communication.ping import ping


def _dummy_echo_handler(msg):
    return msg


class _PickleRequestHandler(StreamRequestHandler):
    message_handler = _dummy_echo_handler

    def setup(self):
        StreamRequestHandler.setup(self)
        threading.current_thread().setName(f"handler-{self.server.display_name}")

    def handle(self):
        """ Created by a request """
        while True:  # Wil terminate on EOF
            try:
                msg = pickle.load(self.rfile)
                # process message in derived class method process()
                try:
                    reply = self.message_handler(msg)
                except Exception as err:
                    self.server.logger.exception("Error processing message: %s", err)
                    continue

                # send reply if necessary (if it is not None)
                try:
                    pickle.dump(reply, self.wfile)
                    self.wfile.flush()
                except Exception as err:
                    self.server.logger.warning("Error sending reply %s: %s", reply, err)
            except EOFError:
                self.server.logger.info("Server feed ended")
                break
            except Exception as err:
                self.server.logger.warning("Error receiving message: %s", err)


def request_handler_factory(timeout, message_handler=None):
    """
    Creates a StreamRequestHandler object with override timeout value.
    Implements the handle() method such that it read commands until
    connection is closed, and pass them to the server.process method.
    if server.process returns not None object, it will be returned as
    a reply to client.
    On any error, connection will be closed.
    """
    request_handler_type = type('LocalPickleRequestHandler', _PickleRequestHandler.__bases__,
                                dict(_PickleRequestHandler.__dict__))
    request_handler_type.timeout = timeout
    if message_handler is not None:
        request_handler_type.message_handler = message_handler
    return request_handler_type


class PickleTcpThreadedServer(ThreadingMixIn, TCPServer):
    daemon_threads = True
    block_on_close = False
    allow_reuse_address = True
    request_queue_size = 20

    def __init__(self, host, port, timeout=None, base_name=None, message_handler=None):
        """
        Initialize a server thread.
        @param host, port: Address is host:port, use host = "" for a INADDR_ANY
            network server
        @param timeout: timeout of session, will raise TimeoutError in process()
            method any time a message will not be received with the specified
            timeout.
            use timeout=None for infinite timeout,
            use timeout=0 to make process non-blocking.
        @param base_name: thread's and logger's name, use None for default name.
        """
        host_display = host if host else '*'
        if base_name is not None:
            self.display_name = f"{base_name}-{host_display}:{port}"
        else:
            self.display_name = f"{host_display}:{port}"
        self.full_name = f"{self.__class__.__name__}-{self.display_name}"

        self.logger = logging.getLogger(self.full_name)
        TCPServer.__init__(self, (host, port), request_handler_factory(timeout, message_handler))

    def handle_error(self, request, client_address):
        self.logger.exception("Exception happened during processing of request from %s: %s", client_address,
                              sys.exc_info()[0])


class DecodeDataTruncated(Exception):
    pass


class SimpleTcpClient:
    def __init__(self, host, port, timeout=None, base_name=None, verbose=True):
        """
        Initialize a client for TcpThreadedServer
        @param host, port: ip and port of server to connect to.
        @param timeout: timeout of session, will raise TimeoutError in
            send_recv() method any time a message will not be received with
            the specified timeout.
            use timeout = None for infinite timeout,
            use timeout = 0 to make process non - blocking.
        @param base_name: name of client, for logging purposes.
        """
        self.host = host
        self.port = port
        self.verbose = verbose
        self.sock: Optional[socket.socket] = None
        self.__lock = threading.RLock()
        self.timeout = None
        self.set_timeout(timeout)

        if base_name is None:
            base_name = "Client"
        self.name = f"{base_name}-{host}:{port}"

        self.default_buffer_len = 4096
        self.logger = logging.getLogger(self.name)

    @staticmethod
    def encode_message(msg) -> bytes:
        return str(msg).encode(encoding='utf-8')

    @staticmethod
    def decode_message(msg: bytes):
        return msg.decode(encoding='utf-8')

    def __del__(self):
        self.close()

    def __repr__(self):
        return self.name

    def __str__(self):
        return self.name

    def close(self):
        with self.__lock:
            try:
                self.sock.close()
            except:
                pass
            else:
                self.logger.debug("Connection closed")

            self.sock = None

    def connect(self):
        """ Connect to server if not already connected """
        with self.__lock:
            # if connected, return
            if self.sock is not None:
                return
            # try and connect:
            try:
                self.sock = socket.create_connection((self.host, self.port), self.timeout)
                self.logger.debug("Connected")
            except Exception as e:
                self.sock = None
                raise e

    def set_timeout(self, timeout):
        with self.__lock:
            self.timeout = timeout
            if self.sock is None:
                return
            self.sock.settimeout(timeout)

    def send_recv(self, msg, recv_len=None):
        """
        Send a message and receive a reply.
        steps:
            verify / connect to server
            send message
            receive reply if recv_len > 0
        """
        if recv_len is None:
            recv_len = self.default_buffer_len

        with self.__lock:
            # make sure the client is connected
            self.connect()

            # send message
            try:
                self.sock.sendall(self.encode_message(msg))

                if recv_len == 0:
                    return

                replay_data = []
                while True:
                    replay = self.sock.recv(recv_len)
                    if not replay:
                        raise DecodeDataTruncated("Got insufficient replay data.")

                    replay_data.append(replay)

                    # parse replay
                    try:
                        ret = self.decode_message(b"".join(replay_data))
                        piece_count = len(replay_data)
                        if piece_count > 1:
                            self.default_buffer_len = piece_count * recv_len
                            self.logger.debug("Got message in %s pieces of %s bytes. Updating default to: %s.",
                                              len(replay_data), recv_len, self.default_buffer_len)
                        return ret
                    except DecodeDataTruncated as e:
                        self.logger.debug("Need more info: %s", e)
            except Exception as e:
                # close connection in send-recv only when error accrued
                self.close()
                ping(self.host)  # test if the host is alive
                raise e


class PickleTcpClient(SimpleTcpClient):
    @staticmethod
    def encode_message(msg) -> bytes:
        return pickle.dumps(msg)

    @staticmethod
    def decode_message(msg: bytes):
        try:
            return pickle.loads(msg)
        except pickle.UnpicklingError as e:
            msg = str(e).strip().lower()
            if "pickle data was truncated" in msg:
                raise DecodeDataTruncated(msg)
            else:
                raise e
        except EOFError as e:
            raise DecodeDataTruncated(str(e))

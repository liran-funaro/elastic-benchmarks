import time
from copy import deepcopy
from mom.communication.pickle_socket_server import PickleTcpThreadedServer
from mom.communication.messages import Message
from mom.config import DictConfig


class GuestServer(PickleTcpThreadedServer):
    """
    A simple TCP server that implements the guest side of the guest network
    Collector.
    """
    def __init__(self, config: DictConfig, monitor, policy, guest_name=None):
        self.config = config
        self.monitor = monitor
        self.policy = policy
        self.data = {}

        self.host = self.config.get('server', 'host')
        self.port = self.config.get('server', 'port')

        PickleTcpThreadedServer.__init__(self, self.host, self.port, timeout=None, base_name=guest_name,
                                         message_handler=self.process_message)

    def process_message(self, message: Message):
        response_message = {}
        start_process = time.time()
        try:
            response = message.process(self.data, self.monitor, self.policy)
            response_message['ack'] = True
            response_message['response'] = response
        except Exception as e:
            self.logger.exception("Failed to process message %s: %s", message, e)
            response_message['ack'] = False
            response_message['error'] = str(e)
            response_message['response'] = None
        end_process = time.time()

        response_message['process-time'] = end_process - start_process
        return response_message

    def interrogate(self):
        return deepcopy(self.data)

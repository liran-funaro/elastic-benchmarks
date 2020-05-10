import time
from typing import Dict

from mom.logged_object import LoggedObject


class MessageError(Exception):
    pass


class Message(LoggedObject):
    """ Abstract class for a message send-respond mechanism """
    __slots__ = 'content',

    def __init__(self, *args, **kwargs):
        LoggedObject.__init__(self)
        self.content = dict(*args, **kwargs)

    @property
    def name(self):
        return self.__class__.__name__

    def __repr__(self):
        """ Convert a message to string. """
        return f"{self.__class__.__name__}({self.content})"

    @staticmethod
    def process(_data: Dict, _monitor, _policy) -> Dict:
        """
        @param _data - a DictDefaultNone instance.
        @param _monitor -  an Entity instance.
        @param _policy -  an Entity instance.
        Activated on the receiver's side to process the received message.
        """
        return {}


class EchoMessage(Message):
    pass


class MessageTargetAllocation(Message):
    """ Wait for new host notification and respond with it.  """
    def process(self, data: Dict, _monitor, policy) -> Dict:
        timeout = self.content.get('timeout', None)
        is_new_notification = policy.wait_for_notify(timeout)
        notification = data.get("notify", {})
        if notification is not None:
            notification['is_new_notification'] = is_new_notification
        return notification


class MessageUpdateResourceDiff(Message):
    """ Updates the policy about the difference between the allocated resource and the seen resource """
    def process(self, data: Dict, monitor, policy) -> Dict:
        policy.update_resource_diff(self.content)
        return Message.process(data, monitor, policy)


class MessageUpdateApplicationTarget(Message):
    """ Updates the policy about the difference between the allocated resource and the seen resource """
    def process(self, data: Dict, monitor, policy) -> Dict:
        app_target_data = data.setdefault('app-target', {})
        app_target_data.update(self.content)
        app_target_data['update-time'] = time.time()
        return Message.process(data, monitor, policy)


class MessageInquiry(Message):
    def process(self, data: Dict, monitor, policy) -> Dict:
        # guest update data with the host's notification
        inquiry_data = data.setdefault('inquiry', {})
        inquiry_data.update(self.content)
        inquiry_data['update-time'] = time.time()
        return policy.inquiry(inquiry_data)


class MessageNotify(Message):
    """
    Host notify the guest on allocation changes.
    Guest respond with ack (default).
    """
    def process(self, data: Dict, monitor, policy) -> Dict:
        # guest update data with the host's notification
        notify_data = data.setdefault('notify', {})
        notify_data.update(self.content)
        notify_data['update-time'] = time.time()
        policy.notify(notify_data)
        return Message.process(data, monitor, policy)

"""
Author: Liran Funaro <liran.funaro@gmail.com>
Based on code by Eyal Posner

Copyright (C) 2006-2018 Liran Funaro

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
import signal
import threading
import time
from typing import Union, Optional

from mom.logged_object import LoggedThread


DEFAULT_MINIMAL_SLEEP = 1e-2


class Terminable:
    def __init__(self, shared_terminable: Optional['Terminable'] = None, minimal_sleep: Union[float, int, None] = None):
        if shared_terminable is None:
            self._termination_event = threading.Event()
            self._minimal_sleep = DEFAULT_MINIMAL_SLEEP
        else:
            self.share_termination(shared_terminable)

        if minimal_sleep is not None:
            self._minimal_sleep = minimal_sleep

    def keyboard_interrupt_handler(self, _signalnum, _frame):
        self.terminate()

    def terminate_on_signal(self, signalnum=signal.SIGINT):
        signal.signal(signalnum, self.keyboard_interrupt_handler)

    def share_termination(self, shared_terminable: 'Terminable'):
        self._termination_event = shared_terminable._termination_event
        self._minimal_sleep = shared_terminable._minimal_sleep

    def terminate(self):
        self._termination_event.set()

    @property
    def should_run(self):
        return not self._termination_event.is_set()

    def terminable_sleep(self, timeout: Union[float, int]):
        if timeout > self._minimal_sleep:
            return not self._termination_event.wait(timeout)
        else:
            return self.should_run

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['_termination_event']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._termination_event = threading.Event()


class DeferredStartThread(LoggedThread, Terminable):
    def __init__(self, *args,
                 shared_deferred_start: Optional['DeferredStartThread'] = None,
                 shared_terminable: Optional['Terminable'] = None, minimal_sleep: Union[float, int, None] = None,
                 start_timeout=None, **kwargs):
        LoggedThread.__init__(self, *args, **kwargs)
        Terminable.__init__(self, shared_terminable=shared_terminable, minimal_sleep=minimal_sleep)

        self._start_time = None

        if shared_deferred_start is None:
            self._shared_deferred_object: Optional[DeferredStartThread] = None
            self._start_event = threading.Event()
            self._start_timeout = None
        else:
            self.share_start(shared_deferred_start)

        if start_timeout is not None:
            self._start_timeout = start_timeout

    def share_start(self, shared_deferred_start: 'DeferredStartThread'):
        self._shared_deferred_object = shared_deferred_start.main_deferred_start
        self._start_event = shared_deferred_start._start_event
        self._start_timeout = shared_deferred_start._start_timeout
        self._start_time = None

    @property
    def start_time(self):
        if self._shared_deferred_object is not None:
            return self._shared_deferred_object.start_time
        else:
            return self._start_time

    @property
    def main_deferred_start(self):
        if self._shared_deferred_object is None:
            return self
        else:
            return self._shared_deferred_object.main_deferred_start

    def go(self):
        if self._shared_deferred_object is not None:
            self._shared_deferred_object.go()
        else:
            self._start_event.set()
            self._start_time = time.time()

    def terminate(self):
        Terminable.terminate(self)
        # Make sure we don't hang while waiting for start event
        self.go()

    def run(self) -> None:
        self.logger.info("Started deferred. Waiting for start event...")
        self._start_event.wait(self._start_timeout)
        if self.should_run:
            LoggedThread.run(self)

    def __getstate__(self):
        state = Terminable.__getstate__(self)
        del state['_start_event']
        return state

    def __setstate__(self, state):
        Terminable.__setstate__(self, state)
        self._start_event = threading.Event()

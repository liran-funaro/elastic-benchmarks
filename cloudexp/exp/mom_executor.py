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
from typing import Union

from mom.config import DictConfig
from mom.momhostd import MomHostDaemon
from mom.util.terminable import Terminable, DeferredStartThread


class MomThreadExecutor(DeferredStartThread):
    def __init__(self, config_options: Union[DictConfig, dict, None] = None, shared_terminable: Terminable = None):
        DeferredStartThread.__init__(self, daemon=True, shared_terminable=shared_terminable)

        self.mom = MomHostDaemon(config_options, shared_terminable=shared_terminable)

    def terminate(self):
        self.mom.terminate()
        DeferredStartThread.terminate(self)

    def start_and_wait_for_guests(self, number_of_guests):
        # Start the thread that waits for go() command
        self.start()

        self.logger.info("Start guest manager, and wait for all guest monitors to be ready...")
        if not self.mom.guest_manager.is_alive():
            self.mom.guest_manager.start()
        while self.should_run and not self._all_guests_ready(number_of_guests):
            self.terminable_sleep(1)

        if self.should_run:
            self.go()
        else:
            self.terminate()

    def _all_guests_ready(self, number_of_guests):
        state = self.mom.guest_manager.get_guests_readiness()

        if len(state) < number_of_guests:
            self.logger.debug("Checking readiness: not enough guests! (%i out of %i)", len(state), number_of_guests)
            return False

        return all(state.values())

    def logged_run(self) -> None:
        self.mom.run_mom()

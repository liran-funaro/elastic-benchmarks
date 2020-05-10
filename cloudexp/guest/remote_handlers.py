"""
Author: Liran Funaro <liran.funaro@gmail.com>

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
import logging
from resource import setrlimit, RLIMIT_CORE, RLIM_INFINITY
from typing import Union

from cloudexp.guest.application import Application
from cloudexp.util import logs
from mom.config import DictConfig


def set_core_unlimited():
    setrlimit(RLIMIT_CORE, (RLIM_INFINITY, RLIM_INFINITY))


def remote_application(app: Application, verbosity):
    logs.start_stdio_logging(verbosity)
    logger = logging.getLogger("remote-application")

    set_core_unlimited()
    logger.info("Starting application: %s", app.__class__.__name__)
    try:
        app.run()  # blocking
    except Exception as e:
        logger.exception("Exception in remote application: %s", e)
    finally:
        logger.info("Application terminated")


def remote_guest_server(verbosity, guest_config: Union[DictConfig, dict, None] = None, guest_name=None):
    logs.start_stdio_logging(verbosity)
    logger = logging.getLogger("remote-guest-server")

    from mom.momguestd import MomGuestDaemon
    mom_guest = MomGuestDaemon(guest_config, guest_name)

    logger.info("Starting guest server")
    try:
        mom_guest.start()  # blocking
    except Exception as e:
        logger.exception("Exception in remote guest server: %s", e)
    finally:
        logger.info("Guest server terminated")
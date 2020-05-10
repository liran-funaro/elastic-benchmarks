"""
Author: Liran Funaro <liran.funaro@gmail.com>
@author: eyal

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
import os
import re

from cloudexp.util import shell
from mom.logged_object import LoggedThread

ARP_REGEX = re.compile(r"\?[ \t]+\((?P<ip>[0-9\.]+)\)[ \t]+at[ \t]+(?P<mac>[0-9a-fA-F:-]+)[ \t]+.*")
PING_LOSS_REGEX = re.compile(r"(?P<transmitted>[0-9]+)[ \t]+packets transmitted[, \t]+"
                             r"(?P<received>[0-9]+)[ \t]+received[, \t]+"
                             r"(?P<loss>[0-9]+)%[ \t]+packet loss[, \t]+"
                             r"time[ \t]+(?P<time>[0-9]+)ms[ \t]*")


def find_ip(mac_address):
    """
    Find IP address via known MAC address using ARP
    :param mac_address: MAX address
    :return: IP address or None
    """
    arp_info = os.popen("arp -an").readlines()
    for l in arp_info:
        m = ARP_REGEX.match(l)
        if m is None:
            continue
        if m.group('mac') == mac_address:
            return m.group('ip')
    return None


def ping_background(ip, repeat=1, logger=None):
    """
    Ping an IP and log the results in a background daemon thread
    :param ip: The IP address
    :param repeat: Ping repeats
    :param logger: The logger to log to
    :return: The thread object
    """
    t = LoggedThread(name="ping %s" % ip,
                     target=ping_log,
                     args=(ip, repeat, logger),
                     daemon=True)
    t.start()
    return t


def ping_log(ip, repeat=1, logger=None):
    """
    Ping an IP and log the results
    :param ip: The IP address
    :param repeat: Ping repeats
    :param logger: The logger to log to
    :return: None
    """
    transmitted, received, loss, time = ping(ip, repeat)
    if logger:
        if loss == 0:
            logger.info("ping %s OK, transmitted: %s, received: %s, delay: %.2fs",
                        ip, transmitted, received, time)
        else:
            logger.warn("ping %s ERROR: packet loss, transmitted: %s, received: %s, loss: %s%%, delay: %.2fs",
                        ip, transmitted, received, loss, time)
    else:
        logger.info("%s : OK, delay: %.2fs", ip, time if loss == 0 else "Unreachable")


def ping(ip, repeat=1):
    """
    Ping an IP
    :param ip: The IP address
    :param repeat: Ping repeats
    :return: transmitted, received, loss, time
    """
    ip = str(ip)
    default_return = 0, 0, 100, 0
    try:
        ping_out, ping_err = shell.run(["ping", "-c", str(repeat), ip])
        res = PING_LOSS_REGEX.findall(ping_out)
        if len(res) == 0:
            return default_return
        else:
            return tuple(map(int, res[0]))
    except ValueError:
        return default_return


if __name__ == "__main__":
    ping_background("www.google.com")
    ping_background("1.1.0.1")

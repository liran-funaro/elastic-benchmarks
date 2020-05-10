"""
@author: eyal
"""
import re
import sys
import time
import logging
from subprocess import Popen, PIPE

PACKET_LOSS_RE = re.compile(r'(\d+)\s*%?\s+packet loss', re.IGNORECASE | re.MULTILINE)


def ping(ip):
    logger = logging.getLogger(f"ping-{ip}")

    start = time.time()
    out, err = Popen(["ping", "-c", "1", str(ip)], encoding='utf-8', stdout=PIPE, stderr=PIPE).communicate()
    ping_runtime = time.time() - start
    m = PACKET_LOSS_RE.search(out)
    if m is None:
        loss = 100.
    else:
        loss = float(m.group(1))

    if loss < sys.float_info.epsilon:
        logger.debug("ping successful, delay: %.2fs", ping_runtime)
    else:
        logger.debug("ping ERROR: packet loss: %s", loss)


if __name__ == "__main__":
    ping("www.google.com")
    ping("1.1.0.1")

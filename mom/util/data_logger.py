"""
Author: Liran Funaro <liran.funaro@gmail.com>

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
import logging
from threading import RLock
from typing import Optional

import msgpack
import msgpack_numpy as m

m.patch()


__write_lock = RLock()
__log_file_path: Optional[str] = None

logger = logging.getLogger("DataLogger")


def start_data_logging(log_file_path):
    global __log_file_path
    __log_file_path = log_file_path
    logger.info(f"Start logging data to: {log_file_path}")


def stop_data_logging():
    global __log_file_path
    logger.info(f"Stop logging data to: {__log_file_path}")
    __log_file_path = None


def append_data(log_type, log_source, data, sample_start, sample_end):
    if __log_file_path is None:
        return

    log_data = dict(
        type=log_type,
        source=log_source,
        sample_start=sample_start,
        sample_end=sample_end,
        interval=sample_end-sample_start,
        **data,
    )

    with __write_lock:
        with open(__log_file_path, 'ab') as f:
            f.write(msgpack.packb(log_data, use_bin_type=True))


def data_log_unpacker(log_file_path, handle_func=lambda x: x):
    with open(log_file_path, 'rb') as f:
        unpacker = msgpack.Unpacker(f, raw=False)
        yield from handle_func(unpacker)


def read_data_log(log_file_path):
    return list(data_log_unpacker(log_file_path))


class DataLogger:
    def __init__(self, log_type, log_source):
        self.log_type = log_type
        self.log_source = log_source

    def append_data(self, data, sample_start, sample_end):
        append_data(self.log_type, self.log_source, data, sample_start, sample_end)

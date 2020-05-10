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
import re
import os
import sys
import logging
import datetime
from typing import Dict, Union, List
from logging import Handler, StreamHandler
from logging.handlers import RotatingFileHandler

from cloudexp.util.timeformat import time_delta
from mom.logged_object import LoggedThread

FMT = "{asctime} | {levelname:<8} | {processName}\t | {threadName}\t | {name}\t | " \
      "{funcName}:{lineno} | {message}"

LOG_HEADERS = 'time', 'level', 'source', 'process', 'thread', 'module', 'function:line', 'message'
LOG_HEADERS_INDEX = {h: i for i, h in enumerate(LOG_HEADERS)}
LOG_REGEXP = re.compile(r'^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2}),(\d{3}) \| '  # time
                        r'(\S+)[ ]* \| '  # level
                        r'([^\t]+)\t \| '  # process
                        r'([^\t]+)\t \| '  # thread
                        r'([^\t]+)\t \| '  # name
                        r'([^ \t:]+:\d+) \| ',  # function:line
                        re.I | re.MULTILINE)


def get_row_time(row: List[str], default_time=None):
    try:
        y, m, d, h, mm, s, ms = map(int, row[:7])
        return datetime.datetime(y, m, d, h, mm, s, ms * 1000)
    except:
        return default_time


def convert_time(cur_time: datetime.datetime, init_time: datetime.datetime):
    try:
        t = cur_time - init_time
    except:
        t = 0
    return time_delta(t.total_seconds(), time_annotations='short', show_seconds=True)


def split_log(log_str):
    log_list = LOG_REGEXP.split(log_str)
    first_item, *log_list = log_list
    if first_item.strip():
        log_list = ['ERROR']*12 + [first_item] + log_list
    return log_list


def get_joint_raw_logs(logs_path: str, log_data=None, use_time_delta=False):
    if log_data is None:
        log_data = {}
    joint_log = []
    for file_name in os.listdir(logs_path):
        log_name, ext = os.path.splitext(file_name)
        if ext != '.log':
            continue
        file_path = os.path.join(logs_path, file_name)
        creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
        with open(file_path, 'r') as f:
            cur_pos = log_data.get(file_path, 0)
            f.seek(cur_pos)
            log = f.read()
            log_data[file_path] = f.tell()

        log = zip(*[iter(split_log(log))] * 13)

        def get_row(row):
            return [get_row_time(row, creation_time), row[7], log_name, *row[8:-1], row[-1][:-1]]

        joint_log.extend(map(get_row, log))

    if len(joint_log) == 0:
        return joint_log

    joint_log = sorted(joint_log, key=lambda x: x[0])
    if not use_time_delta:
        return joint_log

    init_time = log_data.get('init-time', joint_log[0][0])

    for r in joint_log:
        r[0] = convert_time(r[0], init_time)
    log_data['init-time'] = init_time
    return joint_log


def count_errors_and_warnings(logs_path: str = None, joint_log: list = None):
    if logs_path is not None:
        joint_log = get_joint_raw_logs(logs_path, use_time_delta=False)
    elif joint_log is None:
        raise ValueError("Must pass logs_path or the actual joint_log")
    errors = 0
    warnings = 0
    i = LOG_HEADERS.index('level')
    for row in joint_log:
        level = row[i].lower()
        if level == 'error':
            errors += 1
        elif level == 'warning':
            warnings += 1

    return errors, warnings


def get_log_stats(logs_path: str):
    joint_log = get_joint_raw_logs(logs_path, use_time_delta=False)
    if len(joint_log) == 0:
        return None

    time_index = LOG_HEADERS.index('time')
    errors, warnings = count_errors_and_warnings(joint_log=joint_log)
    return {
        'length': len(joint_log),
        'start-time': joint_log[0][time_index],
        'end-time': joint_log[-1][time_index],
        'errors': errors,
        'warnings': warnings,
    }


VERBOSITY_TRANSLATE = {
    'debug': logging.DEBUG, '5': logging.DEBUG,
    'info': logging.INFO, '4': logging.INFO,
    'warning': logging.WARNING, 'warn': logging.WARNING, '3': logging.WARNING,
    'error': logging.ERROR, '2': logging.ERROR,
    'critical': logging.CRITICAL, '1': logging.CRITICAL,
}


def get_verbosity_level(verbosity: Union[str, int]):
    if not isinstance(verbosity, str):
        return verbosity
    return VERBOSITY_TRANSLATE.get(verbosity.lower(), logging.DEBUG)


logging_handlers: Dict[str, Handler] = {}


def start_file_logging(filename, verbosity=logging.DEBUG, max_bytes=67108864, backups_count=1000):
    verbosity = get_verbosity_level(verbosity)
    cur_handler = logging_handlers.get(filename, None)
    if cur_handler is not None:
        cur_handler.setLevel(verbosity)
        return False

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    if filename == 'stdio':
        handler = StreamHandler(sys.stderr)
    else:
        handler = RotatingFileHandler(filename, 'a', max_bytes, backups_count)
    handler.setLevel(verbosity)
    handler.setFormatter(logging.Formatter(FMT, style='{'))
    logger.addHandler(handler)
    logging_handlers[filename] = handler
    return True


def start_stdio_logging(verbosity=logging.DEBUG):
    return start_file_logging('stdio', verbosity)


def stop_file_logging(key):
    try:
        logging.log(logging.DEBUG, "Stopping logging to %s", key)
        logging.getLogger().removeHandler(logging_handlers[key])
        del logging_handlers[key]
        return True
    except KeyError:
        logging.log(logging.WARN, "No logging to %s", key)
        return False


def stop_stdio_logging():
    return stop_file_logging('stdio')


class OutputLogThread(LoggedThread):
    def __init__(self, stream, name=None, log_level=logging.INFO):
        LoggedThread.__init__(self, name=name, daemon=True)
        self.stream = stream
        self.log_level = log_level
        self.line_count = 0
        self.err_output = []

    def logged_run(self) -> None:
        try:
            for line in iter(self.stream.readline, ''):
                line = line.strip()
                if self.log_level is not None:
                    self.logger.log(self.log_level, line.strip())
                else:
                    self.err_output.append(line)
                self.line_count += 1
        except EOFError:
            pass

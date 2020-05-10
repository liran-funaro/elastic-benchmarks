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
import pprint
import logging
import tempfile
from collections import OrderedDict

from cloudexp.guest.application.benchmark import BenchmarkError, BenchmarkLoad, BenchmarkDuration, BenchmarkResults, \
    Benchmark
from cloudexp.util.logs import OutputLogThread
from elasticbench import settings
from elasticbench.apps.memcached import memcached_port

from mom.util.parsers import parameter_int_or_float_or_str

MEMSLAP_OUTPUT_SEPARATOR = b'\x1b[1;1H\x1b[2J'.decode('utf-8')
MEMSLAP_RESULT_TITLE_PATTERN = re.compile(r'^\s*(Get|Set|Total)\s*Statistics\s*$', re.I | re.M)
MEMSLAP_INFO_PATTERN = re.compile(r'^\s*(servers|threads\s+count|concurrency|run\s+time|windows\s+size|'
                                  r'set\s+proportion|get\s+proportion)\s*:\s*(.*\S)\s*$', re.I | re.M)


"""
Output fields example (get/set/total: period/global):
avg(us): 437
geo_dist: 437.26
get_miss: 0
max(us): 742
min(us): 411
net(m/s): 21.0
ops: 20263
std_dev: 32.93
time(s): 1
tps(ops/s): 20263
"""

# If more than one error match, we choose the first matching error.
known_errors = OrderedDict([
    ("SERVER_ERROR out of memory storing object", logging.DEBUG),
    ("SERVER_ERROR", logging.ERROR),
])


class MemSlap(Benchmark):
    # Development page and source: https://launchpad.net/libmemcached
    # Documentation: http://docs.libmemcached.org/bin/memaslap.html?highlight=memaslap

    def __init__(self, keys_dist=None, vals_dist=None, cmd_get_percent=None, win_size=None, seed=None,
                 stats_frequency=5, mode='remote', memslap_cmd='memaslap'):
        """
        keys_dist, vals_dist: key/value distribution
        cmd_get_percent: gets percent (from total requests)
        win_size: window size
        seed: seed for memslap
        stats_frequency: stats output frequency (seconds)
        mode: 'remote' - run on guest, 'local' - run on host
        memslap_cmd: testing the new version of memslap (1.0.17), where the similar one is called mem*a*slap
        """
        Benchmark.__init__(self, is_remote=(mode.lower() == 'remote'))

        config = []
        if keys_dist is not None:
            config.extend(['key', *("%i %i %.1f" % tpl for tpl in keys_dist)])

        if vals_dist is not None:
            config.extend(['value', *("%i %i %.1f" % tpl for tpl in vals_dist)])

        if cmd_get_percent is not None:
            config.extend(['cmd', f'0 {1 - cmd_get_percent:.1f}', f'1 {cmd_get_percent:.1f}'])

        if config:
            config.append('')
            self.config_str = "\n".join(config)
        else:
            self.config_str = None
        self._config_file_name = None

        self.win_size = win_size

        self.seed = seed
        self.stats_frequency = stats_frequency
        self._memslap_cmd = memslap_cmd

    @property
    def config_file(self):
        if self.config_str is None:
            return None

        if self._config_file_name is None:
            with tempfile.NamedTemporaryFile(delete=False, prefix='memaslap-config-', mode='w+') as f:
                f.write(self.config_str)
                f.flush()
                temp_file = f.name

            if not self.is_remote:
                self._config_file_name = temp_file
            else:
                remote_tmp_folder = "/tmp/"
                self.rsync(temp_file, remote_tmp_folder)
                self._config_file_name = os.path.join(remote_tmp_folder, os.path.basename(temp_file))

        return self._config_file_name

    @property
    def memcached_address(self):
        if self.is_remote:
            return f"localhost:{memcached_port}"
        else:
            return f"{self.application_ip}:{memcached_port}"

    @property
    def memslap_cmd(self):
        if self.is_remote:
            return os.path.join('libmemcached-1.0.18', 'clients', self._memslap_cmd)
        else:
            return settings.get_application_path('libmemcached-1.0.18', 'clients', self._memslap_cmd)

    def consume(self, load: BenchmarkLoad, duration: BenchmarkDuration) -> BenchmarkResults:
        args = [
            self.memslap_cmd,
            f"--servers={self.memcached_address}",
            f"--concurrency={load}",
            f"--time={duration}s",
            "--reconnect",
        ]

        if self.win_size is not None:
            args.append(f"--win_size={self.win_size}")
        if self.stats_frequency is not None:
            args.append(f"--stat_freq={self.stats_frequency}s")
        if self.config_file is not None:
            args.append(f"--cfg_cmd={self.config_file}")
        if self.seed is not None:
            args.append(f"--seed={self.seed}")

        p = self.async_run_command(args, verbose=False)  # non blocking
        err_log = OutputLogThread(p.stderr, name=self.__log_name__, log_level=logging.ERROR if duration > 5 else None)
        err_log.start()
        yield from self.iter_parse_lines(p.stdout)  # blocking

        if err_log.line_count > 0:
            if err_log.err_output:
                errors = '\n'.join(err_log.err_output)
                raise BenchmarkError(f"Benchmark issued errors.\n{errors}")
            else:
                raise BenchmarkError("Benchmark issued errors. See log for more information.")

    def iter_parse_lines(self, stdout):
        for lines in self.iter_accumulated_lines(stdout):
            ret = self.parse_lines(lines)
            if not ret:
                continue
            offset = ret.get('total', {}).get('global', {}).get('time(s)', None)
            if offset is None:
                self.logger.warning("Current result does not contain time offset: %s", pprint.pformat(ret))
                offset = 0
            yield offset, ret

    @staticmethod
    def iter_accumulated_lines(stdout):
        accumulated_lines = []
        # Iterate line by line, strip it and filter empty lines. Ending when got an empty line (EOF)
        for line in iter(stdout.readline, ''):
            line = line.strip()
            if not line:
                continue
            # Accumulating lines until a separator is found, then yielding all the lines.
            if MEMSLAP_OUTPUT_SEPARATOR in line:
                if accumulated_lines:
                    yield accumulated_lines
                accumulated_lines = []
            else:
                accumulated_lines.append(line)

    @staticmethod
    def parse_errors(cur_line, error_log):
        for e in known_errors.keys():
            if e in cur_line:
                error_log.setdefault(e, 0)
                error_log[e] += 1
                return True

        return False

    def parse_info(self, cur_line):
        m = MEMSLAP_INFO_PATTERN.match(cur_line)
        if m:
            self.logger.info("[benchmark parameter] %s: %s", *m.groups())
            return True
        return False

    def parse_lines(self, lines):
        ret = {}
        op = None
        headers = None
        error_log = OrderedDict()

        for l in lines:
            if not l:
                # Empty line. This means the table has ended.
                op = None
                headers = None
                continue
            elif self.parse_errors(l, error_log) or self.parse_info(l):
                continue

            # First, match an OP (get/set/total) and split to columns.
            op_m = MEMSLAP_RESULT_TITLE_PATTERN.match(l)
            cols = l.split()
            if op_m is not None:
                # If we match an OP, then starting a new table.
                op = op_m.group(1).lower()
                headers = None
            elif op is None:
                # If we did not found an OP and no OP is set, then it is an unrecognized line.
                self.logger.warning("Unrecognized line: %s", l)
            elif headers is None:
                # If headers is not set, then this is an header row.
                headers = [c.lower() for c in cols]
            elif len(cols) == len(headers):
                # If we found an OP and a header row with the same length, then this is its data row.
                d = dict(zip(headers, map(parameter_int_or_float_or_str, cols)))
                t = d.pop('type').lower()
                ret.setdefault(op, {})[t] = d
            else:
                # If we are in the middle of a table but the line doesn't have the expected number of columns,
                # then it is an unrecognized line.
                self.logger.warning("Unrecognized line: %s", l)

        for e, c in error_log.items():
            self.logger.log(known_errors[e], "Known error appeared %s time(s): %s", c, e)

        return ret

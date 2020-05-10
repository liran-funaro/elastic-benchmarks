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
import argparse

from cloudexp.monitor import run_monitor
from elasticbench import settings


def parse_launch_args():
    parser = argparse.ArgumentParser(prog='elasticbench', description='Elastic Benchmarks.')
    parser.add_argument('-m', '--monitor', action='store_true',
                        help=f'Start experiment monitor.')

    return parser.parse_args()


def main():
    args = parse_launch_args()

    if args.monitor:
        run_monitor(settings.read_output_path())


main()

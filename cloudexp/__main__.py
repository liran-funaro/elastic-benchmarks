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
from cloudexp.exp.single import launch_experiment
from cloudexp.exp.batch import launch_batch_experiment
from cloudexp.util.process import convert_to_daemon


def parse_launch_args():
    import argparse

    parser = argparse.ArgumentParser(prog='cloudexp', description='Experiment.')
    parser.add_argument('outputpath', help='Experiment information.')
    parser.add_argument('-v', '--verbosity', default='info', help='Experiment launcher verbosity.')
    parser.add_argument('-o', '--overwrite', action='store_true', help='Overwrite previous results.')
    parser.add_argument('-d', '--daemon', action='store_true', help='Run as daemon (exit immediately).')
    parser.add_argument('-b', '--batch', action='store_true',
                        help=f'Recursively iterate through folders looking for experiment arguments file.')

    return parser.parse_args()


def main():
    args = parse_launch_args()

    if args.daemon:
        convert_to_daemon()

    if not args.batch:
        launch_experiment(args.outputpath, verbosity=args.verbosity, overwrite=args.overwrite)
    else:
        launch_batch_experiment(args.outputpath, verbosity=args.verbosity, overwrite=args.overwrite)


main()

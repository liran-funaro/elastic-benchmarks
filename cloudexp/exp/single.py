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
import os
import logging
import subprocess
from contextlib import closing
from typing import Optional

import cloudexp
import cloudexp.util.process
from cloudexp.util import logs
from cloudexp.exp import validate_no_experiments, backup_experiment, load_experiment_args
from cloudexp.exp.experiment import Experiment
from cloudexp.util.thread_monitor import AliveThreadMonitor
from cloudexp.exp import DEFAULT_LOG_FILE_NAME, DEFAULT_DATA_FILE_NAME, DEFAULT_LOCAL_PID_FILE_NAME,\
    DEFAULT_GLOBAL_PID_FILE_PATH

from mom.util import data_logger


def terminate_experiment(output_path, sig='int'):
    pid_file = os.path.join(output_path, DEFAULT_LOCAL_PID_FILE_NAME)
    return cloudexp.util.process.terminate_from_pid_file(pid_file, sig)


def launch_experiment(output_path, verbosity, overwrite=False, process=False,
                      daemon=False) -> Optional[subprocess.Popen]:
    if not os.path.isdir(output_path):
        raise NotADirectoryError("Experiment path must be a directory.")

    log_file = os.path.join(output_path, DEFAULT_LOG_FILE_NAME)
    if os.path.exists(log_file) and not overwrite:
        raise FileExistsError(f"Log file '{log_file}' already exists.")

    validate_no_experiments(output_path)

    backup_experiment(output_path, new_args=False)

    if process or daemon:
        args = ['python', '-O', '-m', cloudexp.__name__, output_path, '-v', verbosity]
        if overwrite:
            args.append('-o')
        if daemon:
            args.append('-d')
        return subprocess.Popen(args)

    data_file = os.path.join(output_path, DEFAULT_DATA_FILE_NAME)
    logs.start_file_logging(log_file, verbosity)
    data_logger.start_data_logging(data_file)
    logger = logging.getLogger("experiment-executor")

    local_pid_file = os.path.join(output_path, DEFAULT_LOCAL_PID_FILE_NAME)
    cloudexp.util.process.write_pid_to_files(DEFAULT_GLOBAL_PID_FILE_PATH, local_pid_file, logger=logger)

    logger.info("Reading experiment data from: %s", output_path)
    experiment_kwargs = load_experiment_args(output_path)

    try:
        on_start = experiment_kwargs.pop('on_start', None)
        on_finish = experiment_kwargs.pop('on_finish', None)

        if on_start and hasattr(on_start, '__call__'):
            try:
                on_start(logger, output_path=output_path, **experiment_kwargs)
            except Exception as e:
                logger.exception("on_start() execution failed: %s", e)

        exp_error = None
        try:
            with closing(Experiment(output_path=output_path, **experiment_kwargs)) as exp_obj:
                exp_obj.start_experiment()
        except Exception as e:
            logger.exception("Experiment.start() execution failed: %s", e)
            exp_error = e

        if exp_error is not None:
            logger.error("Experiment.start() execution failed: %s", exp_error)

        if on_finish and hasattr(on_finish, '__call__'):
            try:
                on_finish(logger, output_path=output_path, exp_error=exp_error, **experiment_kwargs)
            except Exception as e:
                logger.exception("on_finish() execution failed: %s", e)

        AliveThreadMonitor(1).run()
    except Exception as e:
        logger.exception("Experiment uncaught error: %s", e, exc_info=1)
    finally:
        data_logger.stop_data_logging()
        logs.stop_file_logging(log_file)

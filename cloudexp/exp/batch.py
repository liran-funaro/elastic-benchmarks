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
import os
import logging
import signal

import cloudexp
import subprocess

from cloudexp import exp
import cloudexp.exp.single
import cloudexp.util.process
from cloudexp.util import logs
import cloudexp.exp.experiment
from cloudexp.util.timeformat import time_delta
from cloudexp.exp import load_experiment_args, DEFAULT_BATCH_LOG_FILE_NAME, \
    DEFAULT_BATCH_LOCAL_PID_FILE_NAME, DEFAULT_BATCH_GLOBAL_PID_FILE_PATH, find_all_sub_experiments


def is_batch_experiment_running(output_path):
    local_pid_file = os.path.join(output_path, DEFAULT_BATCH_LOCAL_PID_FILE_NAME)
    pid, alive = cloudexp.util.process.read_pid_file(local_pid_file)
    return pid is not None and alive


def get_running_batch_experiment(output_path):
    local_pid_file = os.path.join(output_path, DEFAULT_BATCH_LOCAL_PID_FILE_NAME)
    return cloudexp.util.process.find_active_form_pid_files(DEFAULT_BATCH_GLOBAL_PID_FILE_PATH, local_pid_file)


def validate_no_batch_experiments(output_path):
    running_experiments = get_running_batch_experiment(output_path)
    if running_experiments:
        raise cloudexp.exp.ExperimentAlreadyRunningError(f"Batch experiment are running: {running_experiments}.")


def launch_batch_experiment(output_path, verbosity, overwrite=False, process=False, daemon=False):
    if not os.path.isdir(output_path):
        raise NotADirectoryError("Experiment path must be a directory.")

    validate_no_batch_experiments(output_path)
    exp.validate_no_experiments(output_path)

    for f_name in os.listdir(output_path):
        name, ext = os.path.splitext(f_name)
        if ext in ('.log', '.pid'):
            os.remove(os.path.join(output_path, f_name))

    if process or daemon:
        args = ['python', '-O', '-m', cloudexp.__name__, output_path, '-v', verbosity, '-b']
        if overwrite:
            args.append('-o')
        if daemon:
            args.append('-d')
        return subprocess.Popen(args)

    log_file = os.path.join(output_path, DEFAULT_BATCH_LOG_FILE_NAME)
    logs.start_file_logging(log_file, verbosity)
    logger = logging.getLogger("batch-experiment-executor")

    local_pid_file = os.path.join(output_path, DEFAULT_BATCH_LOCAL_PID_FILE_NAME)
    cloudexp.util.process.write_pid_to_files(DEFAULT_BATCH_GLOBAL_PID_FILE_PATH, local_pid_file, logger=logger)

    all_exp_path, all_exp_sub_path, status = find_all_sub_experiments(output_path, only_pending=not overwrite)

    logger.info("Found %s pending experiment(s). Will run in the following order:\n%s",
                len(all_exp_path), "\n".join(f'{i+1:>3}: {p}' for i, p in enumerate(all_exp_sub_path)))

    for exp_path, exp_sub_path in zip(all_exp_path, all_exp_sub_path):
        logger.info("Launching experiment: %s", exp_sub_path)
        process_obj = None
        try:
            process_obj = cloudexp.exp.single.launch_experiment(exp_path, verbosity, overwrite, process=True)
            process_obj.communicate()
        except FileExistsError as e:
            logging.warning("Experiment already executed: %s", e)
        except cloudexp.exp.ExperimentAlreadyRunningError as e:
            logging.error(str(e))
            break
        except KeyboardInterrupt:
            logging.info("Keyboard interrupt. Propagating signal to experiment and waiting.")
            if process_obj:
                process_obj.send_signal(signal.SIGINT)
                process_obj.communicate()
            break
        except Exception as e:
            logger.exception("Exception while launching experiment: %s", e)
            if process_obj:
                process_obj.communicate()

        errors, warnings = logs.count_errors_and_warnings(exp_path)
        if errors == 0 and warnings == 0:
            logger.info("Experiment '%s' finished.", exp_sub_path)
        elif errors == 0:
            logger.warning("Experiment '%s' finished with %s warning(s).", exp_sub_path, warnings)
        elif warnings == 0:
            logger.error("Experiment '%s' finished with %s error(s).", exp_sub_path, errors)
        else:
            logger.error("Experiment '%s' finished with %s error(s) and %s warning(s).", exp_sub_path, errors, warnings)

    logger.info("Finished batch experiments")
    logs.stop_file_logging(log_file)


def terminate_batch_experiment(output_path, sig='int'):
    pid_file = os.path.join(output_path, DEFAULT_BATCH_LOCAL_PID_FILE_NAME)
    return cloudexp.util.process.terminate_from_pid_file(pid_file, sig)


def batch_progress(output_path, ignore_backup=True, only_pending=False, monitor_address=None):
    is_running = is_batch_experiment_running(output_path)
    if is_running:
        print("Batch experiment is running")

    all_exp_path, all_exp_sub_path, status = find_all_sub_experiments(output_path, ignore_backup, only_pending)
    args = [load_experiment_args(p) for p in all_exp_path]
    duration = [d['duration'] for d in args]

    stats = [logs.get_log_stats(p) for p in all_exp_path]
    import pandas as pd
    start = [s['start-time'] if s else pd.NaT for s in stats]
    end = [s['end-time'] if s else pd.NaT for s in stats]
    runtime = [e - s if not pd.isnull(s) else pd.NaT for s, e in zip(start, end)]
    warnings = [s['warnings'] if s else 0 for s in stats]
    errors = [s['errors'] if s else 0 for s in stats]
    log_len = [s['length'] if s else 0 for s in stats]

    pending_remaining = sum(d if s == 'pending' else 0 for s, d in zip(status, duration))
    running_remaining = sum(
        d - r.total_seconds() if s == 'running' else 0 for s, d, r in zip(status, duration, runtime))
    total_remaining = pending_remaining + running_remaining
    print("Total remaining time:", time_delta(total_remaining),
          "- Finish time:", pd.to_datetime('today') + pd.to_timedelta(total_remaining, unit='s'))

    df = pd.DataFrame(map(list, zip(*[all_exp_sub_path, status, duration, runtime, start, end, log_len,
                                      warnings, errors])),
                      columns=['name', 'status', 'duration', 'runtime', 'start', 'end', 'log-length',
                               'warnings', 'errors'])
    df['status'] = pd.Categorical(df['status'], ["finished", "running", "pending"])
    df.sort_values(by=['status', 'start', 'name'], ascending=[True, False, True], inplace=True)
    df.set_index('name')
    df.reset_index(drop=True, inplace=True)
    fmt = {
        "runtime": lambda x: '' if pd.isnull(x) else time_delta(x.seconds),
        "duration": lambda x: time_delta(x),
        "start": lambda x: '' if pd.isnull(x) else x.strftime("%H:%M (%Y-%m-%d)"),
        "end": lambda x: '' if pd.isnull(x) else x.strftime("%H:%M (%Y-%m-%d)"),
    }

    if monitor_address:
        fmt['name'] = lambda x: f'<a href="{monitor_address}#{os.path.abspath(os.path.join(output_path, x))}">{x}</a>'

    status_style = {
        'running': 'color: black; background-color: white;',
        'finished': 'color: white; background-color: green;',
        'pending': '',
    }

    def highlight(s):
        ret = ['' for _ in s]

        ret[1] = status_style[s['status']]

        if s['warnings'] > 0:
            ret[7] = 'color: black; background-color: yellow;'
            if s['status'] == 'finished':
                ret[1] = ret[7]

        if s['errors'] > 0:
            ret[8] = 'color: black; background-color: red;'
            if s['status'] == 'finished':
                ret[1] = ret[8]

        return ret

    return df.style.format(fmt).apply(highlight, axis=1)

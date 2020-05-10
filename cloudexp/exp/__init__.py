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
import time
import shutil
import pickle
import datetime
import numpy as np

import cloudexp
import cloudexp.util.process


class ExperimentAlreadyRunningError(Exception):
    pass


DEFAULT_BACKUP_FOLDER = "backup"
DEFAULT_LOG_FILE_NAME = "experiment.log"
DEFAULT_DATA_FILE_NAME = "experiment.data"
DEFAULT_ARGS_FILE_NAME = "experiment.args"
DEFAULT_LOCAL_PID_FILE_NAME = "experiment.pid"
DEFAULT_GLOBAL_PID_FILE_PATH = f"/tmp/{cloudexp.__name__}-experiment.pid"

DEFAULT_BATCH_LOG_FILE_NAME = "batch-experiment.log"
DEFAULT_BATCH_LOCAL_PID_FILE_NAME = "batch-experiment.pid"
DEFAULT_BATCH_GLOBAL_PID_FILE_PATH = f"/tmp/{cloudexp.__name__}-batch-experiment.pid"


def backup_experiment(output_path, new_args=True):
    if not os.path.exists(output_path):
        os.makedirs(output_path)
        return

    if os.path.isfile(output_path):
        raise ValueError("Cannot write experiment over exiting file.")

    path_files = set(os.listdir(output_path))
    require_backup = path_files - {DEFAULT_BACKUP_FOLDER, DEFAULT_ARGS_FILE_NAME}
    if not new_args and len(require_backup) == 0:
        return

    # Fallback
    created = os.path.getmtime(output_path)

    if DEFAULT_LOG_FILE_NAME in path_files:
        created = os.path.getmtime(os.path.join(output_path, DEFAULT_LOG_FILE_NAME))
    elif new_args:
        try:
            exp_args = load_experiment_args(output_path)
            created = exp_args['extra_info']['timestamp']
        except:
            pass

    created = datetime.datetime.fromtimestamp(created)
    created = created.strftime("%Y-%m-%d--%H-%M-%S")

    backup_path = os.path.join(output_path, DEFAULT_BACKUP_FOLDER)
    backup_target = os.path.join(backup_path, created)

    os.makedirs(backup_target, exist_ok=True)

    for f in path_files:
        if f == DEFAULT_BACKUP_FOLDER:
            continue
        src = os.path.join(output_path, f)
        dst = os.path.join(backup_target, f)
        if f == DEFAULT_ARGS_FILE_NAME and not new_args:
            shutil.copyfile(src, dst)
        else:
            os.rename(src, dst)


def restore_latest_experiment(output_path):
    backup_path = os.path.join(output_path, DEFAULT_BACKUP_FOLDER)
    if not os.path.exists(backup_path):
        return

    backups = [os.path.join(backup_path, p) for p in os.listdir(backup_path)]
    backups = list(filter(os.path.isdir, backups))
    if len(backups) == 0:
        return

    created = [os.path.getmtime(p) for p in backups]
    last_backup = backups[np.argmax(created)]

    for f in os.listdir(output_path):
        if f == DEFAULT_BACKUP_FOLDER:
            continue
        f = os.path.join(output_path, f)
        if os.path.isfile(f):
            os.remove(f)
        else:
            shutil.rmtree(f)

    for f in os.listdir(last_backup):
        shutil.move(os.path.join(last_backup, f), output_path)

    shutil.rmtree(last_backup)
    if len(os.listdir(backup_path)) == 0:
        shutil.rmtree(backup_path)


def save_experiment_args(output_path, **kwargs):
    backup_experiment(output_path)
    kwargs.setdefault('extra_info', {})['timestamp'] = time.time()
    file_path = os.path.join(output_path, DEFAULT_ARGS_FILE_NAME)
    with open(file_path, 'wb') as f:
        pickle.dump(kwargs, f)


def load_experiment_args(output_path):
    args_file_path = os.path.join(output_path, DEFAULT_ARGS_FILE_NAME)
    with open(args_file_path, 'rb') as f:
        return pickle.load(f)


def get_running_experiment(output_path):
    local_pid_file = os.path.join(output_path, DEFAULT_LOCAL_PID_FILE_NAME)
    return cloudexp.util.process.find_active_form_pid_files(DEFAULT_GLOBAL_PID_FILE_PATH, local_pid_file)


def validate_no_experiments(output_path):
    running_experiments = get_running_experiment(output_path)
    if running_experiments:
        raise ExperimentAlreadyRunningError(f"Experiment is running: {running_experiments}.")


def is_experiment_running(output_path):
    local_pid_file = os.path.join(output_path, DEFAULT_LOCAL_PID_FILE_NAME)
    pid, alive = cloudexp.util.process.read_pid_file(local_pid_file)
    return pid is not None and alive


def experiment_status(output_path):
    local_pid_file = os.path.join(output_path, DEFAULT_LOCAL_PID_FILE_NAME)
    pid, alive = cloudexp.util.process.read_pid_file(local_pid_file)
    if pid is None:
        return 'pending'
    elif alive:
        return 'running'
    else:
        return 'finished'


def find_all_sub_experiments(output_path, ignore_backup=True, only_pending=False, only_finished=False, kind=None):
    all_exp_path = set()

    for root, dirs, files in os.walk(output_path):
        if ignore_backup and any(root.startswith(p) for p in all_exp_path):
            # Ignore backup paths
            continue
        if DEFAULT_ARGS_FILE_NAME in files:
            all_exp_path.add(root)

    all_exp_path = sorted(all_exp_path)
    sub_path_init = len(output_path)
    all_exp_sub_path = [f".{exp_path[sub_path_init:]}" for exp_path in all_exp_path]
    status = [experiment_status(p) for p in all_exp_path]

    if only_pending:
        kind = {'pending'}
    elif only_finished:
        kind = {'finished'}
    elif isinstance(kind, str):
        kind = {kind}
    elif kind is None:
        kind = {'pending', 'running', 'finished'}

    ret = zip(*[(p, sp, s) for p, sp, s in zip(all_exp_path, all_exp_sub_path, status) if s in kind])
    all_exp_path, all_exp_sub_path, status = ret
    return all_exp_path, all_exp_sub_path, status

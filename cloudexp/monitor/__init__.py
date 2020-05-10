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
import json
import codecs
import pickle
import time
import traceback
import datetime

from flask import Flask, request, Response

import cloudexp.exp.single
from cloudexp import exp
from cloudexp.exp import batch
from cloudexp.util import logs
from cloudexp.results.data import ExpData


config = dict(
    # Flask configuration
    port=5050,
    debug=True,
    hostname='0.0.0.0',

    # Monitor configuration
    workspace_folder='/',

    # The SECRET_KEY is required, without it you can't have sessions.
    SECRET_KEY='1869abdf7a054d02b35bbbcf9d8b99d0fd74f42c41666d15',
)

app = Flask(__name__)
app.config.from_object(__name__)
app.config.update(config)  # apply config file settings

# Init logger for the entire system
logs.start_stdio_logging('debug')

app.logger.handlers = []
app.logger.propagate = True

cur_path, _ = os.path.split(__name__)


###################################################################################################
# Helper call to run the application
###################################################################################################

def run_monitor(workspace_folder=None):
    if workspace_folder is not None:
        workspace_folder = os.path.expanduser(workspace_folder)
        workspace_folder = os.path.expandvars(workspace_folder)
        workspace_folder = os.path.abspath(workspace_folder)
        workspace_folder = os.path.realpath(workspace_folder)
        app.config['workspace_folder'] = workspace_folder
        app.logger.info("Workspace folder: %s", workspace_folder)
    app.run(host=app.config['hostname'], port=app.config['port'], threaded=True)


###################################################################################################
# Helpers
###################################################################################################


def is_running_any_experiment(output_path: str):
    return exp.is_experiment_running(output_path) or batch.is_batch_experiment_running(output_path)


def get_full_path(requested_path: str):
    workspace_folder = app.config['workspace_folder']
    full_path = os.path.join(app.config['workspace_folder'], requested_path)
    full_path = os.path.abspath(full_path)

    if os.path.commonpath([full_path, workspace_folder]) != workspace_folder:
        raise Exception("Attempt to access files outside of workspace folder.")

    requested_path = full_path[len(workspace_folder):]
    if requested_path and requested_path[0] == os.path.sep:
        requested_path = requested_path[1:]

    if not os.path.isdir(full_path):
        raise NotADirectoryError(f"Requested path '{requested_path}' is not a folder.")
    return full_path, requested_path


###################################################################################################
# Error handler
###################################################################################################

@app.errorhandler(Exception)
def handle_invalid_usage(error):
    """ Handles exception of all the handlers """
    app.logger.exception(error)
    message = str(error)
    return Response(json.dumps({
        "message": message.strip(),
        "traceback": traceback.format_exc().strip(),
    }), 500, mimetype='application/json')


###################################################################################################
# Route handlers
###################################################################################################

@app.route('/', methods=['GET'])
def main_page():
    return app.send_static_file('index.html')


@app.route('/<path:path>')
def static_file(path):
    return app.send_static_file(path)


@app.route('/get_relative_path', methods=['POST'])
def get_relative_path():
    data = request.get_json(force=True)
    requested_path = data.get('path', '')

    full_path, requested_path = get_full_path(requested_path)
    app.logger.debug("Requested relative path: %s", requested_path)

    return Response(json.dumps({
        'url': requested_path,
    }), mimetype='application/json')


@app.route('/tree')
def get_tree():
    workspace = app.config['workspace_folder']
    workspace_init = len(workspace) + 1
    tree = (root for root, dirs, files in os.walk(workspace))
    tree = [d[workspace_init:] for d in filter(lambda d: os.path.isdir(d) and d != workspace, tree)]
    return Response(json.dumps({
        'tree': tree,
    }), mimetype='application/json')


@app.route('/getlogs', methods=['POST'])
def get_logs():
    data = request.get_json(force=True)
    requested_path = data.get('path', '')
    logs_data = data.get('logs_data', None)
    timeout = data.get('timeout', 10)
    wait_for_start = data.get('wait_for_start', False)

    full_path, requested_path = get_full_path(requested_path)
    app.logger.debug("Requested logs from path: %s", requested_path)

    if logs_data is not None:
        logs_data = pickle.loads(codecs.decode(logs_data.encode(), "base64"))
    else:
        logs_data = {}

    is_running = is_running_any_experiment(full_path)

    if not is_running and len(logs_data) == 0:
        try:
            data = ExpData(full_path)
            base_time = data.get_attributes('base-time')[0]
            logs_data['init-time'] = datetime.datetime.fromtimestamp(base_time)
        except Exception as e:
            app.logger.debug("Could not get init time from data logger: %s", e)

    joint_log = []
    end_time = time.time() + max(timeout, 1)
    while not joint_log:
        joint_log = logs.get_joint_raw_logs(full_path, logs_data, use_time_delta=True)
        is_running = is_running_any_experiment(full_path)
        if (is_running or wait_for_start) and not joint_log and time.time() < end_time:
            time.sleep(0.1)
        else:
            break

    logs_data = codecs.encode(pickle.dumps(logs_data), "base64").decode()
    return Response(json.dumps({
        'joint_log': joint_log,
        'headers': logs.LOG_HEADERS,
        'logs_data': logs_data,
        'is_running': is_running,
        'url': requested_path,
    }), mimetype='application/json')


@app.route('/launch', methods=['POST'])
def launch():
    data = request.get_json(force=True)
    requested_path = data.get('path')
    verbosity = data.get('verbosity', 'debug')
    overwrite = data.get('overwrite', False)
    is_batch = data.get('batch', False)

    full_path, requested_path = get_full_path(requested_path)
    app.logger.debug("Requested to launch experiment from path: %s", requested_path)

    if not is_batch:
        exp_proc = cloudexp.exp.single.launch_experiment(full_path, overwrite=overwrite, verbosity=verbosity, daemon=True)
    else:
        exp_proc = batch.launch_batch_experiment(full_path, overwrite=overwrite, verbosity=verbosity, daemon=True)

    # Wait for daemon init process to exit
    exp_proc.communicate(timeout=10)

    # Wait for experiment to start
    end_time = time.time() + 10
    while not is_running_any_experiment(full_path) and time.time() < end_time:
        time.sleep(0.1)

    return Response(json.dumps({
        'url': requested_path,
        'pid': exp_proc.pid,
    }), mimetype='application/json')


@app.route('/terminate', methods=['POST'])
def terminate():
    data = request.get_json(force=True)
    requested_path = data.get('path')
    sig = data.get('signal', 'int')

    full_path, requested_path = get_full_path(requested_path)
    app.logger.debug("Requested terminate experiment from path: %s", requested_path)

    ret_exp = cloudexp.exp.single.terminate_experiment(full_path, sig=sig)
    ret_batch = batch.terminate_batch_experiment(full_path, sig=sig)
    if ret_exp and ret_batch:
        app.logger.error('Failed to terminate experiment: %s, %s', ret_exp, ret_batch)

    return Response(json.dumps({
        'url': requested_path,
    }), mimetype='application/json')

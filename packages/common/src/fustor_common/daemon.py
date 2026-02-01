"""
Common daemon functionality for Fustor services.
This module provides a generic daemon launcher that can be used by
any Fustor service without knowing implementation details.
"""
import os
import sys
import subprocess
from fustor_common.paths import get_fustor_home_dir


def start_daemon(service_module_path, app_var_name, pid_file_name, log_file_name, display_name, port, host='127.0.0.1', verbose=False, reload=False):
    """
    Start a Fustor service as a daemon process.

    Args:
        service_module_path (str): The Python module path to import (e.g., 'fustor_fusion.main')
        app_var_name (str): The name of the application variable in the module (e.g., 'app')
        pid_file_name (str): The name of the PID file (e.g., 'fusion.pid')
        log_file_name (str): The name of the log file (e.g., 'fusion.log')
        display_name (str): The display name for the service (e.g., 'Fustor Fusion')
        port (int): The port to run the service on
        verbose (bool): Whether to enable verbose logging
    """
    # Use the generic daemon launcher script
    daemon_script_path = os.path.join(os.path.dirname(__file__), 'daemon_launcher.py')

    # Create the command to execute the generic daemon launcher
    command = [
        sys.executable,
        daemon_script_path,
        service_module_path,
        app_var_name,
        pid_file_name,
        log_file_name,
        display_name,
        str(port),
        host
    ]

    if verbose:
        command.append('--verbose')

    if reload:
        command.append('--reload')

    # Set up the environment to ensure the subprocess has correct paths
    env = os.environ.copy()
    # Ensure PYTHONPATH includes the current directory for development
    env['PYTHONPATH'] = os.getcwd() + ':' + env.get('PYTHONPATH', '')

    subprocess.Popen(command, stdout=None, stderr=None, stdin=None, close_fds=True, env=env)
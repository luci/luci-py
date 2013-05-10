#!/usr/bin/python2.7
#
# Copyright 2012 Google Inc. All Rights Reserved.

"""Various swarm constants required by the server and the swarm slaves.

This allows the swarm slaves to have this files and the needed variables without
having to download the whole swarm directory.
"""



import os


# The exit code to return when the machine should restart.
RESTART_EXIT_CODE = 99

# Name of python script containing constants.
SWARM_CONSTANTS_SCRIPT = 'swarm_constants.py'

# Name of python script for swarm slaves.
SLAVE_MACHINE_SCRIPT = 'slave_machine.py'

# Name of python script to execute on the remote machine to run a test.
TEST_RUNNER_SCRIPT = 'local_test_runner.py'

# Name of python script to validate swarm file format.
TEST_REQUEST_MESSAGE_SCRIPT = 'test_request_message.py'

# Name of python script to handle url connections.
URL_HELPER_SCRIPT = 'url_helper.py'

# Name of python script to generate slave code version.
SWARM_VERSION_SCRIPT = 'version.py'

# Name of python script to mark folder as package.
PYTHON_INIT_SCRIPT = '__init__.py'

# Name of directories in source tree and/or on remote machine.
TEST_RUNNER_DIR = 'swarm_bot'
COMMON_DIR = 'common'

# Root directory of Swarm scripts.
SWARM_ROOT_DIR = os.path.join(os.path.dirname(__file__), '..')

# The list of swarm common files needed by the swarm bots.
SWARM_BOT_COMMON_FILES = [
    PYTHON_INIT_SCRIPT,
    SWARM_CONSTANTS_SCRIPT,
    SWARM_VERSION_SCRIPT,
    URL_HELPER_SCRIPT,
]

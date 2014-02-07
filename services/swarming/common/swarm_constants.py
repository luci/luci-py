# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Various swarm constants required by multiple files.

This allows the swarm slaves to have this file and the needed variables without
having to download the whole swarm directory.
"""


import os


# The exit code to return when the machine should restart.
RESTART_EXIT_CODE = 99

# The key and file name to use when uploading results from the slaves.
RESULT_STRING_KEY = 'result_output'

# The key to use to access the start slave script file model.
START_SLAVE_SCRIPT_KEY = 'start_slave_script'

# The maximum size a chunk should be when creating chunk models. Although App
# Engine allows bigger, this gives some wiggle room in case something needs to
# be added to a chunk model.
MAX_CHUNK_SIZE = 768 * 1024

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
    TEST_REQUEST_MESSAGE_SCRIPT,
    URL_HELPER_SCRIPT,
]

# Number of days to keep old runners around for.
SWARM_FINISHED_RUNNER_TIME_TO_LIVE_DAYS = 14

# The time (in seconds) to wait after receiving a runner before aborting it.
# This is intended to delete runners that will never run because they will
# never find a matching machine.
SWARM_RUNNER_MAX_WAIT_SECS = 24 * 60 * 60

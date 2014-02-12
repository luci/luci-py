# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Various swarm constants used by both the server and the bot code."""

# The exit code to return when the machine should restart.
# TODO(maruel): This is only needed by swarm_bot/ so move it there.
RESTART_EXIT_CODE = 99

# The key and file name to use when uploading results from the slaves.
RESULT_STRING_KEY = 'result_output'

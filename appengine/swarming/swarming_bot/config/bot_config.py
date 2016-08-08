# coding: utf-8
# Copyright 2013 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""This file is meant to be overriden by the server's specific copy.

You can upload a new version via /restricted/upload/bot_config.

There's 3 types of functions in this file:
  - get_*() to return properties to describe this bot.
  - on_*() as hooks based on events happening on the bot.
  - setup_*() to setup global state on the host.

This file shouldn't import from other scripts in this directory except
os_utilities which is guaranteed to be usable as an API. It's fine to import
from stdlib.

Set the environment variable SWARMING_LOAD_TEST=1 to disable the use of
server-provided bot_config.py. This permits safe load testing.

This file contains unicode to confirm UTF-8 encoded file is well supported.
Here's a pile of poo: 💩
"""

import os

from api import os_utilities

# Unused argument 'bot' - pylint: disable=W0613


def get_dimensions(bot):
  # pylint: disable=line-too-long
  """Returns dict with the bot's dimensions.

  The dimensions are what are used to select the bot that can run each task.

  The bot id will be automatically selected based on the hostname with
  os_utilities.get_dimensions(). If you want something more special, specify it
  in your bot_config.py and override the item 'id'.

  See https://github.com/luci/luci-py/tree/master/appengine/swarming/doc/Magic-Values.md.

  Arguments:
  - bot: bot.Bot instance or None. See ../api/bot.py.
  """
  return os_utilities.get_dimensions()


def get_state(bot):
  # pylint: disable=line-too-long
  """Returns dict with a state of the bot reported to the server with each poll.

  It is only for dynamic state that changes while bot is running for information
  for the sysadmins.

  The server can not use this state for immediate scheduling purposes (use
  'dimensions' for that), but it can use it for maintenance and bookkeeping
  tasks.

  See https://github.com/luci/luci-py/tree/master/appengine/swarming/doc/Magic-Values.md.

  Arguments:
  - bot: bot.Bot instance or None. See ../api/bot.py.
  """
  return os_utilities.get_state()


def get_authentication_headers(bot):
  """Returns authentication headers and their expiration time.

  The returned headers will be passed with each HTTP request to the Swarming
  server (and only Swarming server). The bot will use the returned headers until
  they are close to expiration (usually 6 min, see AUTH_HEADERS_EXPIRATION_SEC
  in remote_client.py), and then it'll attempt to refresh them by calling
  get_authentication_headers again.

  Can be used to implement per-bot authentication. If no headers are returned,
  the server will use only IP whitelist for bot authentication.

  May be called by different threads, but never concurrently.

  Arguments:
  - bot: bot.Bot instance. See ../api/bot.py.

  Returns:
    Tuple (dict with headers or None, unix timestamp of when they expire).
  """
  return (None, None)


### Hooks


def on_bot_shutdown(bot):
  """Hook function called when the bot shuts down, usually rebooting.

  It's a good time to do other kinds of cleanup.

  Arguments:
  - bot: bot.Bot instance. See ../api/bot.py.
  """
  pass


def on_bot_startup(bot):
  """Hook function called when the bot starts.

  It's a good time to do some cleanup before starting to poll for tasks.

  Arguments:
  - bot: bot.Bot instance. See ../api/bot.py.
  """
  pass


def on_before_task(bot, bot_file=None):
  """Hook function called before running a task.

  It shouldn't do much, since it can't cancel the task so it shouldn't do
  anything too fancy.

  Arguments:
  - bot: bot.Bot instance. See ../api/bot.py.
  - bot_file: Path to file to write information about the state of the bot.
              This file can be used to pass certain info about the bot
              to tasks, such as which connected android devices to run on. See
              https://github.com/luci/luci-py/tree/master/appengine/swarming/doc/Magic-Values.md#run_isolated
              TODO(bpastene): Remove default value None.
  """
  pass


def on_after_task(bot, failure, internal_failure, dimensions, summary):
  """Hook function called after running a task.

  It is an excellent place to do post-task cleanup of temporary files.

  The default implementation restarts after a task failure or an internal
  failure.

  Arguments:
  - bot: bot.Bot instance. See ../api/bot.py.
  - failure: bool, True if the task failed.
  - internal_failure: bool, True if an internal failure happened.
  - dimensions: dict, Dimensions requested as part of the task.
  - summary: dict, Summary of the task execution.
  """
  # Example code:
  #if failure:
  #  bot.restart('Task failure')
  #elif internal_failure:
  #  bot.restart('Internal failure')


def on_bot_idle(bot, since_last_action):
  """Hook function called once when the bot has been idle; when it has no
  command to execute.

  This is an excellent place to put device in 'cool down' mode or any
  "pre-warming" kind of stuff that could take several seconds to do, that would
  not be appropriate to do in on_after_task(). It could be worth waiting for
  `since_last_action` to be several seconds before doing a more lengthy
  operation.

  This function is called repeatedly until an action is taken (a task, updating,
  etc).

  This is a good place to do "auto reboot" for hardware based bots that are
  rebooted periodically.

  Arguments:
  - bot: bot.Bot instance. See ../api/bot.py.
  - since_last_action: time in second since last action; e.g. amount of time the
                       bot has been idle.
  """
  pass


### Setup


def setup_bot(bot):
  """Does one time initialization for this bot.

  Returns True if it's fine to start the bot right away. Otherwise, the calling
  script should exit.

  This is an excellent place to drop a README file in the bot directory, to give
  more information about the purpose of this bot.

  Example: making this script starts automatically on user login via
  os_utilities.set_auto_startup_win() or os_utilities.set_auto_startup_osx().
  """
  with open(os.path.join(bot.base_dir, 'README'), 'wb') as f:
    f.write(
"""This directory contains a Swarming bot.

Swarming source code is hosted at https://github.com/luci/luci-py.

The bot was generated from the server %s. To get the bot's attributes, run:

  python swarming_bot.zip attributes
""" % bot.server)
    return True

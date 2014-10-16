# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""This file is meant to be overriden by the server's specific copy.

It implements:
- get_attributes() to return the bot's attributes. It's useful to create
  particular dimensions.
- on_after_task() is a hook called after each task.
- setup_bot() to be called on startup to ensure the bot is in a known state.
  It's the perfect time to setup automatic startup if needed.

The (new) functions are provided a bot.Bot instance.
"""

import os_utilities


def get_attributes():
  """Returns the attributes for this bot."""
  # The bot id will be automatically selected based on the hostname. If you want
  # something more special, specify it in your bot_config.py. You can upload a
  # new version via /restricted/upload/bot_config.
  return os_utilities.get_attributes(None)


def setup_bot():
  """Does one time initialization for this bot.

  Returns True if it's fine to start the bot right away. Otherwise, the calling
  script should exit.

  Example: making this script starts automatically on user login via
  os_utilities.set_auto_startup_win() or os_utilities.set_auto_startup_osx().
  """
  return True


def on_after_task(bot, failure, internal_failure):
  """Hook function to be called after running a task.

  It is an excellent place to do post-task cleanup of temporary files.

  The default implementation restarts after a task failure or an internal
  failure.

  Arguments:
  - bot: bot.Bot instance.
  - failure: bool, True if the task failed.
  - internal_failure: bool, True if an internal failure happened.
  """
  if failure:
    bot.restart('Task failure')
  elif internal_failure:
    bot.restart('Internal failure')

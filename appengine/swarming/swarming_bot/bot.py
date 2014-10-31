# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Bot interface used in bot_config.py."""

import logging
import os

import os_utilities
from utils import zip_package

THIS_FILE = os.path.abspath(zip_package.get_main_script_path())

# Method could be a function - pylint: disable=R0201


class Bot(object):
  def __init__(self, remote, attributes, server_version, base_dir):
    # Do not expose attributes nor remote for now, as attributes will be
    # refactored soon and remote would have a lot of side effects if used by
    # bot_config.
    self._attributes = attributes
    self._remote = remote
    self._server_version = server_version
    self._base_dir = base_dir

  @property
  def base_dir(self):
    """Returns the working directory.

    It is normally the current workind directory, e.g. os.getcwd() but it is
    preferable to not assume that.
    """
    return self._base_dir

  @property
  def dimensions(self):
    """The bot's current dimensions.

    Dimensions are relatively static and not expected to change much. They
    should change only when it effectively affects the bot's capacity to execute
    tasks.
    """
    return self._attributes.get('dimensions', {}).copy()

  @property
  def id(self):
    """Returns the bot's ID."""
    return self._attributes.get('id', 'unknown')

  @property
  def remote(self):
    """XsrfClient instance to talk to the server.

    Should not be normally used by bot_config.py for now.
    """
    return self._remote

  @property
  def server_version(self):
    """Version of the server's implementation.

    The form is nnn-hhhhhhh for pristine version and nnn-hhhhhhh-tainted-uuuu
    for non-upstreamed code base:
      nnn: revision pseudo number
      hhhhhhh: git commit hash
      uuuu: username
    """
    return self._server_version

  @property
  def swarming_bot_zip(self):
    """Absolute path to the swarming_bot.zip file."""
    return THIS_FILE

  def post_error(self, error):
    """Posts given string as a failure.

    This is used in case of internal code error.
    """
    logging.error('Error: %s\n%s', self._attributes, error)
    data = {
      'id': self._attributes['id'],
      'message': error,
    }
    return self._remote.url_read_json('/swarming/api/v1/bot/error', data=data)

  def restart(self, message):
    """Reboots the machine.

    If the reboot is successful, never returns: the process should just be
    killed by OS.

    If reboot fails, logs the error to the server and moves the bot to
    quarantined mode.
    """
    # TODO(maruel): Notify the server that the bot is rebooting.
    # os_utilities.restart should never return, unless restart is not happening.
    # If restart is taking longer than N minutes, it probably not going to
    # finish at all. Report this to the server.
    os_utilities.restart(message, timeout=15*60)
    self.post_error('Bot is stuck restarting for: %s' % message)

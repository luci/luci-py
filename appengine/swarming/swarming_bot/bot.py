# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Bot interface used in bot_config.py."""

import logging

import os_utilities


class Bot(object):
  def __init__(self, remote, attributes):
    # Do not expose attributes nor remote for now, as attributes will be
    # refactored soon and remote would have a lot of side effects if used by
    # bot_config.
    self._attributes = attributes
    self._remote = remote

  @property
  def dimensions(self):
    return self._attributes.get('dimensions', {}).copy()

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

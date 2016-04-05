# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Bot interface used in bot_config.py."""

import logging
import os
import threading
import time

import os_utilities
from utils import zip_package

THIS_FILE = os.path.abspath(zip_package.get_main_script_path())

# Method could be a function - pylint: disable=R0201


class Bot(object):
  def __init__(
      self, remote, attributes, server, server_version, base_dir,
      shutdown_hook):
    # Do not expose attributes nor remote for now, as attributes will be
    # refactored soon and remote would have a lot of side effects if used by
    # bot_config.
    self._attributes = attributes
    self._base_dir = base_dir
    self._remote = remote
    self._server = server
    self._server_version = server_version
    self._shutdown_hook = shutdown_hook
    self._timers = []
    self._timers_dying = False
    self._timers_lock = threading.Lock()

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
    return self.dimensions.get('id', ['unknown'])[0]

  @property
  def remote(self):
    """XsrfClient instance to talk to the server.

    Should not be normally used by bot_config.py for now.
    """
    return self._remote

  @property
  def server(self):
    """URL of the swarming server this bot is connected to.

    It includes the https:// prefix but without trailing /, so it looks like
    "https://foo-bar.appspot.com".
    """
    return self._server

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
  def state(self):
    return self._attributes['state']

  @property
  def swarming_bot_zip(self):
    """Absolute path to the swarming_bot.zip file.

    The bot itself is run as swarming_bot.1.zip or swarming_bot.2.zip. Always
    return swarming_bot.zip since this is the script that must be used when
    starting up.
    """
    return os.path.join(os.path.dirname(THIS_FILE), 'swarming_bot.zip')

  def post_event(self, event_type, message):
    """Posts an event to the server."""
    data = self._attributes.copy()
    data['event'] = event_type
    data['message'] = message
    self._remote.url_read_json('/swarming/api/v1/bot/event', data=data)

  def post_error(self, message):
    """Posts given string as a failure.

    This is used in case of internal code error. It traps exception.
    """
    logging.error('Error: %s\n%s', self._attributes, message)
    try:
      self.post_event('bot_error', message)
    except Exception:
      logging.exception('post_error(%s) failed.', message)

  def restart(self, message):
    """Reboots the machine.

    If the reboot is successful, never returns: the process should just be
    killed by OS.

    If reboot fails, logs the error to the server and moves the bot to
    quarantined mode.
    """
    self.post_event('bot_rebooting', message)
    self.cancel_all_timers()
    if self._shutdown_hook:
      try:
        self._shutdown_hook(self)
      except Exception as e:
        logging.exception('shutdown hook failed: %s', e)
    # os_utilities.restart should never return, unless restart is not happening.
    # If restart is taking longer than N minutes, it probably not going to
    # finish at all. Report this to the server.
    try:
      os_utilities.restart(message, timeout=15*60)
    except LookupError:
      # This is a special case where OSX is deeply hosed. In that case the disk
      # is likely in read-only mode and there isn't much that can be done. This
      # exception is deep inside pickle.py. So notify the server then hang in
      # there.
      self.post_error('This host partition is bad; please fix the host')
      while True:
        time.sleep(1)
    self.post_error('Bot is stuck restarting for: %s' % message)

  def call_later(self, delay_sec, callback):
    """Schedules a function to be called later (if bot is still running).

    All calls are executed in a separate internal thread, be careful with what
    you call from there (Bot object is generally not thread safe).

    Multiple callbacks can be executed concurrently. It is safe to call
    'call_later' from the callback.
    """
    timer = None

    def call_wrapper():
      with self._timers_lock:
        # Canceled already?
        if timer not in self._timers:
          return
        self._timers.remove(timer)
      try:
        callback()
      except Exception:
        logging.exception('Timer callback failed')

    with self._timers_lock:
      if not self._timers_dying:
        timer = threading.Timer(delay_sec, call_wrapper)
        self._timers.append(timer)
        timer.daemon = True
        timer.start()

  def cancel_all_timers(self):
    """Cancels all pending 'call_later' calls and forbids adding new ones."""
    timers = None
    with self._timers_lock:
      self._timers_dying = True
      for t in self._timers:
        t.cancel()
      timers, self._timers = self._timers, []
    for t in timers:
      t.join(timeout=5)
      if t.isAlive():
        logging.error('Timer thread did not terminate fast enough: %s', t)

  def update_dimensions(self, new_dimensions):
    """Called internally to update Bot.dimensions."""
    self._attributes['dimensions'] = new_dimensions

  def update_state(self, new_state):
    """Called internally to update Bot.state."""
    self._attributes['state'] = new_state

# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Bot interface used in bot own code and in bot_config.py."""

import contextlib
import copy
import hashlib
import inspect
import logging
import os
import os
import struct
import sys
import threading
import time
import uuid

from api import os_utilities


class Bot(object):

  def __init__(self, remote, attributes, server, base_dir):
    assert server is None or not server.endswith('/'), server

    # TODO(vadimsh): Make bot ID immutable. Changing it after the handshake is
    # undefined behavior on Swarming and an error on RBE.

    # Immutable.
    self._base_dir = base_dir
    self._remote = remote
    self._server = server
    self._session_id = _gen_session_id()

    # Mutable, see BotMutator.
    self._lock = threading.Lock()
    self._lifecycle_callbacks = []
    self._idle = False
    self._dimensions = (attributes or {}).get('dimensions') or {}
    self._state = (attributes or {}).get('state') or {}
    self._bot_version = (attributes or {}).get('version') or 'unknown'
    self._server_side_dimensions = {}
    self._bot_restart_msg = None
    self._bot_config = {}
    self._rbe_instance = None
    self._rbe_worker_properties = None
    self._rbe_hybrid_mode = False
    self._rbe_session = None

    # Mutable in response to other Bot calls.
    self._shutdown_event_posted = False

    # Populate parts of self._dimensions and self._state that depend on other
    # fields with default values.
    with self.mutate_internals() as mut:
      mut._refresh_attributes()

  @property
  def base_dir(self):
    """The working directory.

    It is normally the current working directory, e.g. os.getcwd() but it is
    preferable to not assume that.
    """
    return self._base_dir

  @property
  def config_dir(self):
    """The directory used for configuration files on the machine."""
    if sys.platform == 'win32':
      return 'C:\\swarming_config'
    if sys.platform == 'cygwin':
      return '/cygdrive/c/swarming_config'
    return '/etc/swarming_config'

  @property
  def dimensions(self):
    """A copy of bot's current dimensions dict.

    Dimensions are relatively static and not expected to change much. They
    should change only when it effectively affects the bot's capacity to execute
    tasks.

    Includes both bot supplied dimensions (as returned by get_dimensions
    bot_config.py hook) and server defined ones (as obtained during handshake
    with the server).

    Server defined dimensions are specified in bots.cfg configuration file on
    the server side. They completely override corresponding bot supplied
    dimensions.

    For example, if bot_config.get_dimensions() returns "pool:Foo"
    and bots.cfg defines "pool:Bar", then the bot will have "pool:Bar"
    dimension. It will NOT be a joined "pool:[Foo,Bar]" dimension.

    That way server can be sure that 'pool' dimension used for the bot is what
    it should be, even if the bot is misbehaving or maliciously trying to move
    itself to a different pool. By forcefully overriding dimensions on the
    server side we can use them as security boundaries.
    """
    with self._lock:
      return copy.deepcopy(self._dimensions)

  @property
  def id(self):
    """The bot's ID."""
    with self._lock:
      return self._dimensions.get('id', ['unknown'])[0]

  @property
  def session_id(self):
    """Bot session ID generated when the bot was instantiated."""
    return self._session_id

  @property
  def session_token(self):
    """The current session token. It is refreshed by various RPC calls."""
    return self._remote.session_token

  @property
  def remote(self):
    """RemoteClient to talk to the server.

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
  def bot_version(self):
    """Version of the running swarming_bot.zip file."""
    return self._bot_version

  @property
  def rbe_worker_properties(self):
    """Worker properties to report to RBE, if any."""
    return self._rbe_worker_properties

  @property
  def state(self):
    """A copy of the current bot state dict, as sent to the server.

    It is accessible from the UI and usually contains various helpful info about
    the bot status.

    The state may change often, but it can't be used in scheduling decisions.
    """
    with self._lock:
      return copy.deepcopy(self._state)

  @property
  def attributes(self):
    """A copy of the dict with bot attributes to send to the server."""
    with self._lock:
      return {
          'dimensions': copy.deepcopy(self._dimensions),
          'state': copy.deepcopy(self._state),
          'version': self._bot_version,
      }

  @property
  def swarming_bot_zip(self):
    """Absolute path to the swarming_bot.zip file.

    The bot itself is run as swarming_bot.1.zip or swarming_bot.2.zip. Always
    return swarming_bot.zip since this is the script that must be used when
    starting up.

    This is generally used by bot_config.setup_bot() when setting up the bot to
    automatically start upon boot.
    """
    return os.path.join(self.base_dir, 'swarming_bot.zip')

  @property
  def shutdown_event_posted(self):
    """True if already submitted `bot_shutdown` or `bot_rebooting` events."""
    return self._shutdown_event_posted

  def get_pseudo_rand(self, width):
    """Returns a constant pseudo-random factor for the bot within +/-width.

    This is useful when we want to desynchronise a global operation, like when
    the bot reboot on a period.

    Returns:
      float between [-width, +width] rounded to 4 decimals.
    """
    b = struct.unpack('h',
                      hashlib.md5(bytes(
                          self.id.encode('utf-8'))).digest()[:2])[0]
    return round(b / 32768. * width, 4)

  def post_event(self, event_type, message):
    """Posts an event to the server."""
    self._remote.post_bot_event(event_type, message, self.attributes)
    if event_type in ('bot_shutdown', 'bot_rebooting'):
      self._shutdown_event_posted = True

  def post_error(self, message):
    """Posts given string as a failure.

    This is used in case of internal code error. It traps exception.

    Include a full stack trace, because sometimes the error is not sufficient
    by itself.
    """
    logging.error('post_error(%s)', message)
    stack = '\nCalling stack:\n%s' % _make_stack()
    try:
      self.post_event('bot_error', '%s%s' % (message.rstrip(), stack))
    except Exception:
      logging.exception('post_error(%s) failed.%s', message, stack)

  def host_reboot(self, message):
    """Reboots the machine.

    If the reboot is successful, never returns: the process should just be
    killed by OS.

    If reboot fails, logs the error to the server and moves the bot to
    quarantined mode.
    """
    self.post_event('bot_rebooting', message)

    # Run the callbacks to prepare for the reboot. They will shutdown RBE
    # session and prepare the bot to auto-start after the reboot.
    self.run_lifecycle_callbacks('reboot')

    # os_utilities.host_reboot should never return, unless the reboot is not
    # happening (e.g. sudo shutdown requires a password). If rebooting the host
    # is taking longer than N minutes, it probably not going to finish at all.
    # Report this to the server.
    try:
      os_utilities.host_reboot(message, timeout=15 * 60)
    except LookupError:
      # This is a special case where OSX is deeply hosed. In that case the disk
      # is likely in read-only mode and there isn't much that can be done. This
      # exception is deep inside pickle.py. So notify the server then hang in
      # there.
      self.post_error('This host partition is bad; please fix the host')
      while True:
        time.sleep(1)
    self.post_error('Host is stuck rebooting for: %s' % message)

  # Compatibility code. TODO(maruel): Remove once all call sites are updated.
  restart = host_reboot

  def bot_restart(self, message):
    """Instructs the bot to restart its own process as soon as possible.

    This can be done when the dimensions need to be updated in a way that cannot
    be done within the process lifetime.

    This is done asynchronously.
    """
    assert isinstance(message, str), message
    with self._lock:
      if self._bot_restart_msg:
        self._bot_restart_msg += '\n' + message
      else:
        self._bot_restart_msg = message

  def bot_restart_msg(self):
    """Returns the current reason to restart the bot process, if any."""
    with self._lock:
      return self._bot_restart_msg

  @contextlib.contextmanager
  def mutate_internals(self):
    """Executes a mutation of the internal bot state under a lock.

    Must never be used from hooks, only from Swarming bot code itself.

    Inside the context manager it is generally unsafe to call Bot methods
    directly. Instead use the emitted BotMutator methods (for both reads and
    writes).
    """
    with self._lock:
      yield BotMutator(self)

  def run_lifecycle_callbacks(self, event):
    """Executes registered lifecycle callbacks."""
    assert event in ('reboot', 'exit'), event
    with self._lock:
      cbs = self._lifecycle_callbacks[:]
    for cb in reversed(cbs):
      try:
        cb(self, event)
      except Exception as e:
        logging.exception('%s lifecycle callback failed: %s', event, e)


class BotMutator(object):
  """Exposes methods to mutate the internal state of a bot."""

  def __init__(self, bot):
    self._bot = bot

  def update_session_token(self, session_token):
    """Updates the session token used by the bot."""
    self._bot._remote.session_token = session_token

  def update_bot_group_cfg(self, cfg):
    """Picks up the server-provided per-bot config.

    This is called once, right after the handshake and it may modify values of
    'state' and 'dimensions' (by augmenting them with server-provided details).

    It is done only to make this information available to bot_config.py hooks.
    The server would still enforce the dimensions with each '/poll' call.

    See docs for '/handshake' call for the format of 'cfg' dict.
    """
    self._bot._server_side_dimensions = (cfg or {}).get('dimensions')
    self._refresh_attributes()

  def update_bot_config(self, name, rev):
    """Picks up bot_config script name and revision.

    This is called at start, and after handshake if a custom script is injected.
    """
    self._bot._bot_config = {'name': name, 'revision': rev}
    self._refresh_attributes()

  def update_idleness(self, idle):
    """Changes the idleness state of the bot, returns the previous value."""
    idle = bool(idle)
    if self._bot._idle == idle:
      return idle
    prev, self._bot._idle = self._bot._idle, idle
    self._refresh_attributes()
    return prev

  def update_auto_cleanup(self, auto_cleanup):
    """Asks Swarming to cleanup after this bot once it's gone."""
    # TODO: Implement.
    _ = auto_cleanup

  def update_rbe_state(self, instance, hybrid_mode, session):
    self._bot._rbe_instance = instance
    self._bot._rbe_hybrid_mode = hybrid_mode
    self._bot._rbe_session = session
    self._refresh_attributes()

  def update_rbe_worker_properties(self, worker_properties):
    self._bot._rbe_worker_properties = worker_properties
    self._refresh_attributes()

  def update_dimensions(self, new_dimensions):
    """Updates `bot.dimensions` by merging-in automatically set dimensions."""
    dimensions = new_dimensions.copy()
    dimensions.update(self._bot._server_side_dimensions)
    bot_config_name = self._bot._bot_config.get('name')
    if bot_config_name:
      dimensions['bot_config'] = [bot_config_name]
    self._bot._dimensions = dimensions
    if self._bot._remote:
      self._bot._remote.bot_id = dimensions.get('id', [None])[0]

  def update_state(self, new_state):
    """Updates `bot.state` by merging-in automatically set keys."""
    state = new_state.copy()
    state['rbe_instance'] = self._bot._rbe_instance
    if self._bot._rbe_instance:
      state['rbe_session'] = self._bot._rbe_session
      state['rbe_hybrid_mode'] = self._bot._rbe_hybrid_mode
      state['rbe_idle'] = self._bot._idle
    else:
      state.pop('rbe_session', None)
      state.pop('rbe_hybrid_mode', None)
      state.pop('rbe_idle', None)
    if self._bot._rbe_worker_properties:
      state['rbe_worker_props'] = self._bot._rbe_worker_properties.to_dict()
    else:
      state.pop('rbe_worker_props', None)
    if self._bot._bot_config:
      state['bot_config'] = self._bot._bot_config
    self._bot._state = state

  def add_lifecycle_callback(self, callback):
    """Registers a callback called before the bot terminates or reboots.

    Callbacks will be executed in reverse order of their registration.

    If the bot is very broken (e.g. can't reboot), callbacks can be called
    multiple times or even periodically. They must be idempotent.

    Args:
      callback: a callback that will be called like `callback(botobj, event)`,
        where `event` will be either "exit" or "reboot".
    """
    self._bot._lifecycle_callbacks.append(callback)

  def _refresh_attributes(self):
    """Updates automatically set keys in `bot.dimensions` and `bot.state`."""
    self.update_dimensions(self._bot._dimensions)
    self.update_state(self._bot._state)


### Private stuff.


def _get_stripper(paths):
  """Returns a function to strip common path prefixes.

  There are 3 kinds of paths:
    - relative paths
    - absolute paths
    - absolute paths in stdlib
  """
  if not paths:
    return lambda f: f

  stdlib = os.path.dirname(os.__file__)
  # Find the common root for paths not in stdlib and not relative.
  split_paths = [[c for c in p.split(os.path.sep) if c] for p in paths
                 if os.path.isabs(p) and not p.startswith(stdlib)]
  common = None
  if split_paths:
    common = []
    for c1, c2 in zip(min(split_paths), max(split_paths)):
      if c1 != c2:
        break
      common.append(c1)
    if common:
      if sys.platform == 'win32':
        common = os.path.sep.join(common)
      else:
        common = os.path.sep + os.path.sep.join(common)

  def stripper(f):
    if f.startswith(stdlib):
      return f[len(stdlib) + 1:]
    if os.path.isabs(f) and common:
      return f[len(common) + 1:]
    if f.startswith('./'):
      return f[2:]
    return f

  return stripper


def _make_stack():
  """Returns a well formatted call stack."""
  frame = inspect.currentframe().f_back
  frames = []
  while frame and len(frames) < 50:
    frames.append(frame)
    frame = frame.f_back
  strip = _get_stripper(f.f_code.co_filename for f in frames)
  return '\n'.join(
      '  %-2d %s:%s:%s()' %
      (i, strip(f.f_code.co_filename), f.f_lineno, f.f_code.co_name)
      for i, f in enumerate(frames))


def _gen_session_id():
  """Generates a new bot session ID."""
  return '%s/%d' % (uuid.uuid4().hex, os.getpid())

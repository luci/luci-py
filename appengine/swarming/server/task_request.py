# coding: utf-8
# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Tasks definition.

Each user request creates a new TaskRequest. The TaskRequest instance saves the
metadata of the request, e.g. who requested it, when why, etc. It links to the
actual data of the request in a TaskProperties. The TaskProperties represents
everything needed to run the task.

This means if two users request an identical task, it can be deduped
accordingly and efficiently by the scheduler.

Note that the mere existence of a TaskRequest in the db doesn't mean it will be
scheduled, see task_scheduler.py for the actual scheduling. Registering tasks
and scheduling are kept separated to keep the immutable and mutable models in
separate files.


Overview of transactions:
- TaskRequest() are created inside a transaction.


Graph of the schema:

    +--------Root-------+
    |TaskRequest        |
    |  +--------------+ |
    |  |TaskProperties| |
    |  |  +--------+  | |
    |  |  |FilesRef|  | |
    |  |  +--------+  | |
    |  +--------------+ |
    |id=<based on epoch>|
    +-------------------+
        |          |
        v          |
    +-----------+  |
    |SecretBytes|  |
    |id=1       |  |
    +-----------+  |
                   |
                   v
    <See task_to_run.py and task_result.py>

TaskProperties is embedded in TaskRequest. TaskProperties is still declared as a
separate entity to clearly declare the boundary for task request deduplication.
"""

import datetime
import hashlib
import posixpath
import random
import re

from google.appengine.api import datastore_errors
from google.appengine.ext import ndb

from components import auth
from components import datastore_utils
from components import pubsub
from components import utils
from components.config import validation

from server import config
from server import service_accounts
from server import task_pack
import cipd


# Maximum acceptable priority value, which is effectively the lowest priority.
MAXIMUM_PRIORITY = 255


# Three days in seconds. Add 10s to account for small jitter.
_THREE_DAY_SECS = 3*24*60*60 + 10


# Seven day in seconds. Add 10s to account for small jitter.
_SEVEN_DAYS_SECS = 7*24*60*60 + 10


# Minimum value for timeouts.
_MIN_TIMEOUT_SECS = 1 if utils.is_local_dev_server() else 30


# The world started on 2010-01-01 at 00:00:00 UTC. The rationale is that using
# EPOCH (1970) means that 40 years worth of keys are wasted.
#
# Note: This creates a 'naive' object instead of a formal UTC object. Note that
# datetime.datetime.utcnow() also return naive objects. That's python.
_BEGINING_OF_THE_WORLD = datetime.datetime(2010, 1, 1, 0, 0, 0, 0)


# Used for isolated files.
_HASH_CHARS = frozenset('0123456789abcdef')

# Keep synced with named_cache.py
_CACHE_NAME_RE = re.compile(ur'^[a-z0-9_]{1,4096}$')


# Early verification of environment variable key name.
_ENV_KEY_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


### Properties validators must come before the models.


def _validate_length(prop, value, maximum):
  if len(value) > maximum:
    raise datastore_errors.BadValueError(
        'too long %s: %d > %d' % (prop._name, len(value), maximum))


def _get_validate_length(maximum):
  return lambda prop, value: _validate_length(prop, value, maximum)


def _validate_isolated(prop, value):
  if not value:
    return

  if not _HASH_CHARS.issuperset(value):
    raise datastore_errors.BadValueError(
        '%s must be lowercase hex, not %s' %
        (prop._name, value))

  length = len(value)
  if length not in (40, 64, 128):
    raise datastore_errors.BadValueError(
        '%s must be lowercase hex of length 40, 64 or 128, but length is %d' %
        (prop._name, length))


def _validate_url(prop, value):
  _validate_length(prop, value, 8192)
  if value and not validation.is_valid_secure_url(value):
    raise datastore_errors.BadValueError(
        '%s must be valid HTTPS URL, not %s' % (prop._name, value))


def _validate_namespace(prop, value):
  _validate_length(prop, value, 128)
  if not config.NAMESPACE_RE.match(value):
    raise datastore_errors.BadValueError('malformed %s' % prop._name)


def _validate_dimensions(_prop, value):
  """Validates TaskProperties.dimensions."""
  if not value:
    raise datastore_errors.BadValueError(u'dimensions must be specified')
  if len(value) > 64:
    raise datastore_errors.BadValueError('dimensions can have up to 64 entries')

  is_list = isinstance(value.items()[0][1], (list, tuple))
  normalized = {}
  for k, values in value.iteritems():
    if not k or not isinstance(k, unicode):
      raise datastore_errors.BadValueError(
          'dimensions must be a dict of strings or list of string, not %r' %
          value)
    if u':' in k:
      raise datastore_errors.BadValueError('dimensions key cannot contain ":"')
    if k.strip() != k:
      raise datastore_errors.BadValueError('dimensions key has whitespace')
    if not values:
      raise datastore_errors.BadValueError(
          'dimensions must be a dict of strings or list of string, not %r' %
          value)
    if is_list:
      if (not isinstance(values, (list, tuple)) or
          not all(isinstance(v, unicode) for v in values)):
        raise datastore_errors.BadValueError(
            'dimensions must be a dict of strings or list of string, not %r' %
            value)
      if k == u'id' and len(values) != 1:
        raise datastore_errors.BadValueError(
            u'\'id\' cannot be specified more than once in dimensions')
      # Do not allow a task to be triggered in multiple pools, as this could
      # cross a security boundary.
      if k == u'pool' and len(values) != 1:
        raise datastore_errors.BadValueError(
            u'\'pool\' cannot be specified more than once in dimensions')
      # Always store the values sorted, that simplies the code.
      normalized[k] = sorted(values)
    else:
      if not isinstance(values, unicode):
        raise datastore_errors.BadValueError(
            'dimensions must be a dict of strings or list of string, not %r' %
            value)
      normalized[k] = values

  return normalized


def _validate_env_key(prop, key):
  """Validates TaskProperties.env."""
  maxlen = 1024
  if not isinstance(key, unicode):
    raise TypeError(
        '%s must have string key, not %r' % (prop._name, key))
  if not key:
    raise datastore_errors.BadValueError(
        'valid key are required in %s' % prop._name)
  if len(key) > maxlen:
    raise datastore_errors.BadValueError(
        'key in %s is too long: %d > %d' % (prop._name, len(key), maxlen))
  if not _ENV_KEY_RE.match(key):
    raise datastore_errors.BadValueError(
        'key in %s is invalid: %r' % (prop._name, key))


def _validate_env(prop, value):
  # pylint: disable=W0212
  if not all(isinstance(v, unicode) for v in value.itervalues()):
    raise TypeError(
        '%s must be a dict of strings, not %r' % (prop._name, value))
  maxlen = 1024
  for k, v in value.iteritems():
    _validate_env_key(prop, k)
    if len(v) > maxlen:
      raise datastore_errors.BadValueError(
          'key in %s is too long: %d > %d' % (prop._name, len(v), maxlen))
  if len(value) > 64:
    raise datastore_errors.BadValueError(
        '%s can have up to 64 keys' % prop._name)


def _validate_env_prefixes(prop, value):
  for k, v in value.iteritems():
    if not isinstance(v, list):
      raise TypeError(
          '%s must have list value, not %r' % (prop._name, v))
    _validate_env_key(prop, k)
    for path in v:
      _validate_rel_path('Env Prefix', path)

  if len(value) > 64:
    raise datastore_errors.BadValueError(
        '%s can have up to 64 keys' % prop._name)


def _validate_expiration(prop, value):
  """Validates TaskRequest.expiration_ts."""
  now = utils.utcnow()
  offset = int(round((value - now).total_seconds()))
  if not (_MIN_TIMEOUT_SECS <= offset <= _SEVEN_DAYS_SECS):
    # pylint: disable=W0212
    raise datastore_errors.BadValueError(
        '%s (%s, %ds from now) must effectively be between %ds and 7 days '
        'from now (%s)' %
        (prop._name, value, offset, _MIN_TIMEOUT_SECS, now))


def _validate_grace(prop, value):
  """Validates grace_period_secs in TaskProperties."""
  if not (0 <= value <= 60*60):
    # pylint: disable=W0212
    raise datastore_errors.BadValueError(
        '%s (%ds) must be between 0s and one hour' % (prop._name, value))


def _validate_priority(_prop, value):
  """Validates TaskRequest.priority."""
  validate_priority(value)
  return value


def _validate_task_run_id(_prop, value):
  """Validates a task_id looks valid without fetching the entity."""
  if not value:
    return None
  task_pack.unpack_run_result_key(value)
  return value


def _validate_timeout(prop, value):
  """Validates timeouts in seconds in TaskProperties."""
  if value and not (_MIN_TIMEOUT_SECS <= value <= _THREE_DAY_SECS):
    # pylint: disable=W0212
    raise datastore_errors.BadValueError(
        '%s (%ds) must be 0 or between %ds and three days' %
            (prop._name, value, _MIN_TIMEOUT_SECS))


def _validate_tags(prop, value):
  """Validates TaskRequest.tags."""
  _validate_length(prop, value, 1024)
  if ':' not in value:
    # pylint: disable=W0212
    raise datastore_errors.BadValueError(
        '%s must be key:value form, not %s' % (prop._name, value))


def _validate_pubsub_topic(prop, value):
  """Validates TaskRequest.pubsub_topic."""
  _validate_length(prop, value, 1024)
  # pylint: disable=W0212
  if value and '/' not in value:
    raise datastore_errors.BadValueError(
        '%s must be a well formatted pubsub topic' % (prop._name))


def _validate_package_name_template(prop, value):
  """Validates a CIPD package name template."""
  _validate_length(prop, value, 1024)
  if not cipd.is_valid_package_name_template(value):
    raise datastore_errors.BadValueError(
        '%s must be a valid CIPD package name template "%s"' % (
              prop._name, value))


def _validate_package_version(prop, value):
  """Validates a CIPD package version."""
  _validate_length(prop, value, 1024)
  if not cipd.is_valid_version(value):
    raise datastore_errors.BadValueError(
        '%s must be a valid package version "%s"' % (prop._name, value))


def _validate_cache_name(prop, value):
  _validate_length(prop, value, 1024)
  if not _CACHE_NAME_RE.match(value):
    raise datastore_errors.BadValueError(
        '%s %r does not match %s' % (prop._name, value, _CACHE_NAME_RE.pattern))


def _validate_cache_path(prop, value):
  _validate_length(prop, value, 1024)
  _validate_rel_path('Cache path', value)


def _validate_package_path(prop, value):
  """Validates a CIPD installation path."""
  _validate_length(prop, value, 1024)
  if not value:
    raise datastore_errors.BadValueError(
        'CIPD package path is required. Use "." to install to run dir.')
  _validate_rel_path('CIPD package path', value)


def _validate_output_path(prop, value):
  """Validates a path for an output file."""
  _validate_length(prop, value, 1024)
  _validate_rel_path('output file', value)


def _validate_rel_path(value_name, path):
  """Validates a relative path to be valid."""
  if not path:
    raise datastore_errors.BadValueError(
        'No argument provided for %s.' % value_name)
  if '\\' in path:
    raise datastore_errors.BadValueError(
        '%s cannot contain \\. On Windows forward-slashes '
        'will be replaced with back-slashes.' % value_name)
  if '..' in path.split('/'):
    raise datastore_errors.BadValueError(
        '%s cannot contain "..".' % value_name)
  normalized = posixpath.normpath(path)
  if path != normalized:
    raise datastore_errors.BadValueError(
        '%s is not normalized. Normalized is "%s".' % (value_name, normalized))
  if path.startswith('/'):
    raise datastore_errors.BadValueError(
        '%s cannot start with "/".' % value_name)


def _validate_service_account(prop, value):
  """Validates that 'service_account' field is 'bot', 'none' or email."""
  _validate_length(prop, value, 1024)
  if not value:
    return None
  if value in ('bot', 'none') or service_accounts.is_service_account(value):
    return value
  raise datastore_errors.BadValueError(
      '%r must be an email, "bot" or "none" string, got %r' %
      (prop._name, value))


### Models.


class FilesRef(ndb.Model):
  """Defines a data tree reference, normally a reference to a .isolated file."""

  # TODO(maruel): make this class have one responsibility. Currently it is used
  # in two modes:
  # - a reference to a tree, as class docstring says.
  # - input/output settings in TaskProperties.

  # The hash of an isolated archive.
  isolated = ndb.StringProperty(validator=_validate_isolated, indexed=False)
  # The hostname of the isolated server to use.
  isolatedserver = ndb.StringProperty(
      validator=_validate_url, indexed=False)
  # Namespace on the isolate server.
  namespace = ndb.StringProperty(validator=_validate_namespace, indexed=False)

  def _pre_put_hook(self):
    # TODO(maruel): Get default value from config
    # IsolateSettings.default_server.
    super(FilesRef, self)._pre_put_hook()
    if not self.isolatedserver or not self.namespace:
      raise datastore_errors.BadValueError(
          'isolate server and namespace are required')


class SecretBytes(ndb.Model):
  """Defines an optional secret byte string logically defined with the
  TaskProperties.

  Stored separately for size and data-leakage reasons.
  """
  _use_memcache = False
  secret_bytes = ndb.BlobProperty(
      validator=_get_validate_length(20*1024), indexed=False)


class CipdPackage(ndb.Model):
  """A CIPD package to install in the run dir before task execution.

  A part of TaskProperties.
  """
  # Package name template. May use cipd.ALL_PARAMS.
  # Most users will specify ${platform} parameter.
  package_name = ndb.StringProperty(
      indexed=False, validator=_validate_package_name_template)
  # Package version that is valid for all packages matched by package_name.
  # Most users will specify tags.
  version = ndb.StringProperty(
      indexed=False, validator=_validate_package_version)
  # Path to dir, relative to the run dir, where to install the package.
  # If empty, the package will be installed in the run dir.
  path = ndb.StringProperty(indexed=False, validator=_validate_package_path)

  def __str__(self):
    return '%s:%s' % (self.package_name, self.version)

  def _pre_put_hook(self):
    super(CipdPackage, self)._pre_put_hook()
    if not self.package_name:
      raise datastore_errors.BadValueError('CIPD package name is required')
    if not self.version:
      raise datastore_errors.BadValueError('CIPD package version is required')


class CipdInput(ndb.Model):
  """Specifies which CIPD client and packages to install, from which server.

  A part of TaskProperties.
  """
  # URL of the CIPD server. Must start with "https://" or "http://".
  server = ndb.StringProperty(indexed=False, validator=_validate_url)

  # CIPD package of CIPD client to use.
  # client_package.version is required.
  # client_package.path must be None.
  client_package = ndb.LocalStructuredProperty(CipdPackage)

  # List of packages to install.
  packages = ndb.LocalStructuredProperty(CipdPackage, repeated=True)

  def _pre_put_hook(self):
    if not self.server:
      raise datastore_errors.BadValueError('cipd server is required')
    if not self.client_package:
      raise datastore_errors.BadValueError('client_package is required')
    if self.client_package.path:
      raise datastore_errors.BadValueError('client_package.path must be unset')
    self.client_package._pre_put_hook()

    if not self.packages:
      raise datastore_errors.BadValueError(
          'cipd_input cannot have an empty package list')
    if len(self.packages) > 64:
      raise datastore_errors.BadValueError(
          'Up to 64 CIPD packages can be listed for a task')

    # Make sure we don't install multiple versions of the same package at the
    # same path.
    package_path_names = set()
    for p in self.packages:
      p._pre_put_hook()
      if not p.path:
        raise datastore_errors.BadValueError(
            'package %s:%s: path is required' % (p.package_name, p.version))
      path_name = (p.path, p.package_name)
      if path_name in package_path_names:
        raise datastore_errors.BadValueError(
           'package %r is specified more than once in path %r'
          % (p.package_name, p.path))
      package_path_names.add(path_name)
    self.packages.sort(key=lambda p: (p.path, p.package_name))


class CacheEntry(ndb.Model):
  """Describes a named cache that should be present on the bot."""
  name = ndb.StringProperty(validator=_validate_cache_name)
  path = ndb.StringProperty(validator=_validate_cache_path)

  def _pre_put_hook(self):
    if not self.name:
      raise datastore_errors.BadValueError('name is not specified')


class TaskProperties(ndb.Model):
  """Defines all the properties of a task to be run on the Swarming
  infrastructure.

  This entity is not saved in the DB as a standalone entity, instead it is
  embedded in a TaskRequest.

  This model is immutable.

  New-style TaskProperties supports invocation of run_isolated. When this
  behavior is desired, the member .inputs_ref with an .isolated field value must
  be supplied. .extra_args can be supplied to pass extraneous arguments.
  """
  # TODO(maruel): convert inputs_ref and _TaskResultCommon.outputs_ref as:
  # - input = String which is the isolated input, if any
  # - isolated_server = <server, metadata e.g. namespace> which is a
  #   simplified version of FilesRef
  # - _TaskResultCommon.output = String which is isolated output, if any.

  caches = ndb.LocalStructuredProperty(CacheEntry, repeated=True)

  # Command to run. This overrides the command in the isolated file if any.
  command = ndb.StringProperty(repeated=True, indexed=False)

  # Relative working directory to run 'command' in, defaults to one specified
  # in an isolated file, if any, else the root mapped directory.
  relative_cwd = ndb.StringProperty(indexed=False)

  # Isolate server, namespace and input isolate hash.
  #
  # Despite its name, contains isolate server URL and namespace for isolated
  # output too. See TODO at the top of this class.
  # May be non-None even if task input is not isolated.
  #
  # Only inputs_ref.isolated or command can be specified.
  inputs_ref = ndb.LocalStructuredProperty(FilesRef)

  # CIPD packages to install.
  cipd_input = ndb.LocalStructuredProperty(CipdInput)

  # Filter to use to determine the required properties on the bot to run on. For
  # example, Windows or hostname. Encoded as json. 'pool' dimension is required
  # for all tasks except terminate (see _pre_put_hook).
  dimensions_data = datastore_utils.DeterministicJsonProperty(
      validator=_validate_dimensions, json_type=dict, indexed=False,
      name='dimensions')

  # Environment variables. Encoded as json. Optional.
  env = datastore_utils.DeterministicJsonProperty(
      validator=_validate_env, json_type=dict, indexed=False)

  # Environment path prefix variables. Encoded as json. Optional.
  #
  # Env key -> [list, of, rel, paths, to, prepend]
  env_prefixes = datastore_utils.DeterministicJsonProperty(
      validator=_validate_env_prefixes, json_type=dict, indexed=False)

  # Maximum duration the bot can take to run this task. It's named hard_timeout
  # in the bot.
  execution_timeout_secs = ndb.IntegerProperty(
      validator=_validate_timeout, required=True, indexed=False)

  # Extra arguments to supply to the command `python run_isolated ...`. Can only
  # be set if inputs_ref.isolated is set.
  extra_args = ndb.StringProperty(repeated=True, indexed=False)

  # Grace period is the time between signaling the task it timed out and killing
  # the process. During this time the process should clean up itself as quickly
  # as possible, potentially uploading partial results back.
  grace_period_secs = ndb.IntegerProperty(
      validator=_validate_grace, default=30, indexed=False)

  # Bot controlled timeout for new bytes from the subprocess. If a subprocess
  # doesn't output new data to stdout for .io_timeout_secs, consider the command
  # timed out. Optional.
  io_timeout_secs = ndb.IntegerProperty(
      validator=_validate_timeout, indexed=False)

  # If True, the task can safely be served results from a previously succeeded
  # task.
  idempotent = ndb.BooleanProperty(default=False, indexed=False)

  # A list of outputs expected. If empty, all files written to
  # $(ISOLATED_OUTDIR) will be returned; otherwise, the files in this list
  # will be added to those in that directory.
  outputs = ndb.StringProperty(repeated=True, indexed=False,
      validator=_validate_output_path)

  # If True, the TaskRequest embedding these TaskProperties has an associated
  # SecretBytes entity.
  has_secret_bytes = ndb.BooleanProperty(default=False, indexed=False)

  @property
  def dimensions(self):
    """Returns dimensions as a dict(unicode, list(unicode)), even for older
    entities.
    """
    # Just look at the first one. The property is guaranteed to be internally
    # consistent.
    for v in self.dimensions_data.itervalues():
      if isinstance(v, (list, tuple)):
        return self.dimensions_data
      break
    return {k: [v] for k, v in self.dimensions_data.iteritems()}

  @property
  def is_terminate(self):
    """If True, it is a terminate request."""
    # Check dimensions last because it's a bit slower.
    return (
        not self.caches and
        not self.command and
        not (self.inputs_ref and self.inputs_ref.isolated) and
        not self.cipd_input and
        not self.env and
        not self.env_prefixes and
        not self.execution_timeout_secs and
        not self.extra_args and
        not self.grace_period_secs and
        not self.io_timeout_secs and
        not self.idempotent and
        not self.outputs and
        not self.has_secret_bytes and
        self.dimensions_data.keys() == [u'id'])

  def to_dict(self):
    out = super(TaskProperties, self).to_dict(
        exclude=['dimensions_data'])
    # Use the data stored as-is, so properties_hash doesn't change.
    out['dimensions'] = self.dimensions_data
    return out

  def _pre_put_hook(self):
    super(TaskProperties, self)._pre_put_hook()
    if self.is_terminate:
      # Most values are not valid with a terminate task. self.is_terminate
      # already check those. Terminate task can only use 'id'.
      return

    if u'pool' not in self.dimensions_data:
      # Only terminate task may not use 'pool'. Others must specify one.
      raise datastore_errors.BadValueError(
          u'\'pool\' must be used as dimensions')

    # Isolated input and commands.
    isolated_input = self.inputs_ref and self.inputs_ref.isolated
    if not self.command and not isolated_input:
      raise datastore_errors.BadValueError(
          'use at least one of command or inputs_ref.isolated')
    if self.command and self.extra_args:
      raise datastore_errors.BadValueError(
          'can\'t use both command and extra_args')
    if self.extra_args and not isolated_input:
      raise datastore_errors.BadValueError(
          'extra_args require inputs_ref.isolated')
    if self.inputs_ref:
      self.inputs_ref._pre_put_hook()
    if len(self.command) > 256:
      raise datastore_errors.BadValueError(
          'command can have up to 256 arguments')
    if len(self.extra_args) > 256:
      raise datastore_errors.BadValueError(
          'extra_args can have up to 256 arguments')

    # Validate caches.
    if len(self.caches) > 64:
      raise datastore_errors.BadValueError(
          'Up to 64 caches can be listed for a task')
    cache_names = set()
    cache_paths = set()
    for c in self.caches:
      c._pre_put_hook()
      if c.name in cache_names:
        raise datastore_errors.BadValueError(
            'Cache name %s is used more than once' % c.name)
      if c.path in cache_paths:
        raise datastore_errors.BadValueError(
            'Cache path "%s" is mapped more than once' % c.path)
      cache_names.add(c.name)
      cache_paths.add(c.path)
    self.caches.sort(key=lambda c: c.name)

    # Validate CIPD Input.
    if self.cipd_input:
      self.cipd_input._pre_put_hook()
      for p in self.cipd_input.packages:
        if p.path in cache_paths:
          raise datastore_errors.BadValueError(
              'Path "%s" is mapped to a named cache and cannot be a target '
              'of CIPD installation' % p.path)
      if self.idempotent:
        pinned = lambda p: cipd.is_pinned_version(p.version)
        assert self.cipd_input.packages  # checked by cipd_input._pre_put_hook
        if any(not pinned(p) for p in self.cipd_input.packages):
          raise datastore_errors.BadValueError(
              'an idempotent task cannot have unpinned packages; '
              'use tags or instance IDs as package versions')

    if len(self.outputs) > 4096:
      raise datastore_errors.BadValueError(
          'Up to 4096 outputs can be listed for a task')


class TaskRequest(ndb.Model):
  """Contains a user request.

  Key id is a decreasing integer based on time since utils.EPOCH plus some
  randomness on lower order bits. See new_request_key() for the complete gory
  details.

  This model is immutable.
  """
  # Hashing algorithm used to hash TaskProperties to create its key.
  HASHING_ALGO = hashlib.sha256

  # Time this request was registered. It is set manually instead of using
  # auto_now_add=True so that expiration_ts can be set very precisely relative
  # to this property.
  created_ts = ndb.DateTimeProperty(required=True)

  # The name for this task request. It's only for description.
  name = ndb.StringProperty(required=True)

  # Authenticated client that triggered this task.
  authenticated = auth.IdentityProperty()

  # Which user to blame for this task. Can be arbitrary, not asserted by any
  # credentials.
  user = ndb.StringProperty(default='')

  # Indicates what OAuth2 credentials the task uses when calling other services.
  #
  # Possible values are: 'none', 'bot' or <email>. For more information see
  # swarming_rpcs.NewTaskRequest.
  #
  # This property exists only for informational purposes and for indexing. When
  # actually getting an OAuth credentials, the properly signed OAuth grant token
  # (stored in hidden 'service_account_token' field) is used.
  service_account = ndb.StringProperty(validator=_validate_service_account)

  # The "OAuth token grant" generated when the task was posted.
  #
  # This is an opaque token generated by the Token Server at the time the task
  # was posted (when the end-user is still present). It can be exchanged
  # for an OAuth token of some service account at a later time (when the task is
  # actually running on some bot).
  #
  # This property never shows up in UI or API responses.
  service_account_token = ndb.BlobProperty()

  # The actual properties are embedded in this model.
  properties = ndb.LocalStructuredProperty(
      TaskProperties, compressed=True, required=True)

  # Priority of the task to be run. A lower number is higher priority, thus will
  # preempt requests with lower priority (higher numbers).
  priority = ndb.IntegerProperty(
      indexed=False, validator=_validate_priority, required=True)

  # If the task request is not scheduled by this moment, it will be aborted by a
  # cron job. It is saved instead of scheduling_expiration_secs so finding
  # expired jobs is a simple query.
  expiration_ts = ndb.DateTimeProperty(
      indexed=True, validator=_validate_expiration, required=True)

  # Tags that specify the category of the task.
  tags = ndb.StringProperty(repeated=True, validator=_validate_tags)

  # Set when a task (the parent) reentrantly create swarming tasks. Must be set
  # to a valid task_id pointing to a TaskRunResult or be None.
  parent_task_id = ndb.StringProperty(validator=_validate_task_run_id)

  # PubSub topic to send task completion notification to.
  pubsub_topic = ndb.StringProperty(
      indexed=False, validator=_validate_pubsub_topic)

  # Secret token to send as 'auth_token' attribute with PubSub messages.
  pubsub_auth_token = ndb.StringProperty(indexed=False)

  # Data to send in 'userdata' field of PubSub messages.
  pubsub_userdata = ndb.StringProperty(
      indexed=False, validator=_get_validate_length(1024))

  # This stores the computed properties_hash for this Task's properties object.
  # It is set in init_new_request. It is None for non-idempotent tasks.
  properties_hash = ndb.BlobProperty(indexed=False)

  @property
  def secret_bytes_key(self):
    if self.properties.has_secret_bytes:
      return task_pack.request_key_to_secret_bytes_key(self.key)

  @property
  def task_id(self):
    """Returns the TaskResultSummary packed id, not the task request key."""
    return task_pack.pack_result_summary_key(
        task_pack.request_key_to_result_summary_key(self.key))

  @property
  def expiration_secs(self):
    """Reconstructs this value from expiration_ts and created_ts. Integer."""
    return int((self.expiration_ts - self.created_ts).total_seconds())

  def to_dict(self):
    """Converts properties_hash to hex so it is json serializable."""
    # to_dict() doesn't recurse correctly into ndb.LocalStructuredProperty!
    out = super(TaskRequest, self).to_dict(
        exclude=['pubsub_auth_token', 'properties', 'service_account_token'])
    out['properties'] = self.properties.to_dict()
    properties_hash = out['properties_hash']
    out['properties_hash'] = (
        properties_hash.encode('hex') if properties_hash else None)
    return out

  def _pre_put_hook(self):
    """Adds automatic tags."""
    super(TaskRequest, self)._pre_put_hook()
    self.properties._pre_put_hook()
    if self.properties.is_terminate:
      if not self.priority == 0:
        raise datastore_errors.BadValueError(
            'terminate request must be priority 0')
    elif self.priority == 0:
      raise datastore_errors.BadValueError(
          'priority 0 can only be used for terminate request')

    if len(self.tags) > 256:
      raise datastore_errors.BadValueError(
          'up to 256 tags can be specified for a task request')

    if (self.pubsub_topic and
        not pubsub.validate_full_name(self.pubsub_topic, 'topics')):
      raise datastore_errors.BadValueError(
          'bad pubsub topic name - %s' % self.pubsub_topic)
    if self.pubsub_auth_token and not self.pubsub_topic:
      raise datastore_errors.BadValueError(
          'pubsub_auth_token requires pubsub_topic')
    if self.pubsub_userdata and not self.pubsub_topic:
      raise datastore_errors.BadValueError(
          'pubsub_userdata requires pubsub_topic')


### Private stuff.


def _get_automatic_tags(request):
  """Returns tags that should automatically be added to the TaskRequest."""
  tags = [
    u'priority:%s' % request.priority,
    u'service_account:%s' % (request.service_account or u'None'),
    u'user:%s' % (request.user or u'None'),
  ]
  for key, values in request.properties.dimensions.iteritems():
    for value in values:
      tags.append(u'%s:%s' % (key, value))
  return tags


### Public API.


def create_termination_task(bot_id):
  """Returns a task to terminate the given bot.

  ACL check must have been done before.

  Returns:
    TaskRequest for priority 0 (highest) termination task.
  """
  properties = TaskProperties(
      dimensions_data={u'id': unicode(bot_id)},
      execution_timeout_secs=0,
      grace_period_secs=0,
      io_timeout_secs=0)
  now = utils.utcnow()
  request = TaskRequest(
      created_ts=now,
      expiration_ts=now + datetime.timedelta(days=1),
      name=u'Terminate %s' % bot_id,
      priority=0,
      properties=properties,
      tags=[u'terminate:1'])
  assert request.properties.is_terminate
  init_new_request(request, True, None)
  return request


def new_request_key():
  """Returns a valid ndb.Key for this entity.

  Task id is a 64 bits integer represented as a string to the user:
  - 1 highest order bits set to 0 to keep value positive.
  - 43 bits is time since _BEGINING_OF_THE_WORLD at 1ms resolution.
    It is good for 2**43 / 365.3 / 24 / 60 / 60 / 1000 = 278 years or 2010+278 =
    2288. The author will be dead at that time.
  - 16 bits set to a random value or a server instance specific value. Assuming
    an instance is internally consistent with itself, it can ensure to not reuse
    the same 16 bits in two consecutive requests and/or throttle itself to one
    request per millisecond.
    Using random value reduces to 2**-15 the probability of collision on exact
    same timestamp at 1ms resolution, so a maximum theoretical rate of 65536000
    requests/sec but an effective rate in the range of ~64k requests/sec without
    much transaction conflicts. We should be fine.
  - 4 bits set to 0x1. This is to represent the 'version' of the entity schema.
    Previous version had 0. Note that this value is XOR'ed in the DB so it's
    stored as 0xE. When the TaskRequest entity tree is modified in a breaking
    way that affects the packing and unpacking of task ids, this value should be
    bumped.

  The key id is this value XORed with task_pack.TASK_REQUEST_KEY_ID_MASK. The
  reason is that increasing key id values are in decreasing timestamp order.
  """
  # TODO(maruel): Use real randomness.
  suffix = random.getrandbits(16)
  return convert_to_request_key(utils.utcnow(), suffix)


def request_key_to_datetime(request_key):
  """Converts a TaskRequest.key to datetime.

  See new_request_key() for more details.
  """
  if request_key.kind() != 'TaskRequest':
    raise ValueError('Expected key to TaskRequest, got %s' % request_key.kind())
  # Ignore lowest 20 bits.
  xored = request_key.integer_id() ^ task_pack.TASK_REQUEST_KEY_ID_MASK
  offset_ms = (xored >> 20) / 1000.
  return _BEGINING_OF_THE_WORLD + datetime.timedelta(seconds=offset_ms)


def datetime_to_request_base_id(now):
  """Converts a datetime into a TaskRequest key base value.

  Used for query order().
  """
  if now < _BEGINING_OF_THE_WORLD:
    raise ValueError(
        'Time %s is set to before %s' % (now, _BEGINING_OF_THE_WORLD))
  delta = now - _BEGINING_OF_THE_WORLD
  return int(round(delta.total_seconds() * 1000.)) << 20


def convert_to_request_key(date, suffix=0):
  assert 0 <= suffix <= 0xffff
  request_id_base = datetime_to_request_base_id(date)
  return request_id_to_key(int(request_id_base | suffix << 4 | 0x1))


def request_id_to_key(request_id):
  """Converts a request id into a TaskRequest key.

  Note that this function does NOT accept a task id. This functions is primarily
  meant for limiting queries to a task creation range.
  """
  return ndb.Key(TaskRequest, request_id ^ task_pack.TASK_REQUEST_KEY_ID_MASK)


def validate_request_key(request_key):
  if request_key.kind() != 'TaskRequest':
    raise ValueError('Expected key to TaskRequest, got %s' % request_key.kind())
  task_id = request_key.integer_id()
  if not task_id:
    raise ValueError('Invalid null TaskRequest key')
  if (task_id & 0xF) == 0xE:
    # New style key.
    return

  # Check the shard.
  # TODO(maruel): Remove support 2015-02-01.
  request_shard_key = request_key.parent()
  if not request_shard_key:
    raise ValueError('Expected parent key for TaskRequest, got nothing')
  if request_shard_key.kind() != 'TaskRequestShard':
    raise ValueError(
        'Expected key to TaskRequestShard, got %s' % request_shard_key.kind())
  root_entity_shard_id = request_shard_key.string_id()
  if (not root_entity_shard_id or
      len(root_entity_shard_id) != task_pack.DEPRECATED_SHARDING_LEVEL):
    raise ValueError(
        'Expected root entity key (used for sharding) to be of length %d but '
        'length was only %d (key value %r)' % (
            task_pack.DEPRECATED_SHARDING_LEVEL,
            len(root_entity_shard_id or ''),
            root_entity_shard_id))


def init_new_request(request, allow_high_priority, secret_bytes_ent):
  """Initializes a new TaskRequest but doesn't store it.

  ACL check must have been done before, except for high priority task.

  Fills up some values and does minimal checks.

  If parent_task_id is set, properties for the parent are used:
  - priority: defaults to parent.priority - 1
  - user: overridden by parent.user

  """
  assert request.__class__ is TaskRequest, request
  if request.parent_task_id:
    run_result_key = task_pack.unpack_run_result_key(request.parent_task_id)
    result_summary_key = task_pack.run_result_key_to_result_summary_key(
        run_result_key)
    request_key = task_pack.result_summary_key_to_request_key(
        result_summary_key)
    parent = request_key.get()
    if not parent or parent.properties.is_terminate:
      raise ValueError('parent_task_id is not a valid task')
    request.priority = min(request.priority, max(parent.priority - 1, 1))
    # Drop the previous user.
    request.user = parent.user

  # If the priority is below 20, make sure the user has right to do so.
  if request.priority < 20 and not allow_high_priority:
    # Special case for terminate request.
    if not request.properties.is_terminate:
      # Silently drop the priority of normal users.
      request.priority = 20

  request.authenticated = auth.get_current_identity()
  if (not request.properties.is_terminate and
      request.properties.grace_period_secs is None):
    request.properties.grace_period_secs = 30
  if request.properties.idempotent is None:
    request.properties.idempotent = False

  # Convert None to 'none', to make it indexable. Here request.service_account
  # can be 'none', 'bot' or an <email>. When using <email>, callers of
  # 'init_new_request' are expected to generate new service account token
  # (by making an RPC to the token server) and put it into service_account_token
  # before storing it.
  request.service_account = request.service_account or 'none'
  request.service_account_token = None

  # This is useful to categorize the task.
  tags = set(request.tags).union(_get_automatic_tags(request))
  request.tags = sorted(tags)

  if request.properties.idempotent:
    props = request.properties.to_dict()
    if secret_bytes_ent is not None:
      props['secret_bytes'] = secret_bytes_ent.secret_bytes.encode('hex')
    request.properties_hash = request.HASHING_ALGO(
        utils.encode_to_json(props)).digest()
  else:
    request.properties_hash = None


def validate_priority(priority):
  """Throws ValueError if priority is not a valid value."""
  if 0 > priority or MAXIMUM_PRIORITY < priority:
    raise datastore_errors.BadValueError(
        'priority (%d) must be between 0 and %d (inclusive)' %
        (priority, MAXIMUM_PRIORITY))

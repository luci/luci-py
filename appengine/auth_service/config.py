# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Adapter between config service client and the rest of auth_service.

Basically a cron job that each minute refetches config files from config service
and modifies auth service datastore state if anything changed.

Following files are fetched:
  imports.cfg - configuration for group importer cron job.
  ip_whitelist.cfg - IP whitelists.
  oauth.cfg - OAuth client_id whitelist.

Configs are ASCII serialized protocol buffer messages. The schema is defined in
proto/config.proto.

Storing infrequently changing configuration in the config service (implemented
on top of source control) allows to use code review workflow for configuration
changes as well as removes a need to write some UI for them.
"""

import collections
import logging
import posixpath

from google import protobuf
from google.appengine.ext import ndb

from components import config
from components import datastore_utils
from components import gitiles
from components import utils
from components.auth import ipaddr
from components.auth import model
from components.config import validation
from components.config import validation_context

from proto import config_pb2
import importer


# Config file revision number and where it came from.
Revision = collections.namedtuple('Revision', ['revision', 'url'])


class CannotLoadConfigError(Exception):
  """Raised when fetching configs if they are missing or invalid."""


def is_remote_configured():
  """True if config service backend URL is defined.

  If config service backend URL is not set auth_service will use datastore
  as source of truth for configuration (with some simple web UI to change it).

  If config service backend URL is set, UI for config management will be read
  only and all config changes must be performed through the config service.
  """
  return bool(get_remote_url())


def get_remote_url():
  """Returns URL of a config service if configured, to display in UI."""
  settings = config.ConfigSettings.cached()
  if settings and settings.service_hostname:
    return 'https://%s' % settings.service_hostname
  return None


def get_config_revision(path):
  """Returns tuple with info about last imported config revision."""
  schema = _CONFIG_SCHEMAS.get(path)
  return schema['revision_getter']() if schema else None


@utils.cache_with_expiration(expiration_sec=60)
def get_delegation_config():
  """Returns imported config_pb2.DelegationConfig (or empty one if missing)."""
  text = _get_service_config('delegation.cfg')
  if not text:
    return config_pb2.DelegationConfig()
  # The config MUST be valid, since we do validation before storing it.
  # Nevertheless, handle the unexpected.
  try:
    msg = config_pb2.DelegationConfig()
    protobuf.text_format.Merge(text, msg)
  except protobuf.text_format.ParseError as ex:
    logging.error('Invalid delegation.cfg: %s', ex)
    return config_pb2.DelegationConfig()
  ctx = validation_context.Context.logging()
  validate_delegation_config(msg, ctx)
  if ctx.result().has_errors:
    return config_pb2.DelegationConfig()
  return msg


def refetch_config(force=False):
  """Refetches all configs from luci-config (if enabled).

  Called as a cron job.
  """
  if not is_remote_configured():
    logging.info('Config remote is not configured')
    return

  # Grab and validate all new configs in parallel.
  try:
    configs = _fetch_configs(_CONFIG_SCHEMAS)
  except CannotLoadConfigError as exc:
    logging.error('Failed to fetch configs\n%s', exc)
    return

  # Figure out what needs to be updated.
  dirty = {}
  dirty_in_authdb = {}
  for path, (new_rev, conf) in sorted(configs.iteritems()):
    assert path in _CONFIG_SCHEMAS, path
    cur_rev = get_config_revision(path)
    if cur_rev != new_rev or force:
      if _CONFIG_SCHEMAS[path]['use_authdb_transaction']:
        dirty_in_authdb[path] = (new_rev, conf)
      else:
        dirty[path] = (new_rev, conf)
    else:
      logging.info('Config %s is up-to-date at rev %s', path, cur_rev.revision)

  # First update configs that do not touch AuthDB, one by one.
  for path, (rev, conf) in sorted(dirty.iteritems()):
    dirty = _CONFIG_SCHEMAS[path]['updater'](rev, conf)
    logging.info(
        'Processed %s at rev %s: %s', path, rev.revision,
        'updated' if dirty else 'up-to-date')

  # Configs that touch AuthDB are updated in a single transaction so that config
  # update generates single AuthDB replication task instead of a bunch of them.
  if dirty_in_authdb:
    _update_authdb_configs(dirty_in_authdb)


### Integration with config validation framework.


@validation.self_rule('delegation.cfg', config_pb2.DelegationConfig)
def validate_delegation_config(conf, ctx):
  # Helper to valid a required list of identifies (or '*').
  def validate_ident_list(name, lst):
    if not lst:
      ctx.error('missing %s field', name)
      return
    for uid in lst:
      if uid != '*':
        try:
          model.Identity.from_bytes(uid)
        except ValueError as exc:
          ctx.error('bad identity string "%s" in %s: %s', uid, name, exc)
    if '*' in lst and len(lst) != 1:
      ctx.warning('redundant entries in %s, it has "*"" already', name)

  for idx, r in enumerate(conf.rules):
    with ctx.prefix('rules #%d: ', idx):
      validate_ident_list('user_id', r.user_id)
      validate_ident_list('target_service', r.target_service)
      if r.max_validity_duration <= 0:
        ctx.error('max_validity_duration must be a positive integer')
      for s in r.allowed_to_impersonate:
        if s.startswith('group:'):
          name = s[len('group:'):]
          if not model.is_valid_group_name(name):
            ctx.error('not a valid group name: %s', name)
          continue
        if '*' in s:
          try:
            model.IdentityGlob.from_bytes(s)
          except ValueError as exc:
            ctx.error('not a valid identity glob "%s": %s', s, exc)
          continue
        try:
          model.Identity.from_bytes(s)
        except ValueError as exc:
          ctx.error('not a valid identity "%s": %s', s, exc)


# TODO(vadimsh): Below use validation context for real (e.g. emit multiple
# errors at once instead of aborting on the first one).


@validation.self_rule('imports.cfg')
def validate_imports_config(conf, ctx):
  try:
    importer.validate_config(conf)
  except ValueError as exc:
    ctx.error(str(exc))


@validation.self_rule('ip_whitelist.cfg', config_pb2.IPWhitelistConfig)
def validate_ip_whitelist_config(conf, ctx):
  try:
    _validate_ip_whitelist_config(conf)
  except ValueError as exc:
    ctx.error(str(exc))


@validation.self_rule('oauth.cfg', config_pb2.OAuthConfig)
def validate_oauth_config(conf, ctx):
  try:
    _validate_oauth_config(conf)
  except ValueError as exc:
    ctx.error(str(exc))


# Simple auth_serivce own configs stored in the datastore as plain text.
# They are different from imports.cfg (no GUI to update them other), and from
# ip_whitelist.cfg and oauth.cfg (not tied to AuthDB changes).


class _AuthServiceConfig(ndb.Model):
  """Text config file imported from luci-config.

  Root entity. Key ID is config file name.
  """
  # The body of the config itself.
  config = ndb.TextProperty()
  # Last imported SHA1 revision of the config.
  revision = ndb.StringProperty(indexed=False)
  # URL the config was imported from.
  url = ndb.StringProperty(indexed=False)


def _get_service_config_rev(cfg_name):
  """Returns last processed Revision of given config."""
  e = _AuthServiceConfig.get_by_id(cfg_name)
  return Revision(e.revision, e.url) if e else None


def _get_service_config(cfg_name):
  """Returns text of given config file or None if missing."""
  e = _AuthServiceConfig.get_by_id(cfg_name)
  return e.config if e else None


@ndb.transactional
def _update_service_config(cfg_name, rev, conf):
  """Stores new config (and its revision).

  This function is called only if config has already been validated.
  """
  assert isinstance(conf, basestring)
  e = _AuthServiceConfig.get_by_id(cfg_name) or _AuthServiceConfig(id=cfg_name)
  old = e.config
  e.populate(config=conf, revision=rev.revision, url=rev.url)
  e.put()
  return old != conf


### Group importer config implementation details.


def _get_imports_config_revision():
  """Returns Revision of last processed imports.cfg config."""
  e = importer.config_key().get()
  if not e or not isinstance(e.config_revision, dict):
    return None
  desc = e.config_revision
  return Revision(desc.get('rev'), desc.get('url'))


def _update_imports_config(rev, conf):
  """Applies imports.cfg config."""
  # Rewrite existing config even if it is the same (to update 'rev').
  cur = importer.read_config()
  importer.write_config(conf, {'rev': rev.revision, 'url': rev.url})
  return cur != conf


### Implementation of configs expanded to AuthDB entities.


class _ImportedConfigRevisions(ndb.Model):
  """Stores mapping config path -> {'rev': SHA1, 'url': URL}.

  Parent entity is AuthDB root (auth.model.root_key()). Updated in a transaction
  when importing configs.
  """
  revisions = ndb.JsonProperty()


def _imported_config_revisions_key():
  return ndb.Key(_ImportedConfigRevisions, 'self', parent=model.root_key())


def _get_authdb_config_rev(path):
  """Returns Revision of last processed config given its name."""
  mapping = _imported_config_revisions_key().get()
  if not mapping or not isinstance(mapping.revisions, dict):
    return None
  desc = mapping.revisions.get(path)
  if not isinstance(desc, dict):
    return None
  return Revision(desc.get('rev'), desc.get('url'))


@datastore_utils.transactional
def _update_authdb_configs(configs):
  """Pushes new configs to AuthDB entity group.

  Args:
    configs: dict {config path -> (Revision tuple, <config>)}.
  """
  revs = _imported_config_revisions_key().get()
  if not revs:
    revs = _ImportedConfigRevisions(
        key=_imported_config_revisions_key(),
        revisions={})
  some_dirty = False
  for path, (rev, conf) in sorted(configs.iteritems()):
    dirty = _CONFIG_SCHEMAS[path]['updater'](rev, conf)
    revs.revisions[path] = {'rev': rev.revision, 'url': rev.url}
    logging.info(
        'Processed %s at rev %s: %s', path, rev.revision,
        'updated' if dirty else 'up-to-date')
    some_dirty = some_dirty or dirty
  revs.put()
  if some_dirty:
    model.replicate_auth_db()


def _validate_ip_whitelist_config(conf):
  if not isinstance(conf, config_pb2.IPWhitelistConfig):
    raise ValueError('Wrong message type: %s' % conf.__class__.__name__)
  whitelists = set()
  for ip_whitelist in conf.ip_whitelists:
    if not model.IP_WHITELIST_NAME_RE.match(ip_whitelist.name):
      raise ValueError('Invalid IP whitelist name: %s' % ip_whitelist.name)
    if ip_whitelist.name in whitelists:
      raise ValueError('IP whitelist %s is defined twice' % ip_whitelist.name)
    whitelists.add(ip_whitelist.name)
    for net in ip_whitelist.subnets:
      # Raises ValueError if subnet is not valid.
      ipaddr.subnet_from_string(net)
  idents = []
  for assignment in conf.assignments:
    # Raises ValueError if identity is not valid.
    ident = model.Identity.from_bytes(assignment.identity)
    if assignment.ip_whitelist_name not in whitelists:
      raise ValueError(
          'Unknown IP whitelist: %s' % assignment.ip_whitelist_name)
    if ident in idents:
      raise ValueError('Identity %s is specified twice' % assignment.identity)
    idents.append(ident)


def _update_ip_whitelist_config(rev, conf):
  assert ndb.in_transaction(), 'Must be called in AuthDB transaction'
  now = utils.utcnow()

  # Existing whitelist entities.
  existing_ip_whitelists = {
    e.key.id(): e
    for e in model.AuthIPWhitelist.query(ancestor=model.root_key())
  }

  # Whitelists being imported (name => IPWhitelist proto msg).
  imported_ip_whitelists = {msg.name: msg for msg in conf.ip_whitelists}

  to_put = []
  to_delete = []

  # New or modified IP whitelists.
  for wl_proto in imported_ip_whitelists.itervalues():
    # Convert proto magic list to a regular list.
    subnets = list(wl_proto.subnets)
    # Existing whitelist and it hasn't changed?
    wl = existing_ip_whitelists.get(wl_proto.name)
    if wl and wl.subnets == subnets:
      continue
    # Update existing (to preserve auth_db_prev_rev) or create a new one.
    if not wl:
      wl = model.AuthIPWhitelist(
          key=model.ip_whitelist_key(wl_proto.name),
          created_ts=now,
          created_by=model.get_service_self_identity())
    wl.subnets = subnets
    wl.description = 'Imported from ip_whitelist.cfg at rev %s' % rev.revision
    to_put.append(wl)

  # Removed IP whitelists.
  for wl in existing_ip_whitelists.itervalues():
    if wl.key.id() not in imported_ip_whitelists:
      to_delete.append(wl)

  # Update assignments. Don't touch created_ts and created_by for existing ones.
  ip_whitelist_assignments = (
      model.ip_whitelist_assignments_key().get() or
      model.AuthIPWhitelistAssignments(
          key=model.ip_whitelist_assignments_key()))
  existing = {
    (a.identity.to_bytes(), a.ip_whitelist): a
    for a in ip_whitelist_assignments.assignments
  }
  updated = []
  for a in conf.assignments:
    key = (a.identity, a.ip_whitelist_name)
    if key in existing:
      updated.append(existing[key])
    else:
      new_one = model.AuthIPWhitelistAssignments.Assignment(
          identity=model.Identity.from_bytes(a.identity),
          ip_whitelist=a.ip_whitelist_name,
          comment='Imported from ip_whitelist.cfg at rev %s' % rev.revision,
          created_ts=now,
          created_by=model.get_service_self_identity())
      updated.append(new_one)

  # Something has changed?
  updated_keys = [
    (a.identity.to_bytes(), a.ip_whitelist)
    for a in updated
  ]
  if set(updated_keys) != set(existing):
    ip_whitelist_assignments.assignments = updated
    to_put.append(ip_whitelist_assignments)

  if not to_put and not to_delete:
    return False
  comment = 'Importing ip_whitelist.cfg at rev %s' % rev.revision
  for e in to_put:
    e.record_revision(
        modified_by=model.get_service_self_identity(),
        modified_ts=now,
        comment=comment)
  for e in to_delete:
    e.record_deletion(
        modified_by=model.get_service_self_identity(),
        modified_ts=now,
        comment=comment)
  futures = []
  futures.extend(ndb.put_multi_async(to_put))
  futures.extend(ndb.delete_multi_async(e.key for e in to_delete))
  for f in futures:
    f.check_success()
  return True


def _validate_oauth_config(conf):
  # Any correctly structured config is acceptable for now.
  if not isinstance(conf, config_pb2.OAuthConfig):
    raise ValueError('Wrong message type')


def _update_oauth_config(rev, conf):
  assert ndb.in_transaction(), 'Must be called in AuthDB transaction'
  existing = model.root_key().get()
  existing_as_dict = {
    'oauth_client_id': existing.oauth_client_id,
    'oauth_client_secret': existing.oauth_client_secret,
    'oauth_additional_client_ids': list(existing.oauth_additional_client_ids),
  }
  new_as_dict = {
    'oauth_client_id': conf.primary_client_id,
    'oauth_client_secret': conf.primary_client_secret,
    'oauth_additional_client_ids': list(conf.client_ids),
  }
  if new_as_dict == existing_as_dict:
    return False
  existing.populate(**new_as_dict)
  existing.record_revision(
      modified_by=model.get_service_self_identity(),
      modified_ts=utils.utcnow(),
      comment='Importing oauth.cfg at rev %s' % rev.revision)
  existing.put()
  return True


### Description of all known config files: how to validate and import them.

# Config file name -> {
#   'proto_class': protobuf class of the config or None to keep it as text,
#   'revision_getter': lambda: <latest imported Revision>,
#   'validator': lambda config: <raises ValueError on invalid format>
#   'updater': lambda rev, config: True if applied, False if not.
#   'use_authdb_transaction': True to call 'updater' in AuthDB transaction.
# }
_CONFIG_SCHEMAS = {
  'delegation.cfg': {
    'proto_class': None, # delegation configs are stored as text in datastore
    'revision_getter': lambda: _get_service_config_rev('delegation.cfg'),
    'updater': lambda rev, c: _update_service_config('delegation.cfg', rev, c),
    'use_authdb_transaction': False,
  },
  'imports.cfg': {
    'proto_class': None, # importer configs are stored as text
    'revision_getter': _get_imports_config_revision,
    'updater': _update_imports_config,
    'use_authdb_transaction': False,
  },
  'ip_whitelist.cfg': {
    'proto_class': config_pb2.IPWhitelistConfig,
    'revision_getter': lambda: _get_authdb_config_rev('ip_whitelist.cfg'),
    'updater': _update_ip_whitelist_config,
    'use_authdb_transaction': True,
  },
  'oauth.cfg': {
    'proto_class': config_pb2.OAuthConfig,
    'revision_getter': lambda: _get_authdb_config_rev('oauth.cfg'),
    'updater': _update_oauth_config,
    'use_authdb_transaction': True,
  },
}


@utils.memcache('auth_service:get_configs_url', time=300)
def _get_configs_url():
  """Returns URL where luci-config fetches configs from."""
  url = config.get_config_set_location(config.self_config_set())
  return url or 'about:blank'


def _fetch_configs(paths):
  """Fetches a bunch of config files in parallel and validates them.

  Returns:
    dict {path -> (Revision tuple, <config>)}.

  Raises:
    CannotLoadConfigError if some config is missing or invalid.
  """
  paths = sorted(paths)
  futures = [
    config.get_self_config_async(
        p, dest_type=_CONFIG_SCHEMAS[p]['proto_class'], store_last_good=False)
    for p in paths
  ]
  configs_url = _get_configs_url()
  ndb.Future.wait_all(futures)
  out = {}
  for path, future in zip(paths, futures):
    rev, conf = future.get_result()
    if conf is None:
      raise CannotLoadConfigError('Config %s is missing' % path)
    try:
      validation.validate(config.self_config_set(), path, conf)
    except ValueError as exc:
      raise CannotLoadConfigError(
          'Config %s at rev %s failed to pass validation: %s' %
          (path, rev, exc))
    out[path] = (Revision(rev, _gitiles_url(configs_url, rev, path)), conf)
  return out


def _gitiles_url(configs_url, rev, path):
  """URL to a directory in gitiles -> URL to a file at concrete revision."""
  try:
    location = gitiles.Location.parse(configs_url)
    return str(gitiles.Location(
        hostname=location.hostname,
        project=location.project,
        treeish=rev,
        path=posixpath.join(location.path, path)))
  except ValueError:
    # Not a gitiles URL, return as is.
    return configs_url

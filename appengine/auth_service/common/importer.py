# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Imports groups from some external tar.gz bundle.

External URL should serve *.tar.gz file with the following file structure:
  <external group system name>/<group name>:
    userid
    userid
    ...

For example ldap.tar.gz may look like:
  ldap/trusted-users:
    jane
    joe
    ...
  ldap/all:
    jane
    joe
    ...

Each tarball may have groups from multiple external systems, but groups from
some external system must not be split between multiple tarballs. When importer
sees <external group system name>/* in a tarball, it modifies group list from
that system on the server to match group list in the tarball _exactly_,
including removal of groups that are on the server, but no longer present in
the tarball.
"""

import collections
import contextlib
import logging
import StringIO
import tarfile

from google.appengine.api import app_identity
from google.appengine.ext import ndb

from components import auth
from components import utils
from components.auth import model


class BundleImportError(Exception):
  """Base class for errors while fetching external bundle."""


class BundleFetchError(BundleImportError):
  """Failed to fetch the archive from remote URL."""

  def __init__(self, url, status_code, content):
    super(BundleFetchError, self).__init__()
    self.url = url
    self.status_code = status_code
    self.content = content

  def __str__(self):
    return 'Request to %s failed with code %d:\n%s' % (
        self.url, self.status_code, self.content)


class BundleUnpackError(BundleImportError):
  """Failed to untar the archive."""

  def __init__(self, inner_exc):
    super(BundleUnpackError, self).__init__()
    self.inner_exc = inner_exc

  def __str__(self):
    return 'Not a valid tar archive: %s' % self.inner_exc


class BundleBadFormatError(BundleImportError):
  """Group file in bundle has invalid format."""

  def __init__(self, inner_exc):
    super(BundleBadFormatError, self).__init__()
    self.inner_exc = inner_exc

  def __str__(self):
    return 'Bundle contains invalid group file: %s' % self.inner_exc


def config_key():
  """Key of GroupImporterConfig singleton entity."""
  return ndb.Key('GroupImporterConfig', 'config')


class GroupImporterConfig(ndb.Model):
  """Singleton entity with group importer configuration JSON."""
  config = ndb.JsonProperty()
  modified_by = auth.IdentityProperty(indexed=False)
  modified_ts = ndb.DateTimeProperty(auto_now=True, indexed=False)


def is_valid_config(config):
  """Checks config for correctness."""
  if not isinstance(config, list):
    return False

  seen_systems = set()
  for item in config:
    if not isinstance(item, dict):
      return False

    # 'url' is a required string: where to fetch tar.gz from.
    url = item.get('url')
    if not url or not isinstance(url, basestring):
      return False

    # 'systems' is a required list of strings: group systems expected to be
    # found in the archive (they act as prefixes to group names, e.g 'ldap').
    systems = item.get('systems')
    if not systems or not isinstance(systems, list):
      return False
    if not all(isinstance(x, basestring) for x in systems):
      return False

    # There should be no overlap in systems between different bundles.
    if set(systems) & seen_systems:
      return False
    seen_systems.update(systems)

    # 'groups' is an optional list of strings: if given, filters imported groups
    # only to this list.
    groups = item.get('groups')
    if groups and not all(isinstance(x, basestring) for x in groups):
      return False

    # 'oauth_scopes' is an optional list of strings: used when generating OAuth
    # access_token to put in Authorization header.
    oauth_scopes = item.get('oauth_scopes')
    if oauth_scopes is not None:
      if not all(isinstance(x, basestring) for x in oauth_scopes):
        return False

    # 'domain' is an optional string: will be used when constructing emails from
    # naked usernames found in imported groups.
    domain = item.get('domain')
    if domain and not isinstance(domain, basestring):
      return False

  return True


def read_config():
  """Returns currently stored config or [] if not set."""
  e = config_key().get()
  return (e.config if e else []) or []


def write_config(config):
  """Updates stored configuration."""
  if not is_valid_config(config):
    raise ValueError('Invalid config')
  e = GroupImporterConfig(
      key=config_key(),
      config=config,
      modified_by=auth.get_current_identity())
  e.put()


def import_external_groups():
  """Refetches all external groups.

  Runs as a cron task.

  Returns:
    True on success, False if at least one import failed.
  """
  config = read_config()
  if not config:
    logging.info('Not configured')
    return True
  if not is_valid_config(config):
    logging.warning('Bad config')
    return False

  # Fetch all bundles specified in config in parallel.
  futures = [
    fetch_bundle_async(
        p['url'],
        p['systems'],
        p.get('groups'),
        p.get('oauth_scopes'),
        p.get('domain'))
    for p in config
  ]

  # {system name -> group name -> list of identities}
  bundles = {}
  errors = False
  for params, future in zip(config, futures):
    if future.get_exception():
      errors = True
      logging.error(
          'Failed to process %s: %s', params['url'], future.get_exception())
    else:
      fetched = future.get_result()
      # There should be no intersection between archives.
      assert not (set(fetched) & set(bundles)), (fetched.keys(), bundles.keys())
      bundles.update(fetched)

  # Nothing to process?
  if not bundles:
    return not errors

  @ndb.transactional
  def snapshot_groups():
    """Fetches all existing groups and AuthDB revision number."""
    groups = model.AuthGroup.query(ancestor=model.root_key()).fetch_async()
    return auth.get_auth_db_revision(), groups.get_result()

  @ndb.transactional
  def apply_import(revision, entities_to_put, keys_to_delete):
    """Transactionally puts and deletes a bunch of entities."""
    # DB changed between transactions, retry.
    if auth.get_auth_db_revision() != revision:
      return False
    # Apply mutations, bump revision number.
    futures = []
    futures.extend(ndb.put_multi_async(entities_to_put))
    futures.extend(ndb.delete_multi_async(keys_to_delete))
    ndb.Future.wait_all(futures)
    if any(f.get_exception() for f in futures):
      raise ndb.Rollback()
    auth.replicate_auth_db()
    return True

  # Try to apply the change until success or deadline. Split transaction into
  # two (assuming AuthDB changes infrequently) to avoid reading and writing too
  # much stuff from within a single transaction (and to avoid keeping the
  # transaction open while calculating the diff).
  while True:
    # Use same timestamp everywhere to reflect that groups were imported
    # atomically within a single transaction.
    ts = utils.utcnow()
    entities_to_put = []
    keys_to_delete = []
    revision, existing_groups = snapshot_groups()
    for system, groups in bundles.iteritems():
      to_put, to_delete = prepare_import(system, existing_groups, groups, ts)
      entities_to_put.extend(to_put)
      keys_to_delete.extend(to_delete)
    if not entities_to_put and not keys_to_delete:
      break
    if apply_import(revision, entities_to_put, keys_to_delete):
      break
  logging.info('Groups updated: %d', len(entities_to_put) + len(keys_to_delete))
  return not errors


@ndb.tasklet
def fetch_bundle_async(url, systems, groups, oauth_scopes, domain):
  """Fetches and extracts group files from  *.tar.gz bundle.

  Args:
    url: url to *.tar.gz file.
    systems: names of external group systems expected to be in the bundle.
    groups: list of group name to extract, or None to extract all.
    oauth_scopes: list of OAuth scopes to use when generating access_token for
        accessing |url|, if not set or empty - do not use OAuth.
    domain: email domain to append to naked user ids.

  Returns:
    Dict {system name -> {group name -> list of identities}}.

  Raises:
    BundleImportError on fetch or extraction errors.
  """
  if utils.is_local_dev_server():
    protocols = ('http://', 'https://')
  else:
    protocols = ('https://',)
  assert url.startswith(protocols), url

  headers = {}
  if oauth_scopes:
    headers['Authorization'] = 'OAuth %s' % (
        app_identity.get_access_token(oauth_scopes)[0])

  ctx = ndb.get_context()
  result = yield ctx.urlfetch(
      url=url,
      method='GET',
      headers=headers,
      follow_redirects=False,
      deadline=5*60,
      validate_certificate=True)
  if result.status_code != 200:
    raise BundleFetchError(url, result.status_code, result.content)

  # System name (e.g. 'ldap') -> full group name -> list of identities.
  bundles = collections.defaultdict(dict)
  try:
    # Expected filenames are <external system name>/<group name>, skip
    # everything else.
    for filename, fileobj in extract_tar_archive(result.content):
      chunks = filename.split('/')
      if len(chunks) != 2 or not auth.is_valid_group_name(filename):
        logging.warning('Skipping file %s, not a valid name', filename)
        continue
      if groups is not None and filename not in groups:
        continue
      system = chunks[0]
      if system not in systems:
        logging.warning('Skipping file %s, not allowed', filename)
        continue
      # Reject the whole bundle if at least one group file is broken. That way
      # all existing groups will stay intact. Simply ignoring broken group here
      # will cause the importer to remove it completely.
      try:
        bundles[system][filename] = parse_group_file(fileobj.read(), domain)
      except ValueError as err:
        raise BundleBadFormatError(err)
  except tarfile.TarError as exc:
    raise BundleUnpackError('Not a valid tar archive: %s' % exc)
  raise ndb.Return(dict(bundles.iteritems()))


def extract_tar_archive(content):
  """Given a body of tar.gz file yields pairs (file name, file obj)."""
  stream = StringIO.StringIO(content)
  with tarfile.open(mode='r|gz', fileobj=stream) as tar:
    for item in tar:
      if item.isreg():
        with contextlib.closing(tar.extractfile(item)) as extracted:
          yield item.name, extracted


def parse_group_file(body, domain):
  """Given body of imported group file returns list of Identities.

  Raises ValueError if group file is malformed.
  """
  members = []
  for uid in body.splitlines():
    # This raises ValueError if uid is not correct.
    ident = auth.Identity(
        auth.IDENTITY_USER,
        '%s@%s' % (uid, domain) if domain else uid)
    members.append(ident)
  return sorted(members, key=lambda x: x.to_bytes())


def prepare_import(system_name, existing_groups, imported_groups, timestamp):
  """Prepares lists of entities to put and delete to apply group import.

  Args:
    system_name: name of external groups system being imported (e.g. 'ldap'),
      all existing groups belonging to that system will be replaced with
      |imported_groups|.
    existing_groups: ALL existing groups.
    imported_groups: dict {imported group name -> list of identities}.
    timestamp: modification timestamp to set on all touched entities.

  Returns:
    (List of entities to put, list of keys to delete).
  """
  # Return values of this function.
  to_put = []
  to_delete = []

  # Pick only groups that belong to |system_name|.
  system_groups = {
    g.key.id(): g for g in existing_groups
    if g.key.id().startswith('%s/' % system_name)
  }

  def clear_group(group_name):
    ent = system_groups[group_name]
    if ent.members:
      ent.members = []
      ent.modified_ts = timestamp
      ent.modified_by = auth.get_service_self_identity()
      to_put.append(ent)

  def delete_group(group_name):
    to_delete.append(system_groups[group_name].key)

  def create_group(group_name):
    ent = model.AuthGroup(
        key=model.group_key(group_name),
        members=imported_groups[group_name],
        created_ts=timestamp,
        created_by=auth.get_service_self_identity(),
        modified_ts=timestamp,
        modified_by=auth.get_service_self_identity())
    to_put.append(ent)

  def update_group(group_name):
    existing = system_groups[group_name]
    imported = imported_groups[group_name]
    if existing.members != imported:
      existing.members = imported
      existing.modified_ts = timestamp
      existing.modified_by = auth.get_service_self_identity()
      to_put.append(existing)

  # Delete groups that are no longer present in the bundle. If group is
  # referenced somewhere, just clear its members list (to avoid creating
  # inconsistency in group inclusion graph).
  for group_name in (set(system_groups) - set(imported_groups)):
    if any(group_name in g.nested for g in existing_groups):
      clear_group(group_name)
    else:
      delete_group(group_name)

  # Create new groups.
  for group_name in (set(imported_groups) - set(system_groups)):
    create_group(group_name)

  # Update existing groups.
  for group_name in (set(imported_groups) & set(system_groups)):
    update_group(group_name)

  return to_put, to_delete

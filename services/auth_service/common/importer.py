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

import logging

from google.appengine.ext import ndb

from components import auth


# Key of GroupImporterConfig singleton entity.
CONFIG_KEY = ndb.Key('GroupImporterConfig', 'config')


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
  e = CONFIG_KEY.get()
  return (e.config if e else []) or []


def write_config(config):
  """Updates stored configuration."""
  if not is_valid_config(config):
    raise ValueError('Invalid config')
  e = GroupImporterConfig(
      key=CONFIG_KEY,
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
  # TODO(vadimsh): Implement.
  return True

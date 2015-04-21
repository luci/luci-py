# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Imports config files stored in Gitiles.

If services_config_location is set in admin.GlobalConfig root entity,
each directory in the location is imported as services/<directory_name>.

For each project defined in the project registry with
config_storage_type == Gitiles, projects/<project_id> config set is imported
from project.config_location.
"""

import contextlib
import logging
import os
import re
import StringIO
import tarfile

from google.appengine.api import urlfetch_errors
from google.appengine.ext import ndb
from google.protobuf import text_format

from components import auth
from components import gitiles
from components import net
from components.datastore_utils import txn

from proto import service_config_pb2
import admin
import projects
import storage
import validation


DEFAULT_GITILES_IMPORT_CONFIG = service_config_pb2.ImportCfg.Gitiles(
    fetch_log_deadline=15,
    fetch_archive_deadline=15,
    project_config_default_ref='refs/heads/luci',
    project_config_default_path='/',
    ref_config_default_path='luci',
)


def get_gitiles_config():
  cfg = service_config_pb2.ImportCfg(gitiles=DEFAULT_GITILES_IMPORT_CONFIG)
  try:
    cfg = storage.get_self_config('import.cfg', lambda: cfg)
  except text_format.ParseError as ex:
    # It is critical that get_gitiles_config() returns a valid config.
    # If import.cfg is broken, it should not break importing mechanism,
    # otherwise the system won't be able to heal itself by importing a fixed
    # config.
    logging.exception('import.cfg is broken')
  return cfg.gitiles


def import_revision(config_set, location, create_config_set=False):
  """Imports a referenced Gitiles revision into a config set.

  If |create_config_set| is True and Revision entity does not exist,
  then creates ConfigSet with latest_revision set to |location.treeish|.
  """
  assert re.match('[0-9a-f]{40}', location.treeish), (
      '"%s" is not a valid sha' % location.treeish
  )
  logging.debug('Importing revision %s:%s', config_set, location.treeish)
  rev_key = ndb.Key(
      storage.ConfigSet, config_set,
      storage.Revision, location.treeish)
  if rev_key.get():
    if create_config_set:
      storage.ConfigSet(id=config_set, latest_revision=location.treeish).put()
    return

  # Fetch archive, extract files and save them to Blobs outside ConfigSet
  # transaction.
  archive = location.get_archive(
      deadline=get_gitiles_config().fetch_archive_deadline)
  if not archive:
    logging.error(
        'Could not import %s: configuration does not exist', config_set)
    return

  logging.info('%s archive size: %d bytes' % (config_set, len(archive)))

  entites_to_put = [storage.Revision(key=rev_key)]
  if create_config_set:
    entites_to_put.append(
        storage.ConfigSet(id=config_set, latest_revision=location.treeish))

  stream = StringIO.StringIO(archive)
  blob_futures = []
  with tarfile.open(mode='r|gz', fileobj=stream) as tar:
    for item in tar:
      if not item.isreg():  # pragma: no cover
        continue
      with contextlib.closing(tar.extractfile(item)) as extracted:
        content = extracted.read()
        if not validation.validate_config(
            config_set, item.name, content, log_errors=True):
          logging.error('Invalid revision: %s/%s', config_set, location.treeish)
          return
        content_hash = storage.compute_hash(content)
        blob_futures.append(storage.import_blob_async(
            content=content, content_hash=content_hash))
        entites_to_put.append(
            storage.File(
                id=item.name,
                parent=rev_key,
                content_hash=content_hash)
        )

  # Wait for Blobs to be imported before proceeding.
  ndb.Future.wait_all(blob_futures)

  @ndb.transactional
  def do_import():
    if not rev_key.get():
      ndb.put_multi(entites_to_put)

  do_import()
  logging.info('Imported revision %s/%s', config_set, location.treeish)


def import_config_set(config_set, location):
  """Imports the latest version of config set from a Gitiles location.

  Args:
    config_set (str): name of a config set to import.
    location (gitiles.Location): location of the config set.
  """
  assert config_set
  assert location
  try:
    logging.debug('Importing %s from %s', config_set, location)

    log = location.get_log(
        limit=1, deadline=get_gitiles_config().fetch_log_deadline)
    if not log or not log.commits:
      logging.warning('Could not load commit log for %s', location)
      return
    commit = log.commits[0]

    config_set_key = ndb.Key(storage.ConfigSet, config_set)
    config_set_entity = config_set_key.get()
    if config_set_entity and config_set_entity.latest_revision == commit.sha:
      logging.debug('Config set %s is up to date', config_set)
      return

    commit_location = location._replace(treeish=commit.sha)
    # ConfigSet.latest_revision needs to be updated in the same transaction as
    # Revision. Assume ConfigSet.latest_revision is the only attribute and
    # use import_revision's create_config_set parameter to set latest_revision.
    import_revision(config_set, commit_location, create_config_set=True)
  except urlfetch_errors.DeadlineExceededError:
    logging.error(
        'Could not import config set %s from %s: urlfetch deadline exceeded',
        config_set, location)
  except net.AuthError:
    # TODO(nodir): change to error when luci-config has access to internal
    # repos.
    logging.warning(
        'Could not import config set %s from %s: permission denied',
        config_set, location)


def import_services(location_root):
  assert location_root
  tree = location_root.get_tree()

  for service_entry in tree.entries:
    service_id = service_entry.name
    if not validation.is_valid_service_id(service_id):
      logging.warning('Invalid service id: %s', service_id)
      continue
    service_location = location_root._replace(
        path=os.path.join(location_root.path, service_entry.name))
    import_config_set('services/%s' % service_id, service_location)


def import_project(project_id, location):
  cfg = get_gitiles_config()

  # Adjust location
  treeish = location.treeish
  if not treeish or treeish == 'HEAD':
    treeish = cfg.project_config_default_ref
  location = location._replace(
      treeish=treeish,
      path=location.path.strip('/') or cfg.project_config_default_path,
  )

  # Update project repo info.
  repo_url = str(location._replace(treeish=None, path=None))
  projects.update_import_info(
      project_id, projects.RepositoryType.GITILES, repo_url)

  import_config_set('projects/%s' % project_id, location)

  # Import refs
  for ref in projects.get_refs(project_id):
    assert ref.name
    assert ref.name.startswith('refs/'), ref.name
    ref_location = location._replace(
        treeish=ref.name,
        path=ref.config_path or cfg.ref_config_default_path,
    )
    import_config_set(
        'projects/%s/%s' % (project_id, ref.name), ref_location)


def import_projects():
  """Imports project configs that are stored in Gitiles."""
  for project in projects.get_projects():
    if project.config_storage_type != service_config_pb2.Project.GITILES:
      continue
    try:
      location = gitiles.Location.parse_resolve(project.config_location)
    except ValueError:
      logging.exception('Invalid project location: %s', project.config_location)
      continue

    try:
      import_project(project.id, location)
    except Exception as ex:
      logging.exception('Could not import project %s', project.id)


def cron_run_import():  # pragma: no cover
  conf = admin.GlobalConfig.fetch()
  gitiles_type = admin.ServiceConfigStorageType.GITILES
  if (conf and conf.services_config_storage_type == gitiles_type and
      conf.services_config_location):
    loc = gitiles.Location.parse_resolve(conf.services_config_location)
    import_services(loc)
  import_projects()

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

from components import config
from components import gitiles
from components import net
from components.config.proto import service_config_pb2

import admin
import common
import notifications
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


def _commit_to_revision_info(commit, location):
  if commit is None:
    return None
  url = ''
  if location:
    url = str(location._replace(treeish=commit.sha))
  return storage.RevisionInfo(
      id=commit.sha,
      url=url,
      committer_email=commit.committer.email,
      time=commit.committer.time,
  )


def get_gitiles_config():
  cfg = service_config_pb2.ImportCfg(gitiles=DEFAULT_GITILES_IMPORT_CONFIG)
  try:
    cfg = storage.get_self_config_async(
        common.IMPORT_FILENAME, lambda: cfg).get_result()
  except text_format.ParseError as ex:
    # It is critical that get_gitiles_config() returns a valid config.
    # If import.cfg is broken, it should not break importing mechanism,
    # otherwise the system won't be able to heal itself by importing a fixed
    # config.
    logging.exception('import.cfg is broken')
  return cfg.gitiles


def import_revision(config_set, base_location, commit):
  """Imports a referenced Gitiles revision into a config set.

  |base_location| will be used to set storage.ConfigSet.location.

  Updates last ImportAttempt for the config set.

  If Revision entity does not exist, then creates ConfigSet initialized from
  arguments.
  """
  revision = commit.sha
  assert re.match('[0-9a-f]{40}', revision), (
      '"%s" is not a valid sha' % revision
  )
  logging.debug('Importing revision %s @ %s', config_set, revision)
  rev_key = ndb.Key(
      storage.ConfigSet, config_set,
      storage.Revision, revision)

  location = base_location._replace(treeish=revision)
  attempt = storage.ImportAttempt(
      key=storage.last_import_attempt_key(config_set),
      revision=_commit_to_revision_info(commit, location))
  if rev_key.get():
    attempt.success = True
    attempt.message = 'Up-to-date'
    attempt.put()
    return

  rev_entities = [
    storage.ConfigSet(
        id=config_set,
        latest_revision=revision,
        latest_revision_url=str(location),
        latest_revision_committer_email=commit.committer.email,
        latest_revision_time=commit.committer.time,
        location=str(base_location),
    ),
    storage.Revision(key=rev_key),
  ]

  # Fetch archive outside ConfigSet transaction.
  archive = location.get_archive(
      deadline=get_gitiles_config().fetch_archive_deadline)
  if not archive:
    logging.warning(
        'Configuration %s does not exist. Probably it was deleted', config_set)
    attempt.success = True
    attempt.message = 'Config directory not found. Imported as empty'
  else:
    # Extract files and save them to Blobs outside ConfigSet transaction.
    files, validation_result = _read_and_validate_archive(
        config_set, rev_key, archive)
    if validation_result.has_errors:
      logging.warning('Invalid revision %s@%s', config_set, revision)
      notifications.notify_gitiles_rejection(
          config_set, location, validation_result)

      attempt.success = False
      attempt.message = 'Validation errors'
      attempt.validation_messages = [
        storage.ImportAttempt.ValidationMessage(
            severity=config.Severity.lookup_by_number(m.severity),
            text=m.text,
        )
        for m in validation_result.messages
      ]
      attempt.put()
      return
    rev_entities += files
    attempt.success = True
    attempt.message = 'Imported'

  @ndb.transactional
  def txn():
    if not rev_key.get():
      ndb.put_multi(rev_entities)
    attempt.put()

  txn()
  logging.info('Imported revision %s/%s', config_set, location.treeish)


def _read_and_validate_archive(config_set, rev_key, archive):
  """Reads an archive, validates all files, imports blobs and returns files.

  If all files are valid, saves contents to Blob entities and returns
  files with their hashes.

  Return:
      (files, validation_result) tuple.
  """
  logging.info('%s archive size: %d bytes' % (config_set, len(archive)))

  stream = StringIO.StringIO(archive)
  blob_futures = []
  with tarfile.open(mode='r|gz', fileobj=stream) as tar:
    files = {}
    ctx = config.validation.Context()
    for item in tar:
      if not item.isreg():  # pragma: no cover
        continue
      with contextlib.closing(tar.extractfile(item)) as extracted:
        content = extracted.read()
        files[item.name] = content
        validation.validate_config(config_set, item.name, content, ctx=ctx)

  if ctx.result().has_errors:
    return [], ctx.result()

  entities = []
  for name, content in files.iteritems():
    content_hash = storage.compute_hash(content)
    blob_futures.append(storage.import_blob_async(
      content=content, content_hash=content_hash))
    entities.append(
      storage.File(
        id=name,
        parent=rev_key,
        content_hash=content_hash)
    )
  # Wait for Blobs to be imported before proceeding.
  ndb.Future.wait_all(blob_futures)
  return entities, ctx.result()


def import_config_set(config_set, location):
  """Imports the latest version of config set from a Gitiles location.

  Args:
    config_set (str): name of a config set to import.
    location (gitiles.Location): location of the config set.
  """
  assert config_set
  assert location

  commit = None
  def save_attempt(success, msg):
    storage.ImportAttempt(
      key=storage.last_import_attempt_key(config_set),
      revision=_commit_to_revision_info(commit, location),
      success=success,
      message=msg,
    ).put()

  try:
    logging.debug('Importing %s from %s', config_set, location)

    log = location.get_log(
        limit=1, deadline=get_gitiles_config().fetch_log_deadline)
    if not log or not log.commits:
      save_attempt(False, 'Could not load commit log')
      logging.warning('Could not load commit log for %s', location)
      return
    commit = log.commits[0]

    config_set_key = ndb.Key(storage.ConfigSet, config_set)
    config_set_entity = config_set_key.get()
    if config_set_entity and config_set_entity.latest_revision == commit.sha:
      save_attempt(True, 'Up-to-date')
      logging.debug('Config set %s is up-to-date', config_set)
      return

    import_revision(config_set, location, commit)
  except urlfetch_errors.DeadlineExceededError:
    save_attempt(False, 'Could not import: deadline exceeded')
    logging.error(
        'Could not import config set %s from %s: urlfetch deadline exceeded',
        config_set, location)
  except net.AuthError:
    save_attempt(False, 'Could not import: permission denied')
    logging.error(
        'Could not import config set %s from %s: permission denied',
        config_set, location)


def import_services(location_root):
  # TODO(nodir): import services from location specified in services.cfg
  assert location_root
  tree = location_root.get_tree()

  for service_entry in tree.entries:
    service_id = service_entry.name
    if service_entry.type != 'tree':
      continue
    if not config.validation.is_valid_service_id(service_id):
      logging.error('Invalid service id: %s', service_id)
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
    loc = project.config_location
    if loc.storage_type != service_config_pb2.ConfigSetLocation.GITILES:
      continue
    try:
      location = gitiles.Location.parse_resolve(loc.url)
    except ValueError:
      logging.exception('Invalid project location: %s', project.config_location)
      continue
    except net.AuthError as ex:
      logging.error(
          'Could not resolve %s due to permissions: %s',
          project.config_location, ex.message)
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

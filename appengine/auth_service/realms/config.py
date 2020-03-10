# Copyright 2020 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Loading and interpretation of realms.cfg config files.

The primary purpose of the logic here is to keep datastore entities that are
part of AuthDB entity group up-to-date whenever external (realms.cfg) or
internal (permissions.DB) configs change.

refetch_config() is the actuator that makes sure that AuthDB entity group is
eventually fully up-to-date. It may do so through multiple separate commits if
necessary. Each commit produces a new AuthDB revision.
"""

import collections
import functools
import logging
import random
import time

from google.appengine.api import datastore_errors
from google.appengine.ext import ndb
from google.appengine.runtime import apiproxy_errors

from components.auth import model

from realms import permissions
from realms import validation

import replication


# Register the config validation hook.
validation.register()


# Information about fetched or previously processed realms.cfg.
#
# Comes either from LUCI Config (then `config_body` is set, but `db_rev` isn't)
# or from the datastore (then `db_rev` is set, but `config_body` isn't). All
# other fields are always set.
RealmsCfgRev = collections.namedtuple(
    'RealmsCfgRev',
    [
        'project_id',     # ID of the project in LUCI Config
        'config_rev',     # the revision the config was fetched from, FYI
        'config_digest',  # digest of the raw config body

        # These two are mutually exclusive, and one MUST be non-None.
        'config_body', # byte blob with the fetched config
        'db_rev',      # revision of the permissions DB used
    ])


# How many AuthDB revisions to produce when permission.DB changes (e.g. when
# a new permission is added to an existing role).
DB_REEVAL_REVISIONS = 10


def refetch_config():
  """Called periodically in a cron job to import changes into the AuthDB.

  Returns:
    True on success, False on partial success or failure.
  """
  jobs = []
  db = permissions.db()

  # If db.permissions has changed, we need to propagate changes into the AuthDB.
  jobs.extend(check_permission_changes(db))

  # If either realms.cfg in one or more projects has changed, or the expansion
  # of roles into permissions has changed, we need to update flat expanded
  # realms representation in the AuthDB as well.
  latest = get_latest_revs_async()  # pylint: disable=assignment-from-no-return
  stored = get_stored_revs_async()  # pylint: disable=assignment-from-no-return
  jobs.extend(
      check_config_changes(db, latest.get_result(), stored.get_result()))

  # Land all scheduled transactions, sleeping between them for 2 sec. No rush.
  return execute_jobs(jobs, 2.0)


def execute_jobs(jobs, txn_sleep_time):
  """Executes all jobs one by one, sleeping between them.

  This gives the datastore some time to "land" transactions. It's not a
  guarantee, but just a best effort attempt to avoid contention. If it
  happens, refetch_config() will resume unfinished work on the next iteration.

  There should be no casual dependencies between jobs. Even through they will
  be executed sequentially, some may fail, and this does NOT stop processing
  of subsequent jobs.

  Args:
    txn_sleep_time: how long to sleep between jobs.

  Returns:
    True if all jobs succeeded, False if at least one failed.
  """
  success = True
  for idx, job in enumerate(jobs):
    if idx:
      time.sleep(txn_sleep_time)
    try:
      job()
    except (
          apiproxy_errors.Error,
          datastore_errors.Error,
          replication.ReplicationTriggerError) as exc:
      logging.error(
          'Failed, will try again later: %s - %s',
          exc.__class__.__name__, exc)
      success = False
  return success


def check_permission_changes(db):
  """Returns jobs to update permissions list stored in the AuthDB.

  The AuthDB distributed to all services contains a list of all defined
  permissions. This list is a superset of permissions referenced by all realms.
  In particular, it may have entries that are not yet used in any realm.
  Downstream services are still interested in seeing them (for example, to
  compare with the list of permissions the service is interested in checking,
  to catch typos and misconfigurations).

  Args:
    db: a permissions.DB instance with current permissions and roles.

  Returns:
    A list of parameterless callbacks.
  """
  perms_to_map = lambda perms: {p.name: p for p in perms}

  stored = model.realms_globals_key().get()
  if stored and perms_to_map(stored.permissions) == db.permissions:
    return []  # permissions in the AuthDB are up to date

  logging.info('Updating permissions in AuthDB to rev "%s"', db.revision)

  @ndb.transactional
  def update_stored():
    stored = model.realms_globals_key().get()
    if not stored:
      stored = model.AuthRealmsGlobals(key=model.realms_globals_key())
    if perms_to_map(stored.permissions) == db.permissions:
      logging.info('Skipping, already up-to-date')
      return
    stored.permissions = sorted(db.permissions.values(), key=lambda p: p.name)
    stored.record_revision(
        modified_by=model.get_service_self_identity(),
        comment='Updating permissions to rev "%s"' % db.revision)
    stored.put()
    model.replicate_auth_db()

  return [update_stored]


def check_config_changes(db, latest, stored):
  """Yields jobs to update the AuthDB based on detected realms.cfg changes.

  Args:
    db: a permissions.DB instance with current permissions and roles.
    latest: a list of RealmsCfgRev with all fetched configs.
    stored: a list of RealmsCfgRev representing all currently applied configs.

  Yields:
    A list of parameterless callbacks.
  """
  latest_map = {r.project_id: r for r in latest}
  stored_map = {r.project_id: r for r in stored}

  assert len(latest_map) == len(latest)
  assert len(stored_map) == len(stored)

  # Shuffling helps to progress if one of the configs is somehow very
  # problematic (e.g. causes OOM). When the cron job is repeatedly retried, all
  # healthy configs will eventually be processed before the problematic ones.
  latest = latest[:]
  random.shuffle(latest)

  # List of RealmsCfgRev we'll need to reevaluate because they were generated
  # with stale db.revision.
  reeval = []

  # Detect changed realms.cfg and ones that need reevaluation.
  for rev in latest:
    cur = stored_map.get(rev.project_id)
    if not cur or cur.config_digest != rev.config_digest:
      yield functools.partial(update_realms, db, [rev])  # the body has changed
    elif cur.db_rev != db.revision:
      reeval.append(rev)  # was evaluated with potentially stale roles

  # Detect realms.cfg that were removed completely.
  for rev in stored:
    if rev.project_id not in latest_map:
      yield functools.partial(delete_realms, rev.project_id)

  # Changing the permissions DB (e.g. adding a new permission to a widely used
  # role) may affect ALL projects. In this case generating a ton of AuthDB
  # revisions is wasteful. We could try to generate a single giant revision, but
  # it may end up being too big, hitting datastore limits. So we "heuristically"
  # split it into DB_REEVAL_REVISIONS revisions, hoping for the best.
  batch_size = max(1, len(reeval) // DB_REEVAL_REVISIONS)
  for i in range(0, len(reeval), batch_size):
    yield functools.partial(update_realms, db, reeval[i:i+batch_size])


@ndb.tasklet
def get_latest_revs_async():
  """Returns a list of all current RealmsCfgRev by querying LUCI Config."""
  # TODO(vadimsh): Implement via components.config API.
  raise ndb.Return([])


@ndb.tasklet
def get_stored_revs_async():
  """Returns a list of all stored RealmsCfgRev based on data in the AuthDB."""
  # TODO(vadimsh): Implement via a strongly consistent AuthDB datastore query.
  raise ndb.Return([])


def update_realms(db, revs):
  """Performs an AuthDB transaction that updates realms of some projects.

  It interprets realms.cfg, expanding them into an internal flat representation
  (using rules in `db`), and puts them into the AuthDB (if not already there).

  Args:
    db: a permissions.DB instance with current permissions and roles.
    revs: a list of RealmsCfgRev with fetched configs to reevaluate.
  """
  # TODO(vadimsh): Implement:
  #   1. Parse all text protos.
  #   2. Expand them into realms_pb2.Realms based on rules in `db`.
  #   3. In an AuthDB transaction visit all related ExpandedRealms entities and:
  #     a. If realms_pb2.Realms there is already up-to-date, just update
  #        associated config_rev, config_digest, db_rev (to avoid revisiting
  #        this config again).
  #     b. If realms_pb2.Realms is stale, update it (and all associated
  #        metadata). Remember this.
  #     c. If some realms_pb2.Realms were indeed updated, trigger AuthDB
  #        replication. Do NOT trigger it if we updated only bookkeeping
  #        metadata.
  _ = db
  _ = revs


def delete_realms(project_id):
  """Performs an AuthDB transaction that deletes all realms of some project.

  Args:
    project_id: ID of the project being deleted.
  """
  # TODO(vadimsh): Implement. Transactionally:
  #   1. Check ExpandedRealms(project_id) entity still exists.
  #   2. Delete it and trigger the replication if it does, noop if doesn't.
  _ = project_id

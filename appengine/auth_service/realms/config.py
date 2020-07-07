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
import hashlib
import logging
import random
import time

from google.protobuf import text_format

from google.appengine.api import datastore_errors
from google.appengine.ext import ndb
from google.appengine.runtime import apiproxy_errors

from components import config
from components import utils
from components.auth import model

from proto import realms_config_pb2

from realms import common
from realms import permissions
from realms import rules
from realms import validation

import replication


# Register the config validation hook.
validation.register()


# Information about fetched or previously processed realms.cfg.
#
# Comes either from LUCI Config (then `config_body` is set, but `perms_rev`
# isn't) or from the datastore (then `perms_rev` is set, but `config_body`
# isn't). All other fields are always set.
RealmsCfgRev = collections.namedtuple(
    'RealmsCfgRev',
    [
        'project_id',     # ID of the project in LUCI Config
        'config_rev',     # the revision the config was fetched from, FYI
        'config_digest',  # digest of the raw config body

        # These two are mutually exclusive, and one MUST be non-None.
        'config_body',  # byte blob with the fetched config
        'perms_rev',    # revision of the permissions DB used
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
      yield functools.partial(
          update_realms, db, [rev],
          'Realms config rev "%s"' % rev.config_rev)
    elif cur.perms_rev != db.revision:
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
    yield functools.partial(
        update_realms, db, reeval[i:i+batch_size],
        'Permissions rev "%s"' % db.revision)


@ndb.tasklet
def get_latest_revs_async():
  """Returns a list of all current RealmsCfgRev by querying LUCI Config."""

  # In parallel load all project realms (from projects' config sets) and
  # internal realms (from the service config set).
  #
  # Per the config client library API, here
  #   `configs` is {project_id -> (rev, body, exc)}, where `exc` is always None.
  #   `internal` is (rev, body) where (None, None) indicates "no such config".
  configs, internal = yield (
      config.get_project_configs_async(common.cfg_path()),
      config.get_self_config_async(common.cfg_path(), store_last_good=False),
  )

  # Pretend internal realms came from special "@internal" project. Such project
  # name is forbidden by LUCI Config, so there should be no confusion.
  if common.INTERNAL_PROJECT in configs:
    raise ValueError('Unexpected LUCI project %s' % common.INTERNAL_PROJECT)
  internal_rev, internal_body = internal
  if internal_body:
    configs[common.INTERNAL_PROJECT] = (internal_rev, internal_body, None)

  # Convert the result to a list of RealmsCfgRev in no particular order.
  out = []
  for project_id, (rev, body, exc) in configs.items():
    # Errors are impossible when when not specifying 2nd parameter of
    # get_project_configs_async.
    assert body is not None
    assert exc is None
    out.append(RealmsCfgRev(
        project_id=project_id,
        config_rev=rev or 'unknown',
        config_digest=hashlib.sha256(body).hexdigest(),
        config_body=body,
        perms_rev=None,
    ))
  raise ndb.Return(out)


@ndb.tasklet
def get_stored_revs_async():
  """Returns a list of all stored RealmsCfgRev based on data in the AuthDB."""
  out = []
  metas = yield AuthProjectRealmsMeta.query(
      ancestor=model.root_key()).fetch_async()
  for meta in metas:
    out.append(RealmsCfgRev(
        project_id=meta.project_id,
        config_rev=meta.config_rev,
        config_digest=meta.config_digest,
        config_body=None,
        perms_rev=meta.perms_rev,
    ))
  raise ndb.Return(out)


def update_realms(db, revs, comment):
  """Performs an AuthDB transaction that updates realms of some projects.

  It interprets realms.cfg, expanding them into an internal flat representation
  (using rules in `db`), and puts them into the AuthDB (if not already there).

  Has verbose logging inside, since this function operates with potentially huge
  proto messages which GAE Python runtime is known to have issues with.

  Args:
    db: a permissions.DB instance with current permissions and roles.
    revs: a list of RealmsCfgRev with fetched configs to reevaluate.
    comment: a comment for the AuthDB log.
  """
  expanded = []  # list of (RealmsCfgRev, realms_pb2.Realms)

  for r in revs:
    logging.info('Expanding realms of project "%s"...', r.project_id)
    start = time.time()

    try:
      parsed = realms_config_pb2.RealmsCfg()
      text_format.Merge(r.config_body, parsed)
      expanded.append((r, rules.expand_realms(db, r.project_id, parsed)))
    except (text_format.ParseError, ValueError) as exc:
      # We end up here if realms.cfg could not be parsed or it fails validation.
      # This logging line should surface in ereporter2.
      logging.error('Failed to process realms of "%s": %s', r.project_id, exc)

    # Cheesy, but effective. These errors should surface in ereporter2 alerts.
    # We can't really setup effective time series metrics since this code path
    # is hit very infrequently (only when configs change, so only a few times
    # per day).
    dt = time.time() - start
    if dt > 5.0:
      logging.error('Realms expansion of "%s" is slow: %1.fs', r.project_id, dt)

  if not expanded:
    return

  @ndb.transactional
  def update():
    existing = ndb.get_multi(
        model.project_realms_key(rev.project_id)
        for rev, _ in expanded
    )

    updated = []
    metas = []

    for (rev, realms), ent in zip(expanded, existing):
      logging.info('Visiting project "%s"...', rev.project_id)
      if not ent:
        logging.info('New realms config in project "%s"', rev.project_id)
        ent = model.AuthProjectRealms(
            key=model.project_realms_key(rev.project_id),
            realms=realms,
            config_rev=rev.config_rev,
            perms_rev=db.revision)
        ent.record_revision(
            modified_by=model.get_service_self_identity(),
            comment='New realms config')
        updated.append(ent)
      elif ent.realms != realms:
        logging.info('Updated realms config in project "%s"', rev.project_id)
        ent.realms = realms
        ent.config_rev = rev.config_rev
        ent.perms_rev = db.revision
        ent.record_revision(
            modified_by=model.get_service_self_identity(),
            comment=comment)
        updated.append(ent)
      else:
        logging.info('Realms config in project "%s" are fresh', rev.project_id)

      # Always update AuthProjectRealmsMeta to match the state we just checked.
      metas.append(AuthProjectRealmsMeta(
          key=project_realms_meta_key(rev.project_id),
          config_rev=rev.config_rev,
          perms_rev=db.revision,
          config_digest=rev.config_digest,
          modified_ts=utils.utcnow(),
      ))

    logging.info('Persisting changes...')
    ndb.put_multi(updated + metas)
    if updated:
      model.replicate_auth_db()

  logging.info('Entering the transaction...')
  update()
  logging.info('Transaction landed')


@ndb.transactional
def delete_realms(project_id):
  """Performs an AuthDB transaction that deletes all realms of some project.

  Args:
    project_id: ID of the project being deleted.
  """
  realms = model.project_realms_key(project_id).get()
  if not realms:
    return  # already gone
  realms.record_deletion(
      modified_by=model.get_service_self_identity(),
      comment='No longer in the configs')
  realms.key.delete()
  project_realms_meta_key(project_id).delete()
  model.replicate_auth_db()


class AuthProjectRealmsMeta(ndb.Model):
  """Metadata of some AuthProjectRealms entity.

  Always created/deleted/updated transactionally with the corresponding
  AuthProjectRealms entity, but it is not a part of AuthDB itself (i.e.
  components.auth doesn't know about this entity and never fetches it).

  Used to hold bookkeeping state related to realms.cfg processing. Can be
  fetched very efficiently (compared to fetching AuthProjectRealms with their
  fat realm_pb2.Realms bodies).

  ID is always 'meta', the parent entity is corresponding AuthProjectRealms.
  """
  # The git revision the config was picked up from.
  config_rev = ndb.StringProperty(indexed=False)
  # Revision of permissions DB used to expand roles.
  perms_rev = ndb.StringProperty(indexed=False)
  # SHA256 digest of the raw config body.
  config_digest = ndb.StringProperty(indexed=False)
  # When it was updated the last time (mostly FYI).
  modified_ts = ndb.DateTimeProperty(indexed=False)

  @property
  def project_id(self):
    assert self.key.parent().kind() == 'AuthProjectRealms'
    return self.key.parent().id()


def project_realms_meta_key(project_id):
  """An ndb.Key for an AuthProjectRealmsMeta entity."""
  return ndb.Key(
      AuthProjectRealmsMeta, 'meta',
      parent=model.project_realms_key(project_id))

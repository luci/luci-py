# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Primary side of Primary <-> Replica protocol."""

from google.appengine.ext import ndb

from components import datastore_utils


# Root key for AuthReplicaState entities. Entity itself doesn't exist.
REPLICAS_ROOT_KEY = ndb.Key('AuthReplicaStateRoot', 'root')


class AuthReplicaState(ndb.Model, datastore_utils.SerializableModelMixin):
  """Last known state of a Replica as known by Primary.

  Parent key is REPLICAS_ROOT_KEY. Key id is GAE application ID of a replica.
  """
  # How to convert this entity to serializable dict.
  serializable_properties = {
    'replica_url': datastore_utils.READABLE,
    'auth_db_rev': datastore_utils.READABLE,
    'rev_modified_ts': datastore_utils.READABLE,
  }

  # URL of a host to push AuthDB updates to, especially useful on dev_appserver.
  replica_url = ndb.StringProperty(indexed=False)
  # Revision of auth DB replica is synced to.
  auth_db_rev = ndb.IntegerProperty(default=0, indexed=False)
  # Time when auth_db_rev was created (by primary clock).
  rev_modified_ts = ndb.DateTimeProperty(indexed=False)


def register_replica(app_id, replica_url):
  """Creates a new AuthReplicaState or resets the state of existing one."""
  ent = AuthReplicaState(
      id=app_id,
      parent=REPLICAS_ROOT_KEY,
      replica_url=replica_url)
  ent.put()

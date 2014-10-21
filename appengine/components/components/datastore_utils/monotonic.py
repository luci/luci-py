# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Monotonic addition of entities."""

from google.appengine.api import datastore_errors
from google.appengine.ext import ndb
from google.appengine.runtime import apiproxy_errors

__all__ = [
  'HIGH_KEY_ID',
  'get_versioned_most_recent_with_root',
  'get_versioned_root_model',
  'insert',
  'store_new_version',
]


# 2^53 is the largest that can be represented with a float. It's a bit large
# though so save a bit and start at 2^48-1.
HIGH_KEY_ID = (1 << 47) - 1


### Private stuff.


@ndb.transactional(retries=0)  # pylint: disable=E1120
def _insert(entities):
  """Guarantees insertion of the first entity and return True on success.

  Other entities can be saved simultaneously.
  """
  if entities[0].key.get():
    # The entity exists, abort.
    return False
  ndb.put_multi(entities)
  return True


### Public API.


def insert(entity, new_key_callback=None, extra=None):
  """Inserts an entity in the DB and guarantees creation.

  Similar in principle to ndb.Model.get_or_insert() except that it only succeeds
  when the entity was not already present. As such, this always requires a
  transaction.

  Optionally retries with a new key if |new_key_callback| is provided.

  Arguments:
    entity: entity to save, it should have its .key already set accordingly. The
        .key property will be mutated, even if the function fails. It is highly
        preferable to have a root entity so the transaction can be done safely.
    new_key_callback: function to generates a new key if the previous key was
        already taken. If this function returns None, the execution is aborted.
        If this parameter is None, insertion is only tried once.
    extra: additional entities to store simultaneously. For example a bookeeping
        entity that must be updated simultaneously along |entity|. All the
        entities must be inside the same entity group.

  Returns:
    ndb.Key of the newly saved entity or None if the entity was already present
    in the db.
  """
  assert entity.key.id(), entity.key
  entities = [entity]
  if extra:
    entities.extend(extra)
    root = entity.key.pairs()[0]
    assert all(i.key and i.key.pairs()[0] == root for i in extra), extra
  if not new_key_callback:
    new_key_callback = lambda: None

  # TODO(maruel): Run a severe load test and count the number of retries.
  while True:
    # First iterate outside the transaction in case the first entity key number
    # selected is already used.
    while entity.key and entity.key.id() and entity.key.get():
      entity.key = new_key_callback()

    if not entity.key or not entity.key.id():
      break
    try:
      if _insert(entities):
        break
    except (
        apiproxy_errors.CancelledError,
        datastore_errors.BadRequestError,
        datastore_errors.Timeout,
        datastore_errors.TransactionFailedError,
        RuntimeError):
      pass
    # Entity existed. Get the next key.
    entity.key = new_key_callback()
  return entity.key


def get_versioned_root_model(model_name):
  """Returns a root model that can be used for versioned entities.

  Using this entity for get_versioned_most_recent_with_root() and
  store_new_version() is optional. Any entity with .current as an
  ndb.IntegerProperty will do.
  """
  assert isinstance(model_name, str), model_name

  class Root(ndb.Model):
    current = ndb.IntegerProperty(indexed=False)

    @classmethod
    def _get_kind(cls):
      return model_name

  return Root


def get_versioned_most_recent_with_root(cls, root_key):
  """Returns the most recent instance of a versioned entity."""
  assert issubclass(cls, ndb.Model), cls
  assert root_key is None or isinstance(root_key, ndb.Key), root_key

  root = root_key.get()
  if not root:
    return None, None
  index = root.current or HIGH_KEY_ID
  return root, ndb.Key(cls, index, parent=root_key).get()


def store_new_version(entity, root_factory):
  """Stores a new version of the instance.

  entity.key is updated to the key used to store the entity. Only the parent key
  needs to be set.

  If there was no root entity, one is created by calling root_factory().

  Returns:
    tuple(root, entity) with the two entities that were PUT in the db.
  """
  assert isinstance(entity, ndb.Model), entity
  assert entity.key and entity.key.parent(), 'entity.key.parent() must be set.'

  root, previous = get_versioned_most_recent_with_root(
      entity.__class__, entity.key.parent())
  if not root:
    root = root_factory(key=entity.key.parent(), current=HIGH_KEY_ID)
  assert root.current, root

  if previous:
    entity.key = previous.key
  else:
    # Starts with an invalid key, it'll be updated automatically.
    entity.key = ndb.Key(entity.__class__, None, parent=root.key)

  # Very similar to append_decreasing() except that it also updated
  # root.current inside the transaction.

  flat = list(entity.key.flat())
  if not flat[-1]:
    flat[-1] = root.current
    entity.key = ndb.Key(flat=flat)

  def _new_key_minus_one_current():
    flat[-1] -= 1
    root.current = flat[-1]
    return ndb.Key(flat=flat)

  return insert(entity, _new_key_minus_one_current, extra=[root])

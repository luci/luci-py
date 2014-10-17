# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Monotonic addition of entities."""

from google.appengine.api import datastore_errors
from google.appengine.ext import ndb
from google.appengine.runtime import apiproxy_errors

__all__ = [
  'insert',
]


### Private stuff.


@ndb.transactional(retries=0)  # pylint: disable=E1120
def _insert(entity):
  """Guarantees insertion and return True on success.

  This transaction is intentionally very short.
  """
  if entity.key.get():
    # The entity exists, abort.
    return False
  entity.put()
  return True


### Public API.


def insert(entity, new_key_callback=None):
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

  Returns:
    ndb.Key of the newly saved entity or None if the entity was already present
    in the db.
  """
  new_key_callback = new_key_callback or (lambda: None)

  # TODO(maruel): Run a severe load test and count the number of retries.
  while True:
    # First iterate outside the transaction in case the first entity key number
    # selected is already used.
    while entity.key and entity.key.get():
      entity.key = new_key_callback()

    if not entity.key:
      break
    try:
      if _insert(entity):
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

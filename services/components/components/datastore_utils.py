# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Mixed bag of ndb related utilities.

- Sharding Entity group utility function to improve performance.
- Query management functions.

TODO(maruel): Extract ndb related code from utils.py back in here.
"""

import hashlib
import json
import string

from google.appengine.api import datastore_errors
from google.appengine.ext import ndb

from components import utils

_HEX = frozenset(string.hexdigits.lower())


class BytesComputedProperty(ndb.ComputedProperty):
  """Adds support to ComputedProperty for raw binary data.

  Use this class instead of ComputedProperty if the returned data is raw binary
  and not utf-8 compatible, as ComputedProperty assumes.
  """
  # pylint: disable=R0201
  def _db_set_value(self, v, p, value):
    # From BlobProperty.
    p.set_meaning(ndb.google_imports.entity_pb.Property.BYTESTRING)
    v.set_stringvalue(value)


class DeterministicJsonProperty(ndb.BlobProperty):
  """Makes JsonProperty encoding deterministic where the same data results in
  the same blob all the time.

  For example, a dict is guaranteed to have its keys sorted, the whitespace
  separators are stripped, encoding is set to utf-8 so the output is constant.

  Sadly, we can't inherit from JsonProperty because it would result in
  duplicate encoding. So copy-paste the class from SDK v1.9.0 here.
  """
  _json_type = None

  # pylint: disable=W0212,E1002,R0201
  @ndb.utils.positional(1 + ndb.BlobProperty._positional)
  def __init__(self, name=None, compressed=False, json_type=None, **kwds):
    super(DeterministicJsonProperty, self).__init__(
        name=name, compressed=compressed, **kwds)
    self._json_type = json_type

  def _validate(self, value):
    if self._json_type is not None and not isinstance(value, self._json_type):
      # Add the property name, otherwise it's annoying to try to figure out
      # which property is incorrect.
      raise TypeError(
          'Property %s must be a %s' % (self._name, self._json_type))

  def _to_base_type(self, value):
    """Makes it deterministic compared to ndb.JsonProperty._to_base_type()."""
    return utils.encode_to_json(value)

  def _from_base_type(self, value):
    return json.loads(value)


def shard_key(key, number_of_letters, root_entity_type):
  """Returns an ndb.Key to a virtual entity of type |root_entity_type|.

  This key is to be used as an entity group for database sharding. Transactions
  can be done over this group. Note that this sharding root entity doesn't have
  to ever exist in the database.

  Arguments:
    key: full key to take a subset of. It must be '[0-9a-f]+'. It is assumed
        that this key is well distributed, if not, use hashed_shard_key()
        instead. This means the available number of buckets is done in
        increments of 4 bits, e.g. 16, 256, 4096, 65536.
    number_of_letters: number of letters to use from |key|. key length must be
        encoded through an out-of-band mean and be constant.
    root_entity_type: root entity type. It can be either a reference to a
        ndb.Model class or just a string.
  """
  assert _HEX.issuperset(key), key
  assert isinstance(key, str) and len(key) >= number_of_letters, key
  # number_of_letters==10 means 1099511627776 shards, which is unreasonable.
  assert 1 <= number_of_letters < 10, number_of_letters
  assert isinstance(root_entity_type, (ndb.Model, str)) and root_entity_type, (
      root_entity_type)
  return ndb.Key(root_entity_type, key[:number_of_letters])


def hashed_shard_key(key, number_of_letters, root_entity_type):
  """Returns a ndb.Key to a virtual entity of type |root_entity_type|.

  The main difference with shard_key() is that it doesn't assume the key is well
  distributed so it first hashes the value via MD5 to make it more distributed.
  """
  return shard_key(
      hashlib.md5(key).hexdigest(), number_of_letters, root_entity_type)


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
    except datastore_errors.TransactionFailedError:
      pass
    # Entity existed. Get the next key.
    entity.key = new_key_callback()
  return entity.key


def pop_future_done(futures):
  """Removes the currently done futures."""
  for i in xrange(len(futures) - 1, -1, -1):
    if futures[i].done():
      futures.pop(i)


def page_queries(queries, fetch_page_size=20):
  """Yields all the items returned by the queries, page by page.

  It makes heavy use of fetch_page_async() for maximum efficiency.
  """
  queries = queries[:]
  futures = [q.fetch_page_async(fetch_page_size) for q in queries]
  while queries:
    i = futures.index(ndb.Future.wait_any(futures))
    results, cursor, more = futures[i].get_result()
    if not more:
      # Remove completed queries.
      queries.pop(i)
      futures.pop(i)
    else:
      futures[i] = queries[i].fetch_page_async(
          fetch_page_size, start_cursor=cursor)
    yield results


def _process_chunk_of_items(
    map_fn, action_futures, items_to_process, max_inflight, map_page_size):
  """Maps as many items as possible and throttles down to 'max_inflight'.

  |action_futures| is modified in-place.
  Remaining items_to_process is returned.
  """
  # First, throttle.
  pop_future_done(action_futures)
  while len(action_futures) > max_inflight:
    ndb.Future.wait_any(action_futures)
    pop_future_done(action_futures)

  # Then, map. map_fn() may return None so "or []" to not throw an exception. It
  # just means there no async operation to wait on.
  action_futures.extend(map_fn(items_to_process[:map_page_size]) or [])
  return items_to_process[map_page_size:]


def incremental_map(
    queries, map_fn, filter_fn=None, max_inflight=100, map_page_size=20,
    fetch_page_size=20):
  """Applies |map_fn| to objects in a list of queries asynchrously.

  This function is itself synchronous.

  It's a mapper without a reducer.

  Arguments:
    queries: list of iterators of items to process.
    map_fn: callback that accepts a list of objects to map and optionally
            returns a list of ndb.Future.
    filter_fn: optional callback that can filter out items from |query| from
               deletion when returning False.
    max_inflight: maximum limit of number of outstanding futures returned by
                  |map_fn|.
    map_page_size: number of items to pass to |map_fn| at a time.
    fetch_page_size: number of items to retrieve from |queries| at a time.
  """
  items_to_process = []
  action_futures = []

  for items in page_queries(queries, fetch_page_size=fetch_page_size):
    items_to_process.extend(i for i in items if not filter_fn or filter_fn(i))
    while len(items_to_process) >= map_page_size:
      items_to_process = _process_chunk_of_items(
          map_fn, action_futures, items_to_process, max_inflight, map_page_size)

  while items_to_process:
    items_to_process = _process_chunk_of_items(
        map_fn, action_futures, items_to_process, max_inflight, map_page_size)

  ndb.Future.wait_all(action_futures)

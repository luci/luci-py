# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Storage of config files."""

import hashlib
import logging
import urlparse

from google.appengine.api import app_identity
from google.appengine.ext import ndb
from google.appengine.ext.ndb import msgprop
from google.protobuf import text_format

from components import config
from components import utils

class Blob(ndb.Model):
  """Content-addressed blob. Immutable.

  Entity key:
    Id is content hash that has format "v1:<sha>"
    where sha is hex-encoded Git-compliant SHA-1 of
    'blob {content len}\0{content}'. Computed by compute_hash function.
    Blob has no parent.
  """
  created_ts = ndb.DateTimeProperty(auto_now_add=True)
  content = ndb.BlobProperty(required=True)


class ConfigSet(ndb.Model):
  """Versioned collection of config files.

  Entity key:
    Id is a config set name. Examples: services/luci-config, projects/chromium.

  gitiles_import.py relies on the fact that this class has only one attribute.
  """
  # last imported revision of the config set. See also Revision and File.
  latest_revision = ndb.StringProperty(required=True)
  latest_revision_url = ndb.StringProperty(indexed=False)
  latest_revision_time = ndb.DateTimeProperty(indexed=False)
  latest_revision_committer_email = ndb.StringProperty(indexed=False)

  location = ndb.StringProperty(required=True)


class RevisionInfo(ndb.Model):
  """Contains revision metadata.

  Used with StructuredProperty.
  """
  id = ndb.StringProperty(required=True, indexed=False)
  url = ndb.StringProperty(indexed=False)
  time = ndb.DateTimeProperty(indexed=False)
  committer_email = ndb.StringProperty(indexed=False)


class ImportAttempt(ndb.Model):
  """Describes what happened last time we tried to import a config set.

  Entity key:
    Parent is ConfigSet (does not have to exist).
    ID is "last".
  """
  time = ndb.DateTimeProperty(auto_now_add=True, required=True, indexed=False)
  revision = ndb.StructuredProperty(RevisionInfo, indexed=False)
  success = ndb.BooleanProperty(required=True, indexed=False)
  message = ndb.StringProperty(required=True, indexed=False)

  class ValidationMessage(ndb.Model):
    severity = msgprop.EnumProperty(config.Severity, indexed=False)
    text = ndb.StringProperty(indexed=False)

  validation_messages = ndb.StructuredProperty(ValidationMessage, repeated=True)


class Revision(ndb.Model):
  """A single revision of a config set. Immutable.

  Parent of File entities. Revision entity does not have to exist.

  Entity key:
    Id is a revision name. If imported from Git, it is a commit hash.
    Parent is ConfigSet.
  """


class File(ndb.Model):
  """A single file in a revision. Immutable.

  Entity key:
    Id is a filename without a leading slash. Parent is Revision.
  """
  created_ts = ndb.DateTimeProperty(auto_now_add=True)
  # hash of the file content, computed by compute_hash().
  # A Blob entity with this key must exist.
  content_hash = ndb.StringProperty(indexed=False, required=True)

  def _pre_put_hook(self):
    assert isinstance(self.key.id(), str)
    assert not self.key.id().startswith('/')


def last_import_attempt_key(config_set):
  return ndb.Key(ConfigSet, config_set, ImportAttempt, 'last')


@ndb.tasklet
def get_config_sets_async(config_set=None):
  if config_set:
    existing = yield ConfigSet.get_by_id_async(config_set)
    config_sets = [existing or ConfigSet(id=config_set)]
  else:
    config_sets = yield ConfigSet.query().fetch_async()
  raise ndb.Return(config_sets)


@ndb.tasklet
def get_latest_revision_async(config_set):
  """Returns latest known revision of the |config_set|. May return None."""
  config_set_entity = yield ConfigSet.get_by_id_async(config_set)
  raise ndb.Return(
      config_set_entity.latest_revision if config_set_entity else None)


@ndb.tasklet
def get_config_hash_async(config_set, path, revision=None):
  """Returns tuple (revision, content_hash).

  |revision| detaults to the latest revision.
  """
  assert isinstance(config_set, basestring)
  assert config_set
  assert isinstance(path, basestring)
  assert path
  assert not path.startswith('/')

  if not revision:
    revision = yield get_latest_revision_async(config_set)
    if revision is None:
      logging.warning('Config set not found: %s' % config_set)
      raise ndb.Return(None, None)

  assert revision
  file_key = ndb.Key(
      ConfigSet, config_set,
      Revision, revision,
      File, path)
  file_entity = yield file_key.get_async()
  content_hash = file_entity.content_hash if file_entity else None
  if not content_hash:
    revision = None
  raise ndb.Return(revision, content_hash)


@ndb.tasklet
def get_config_by_hash_async(content_hash):
  """Returns config content by its hash."""
  blob = yield Blob.get_by_id_async(content_hash)
  raise ndb.Return(blob.content if blob else None)


@ndb.tasklet
def get_latest_async(config_set, path):
  """Returns latest content of a config file."""
  _, content_hash = yield get_config_hash_async(config_set, path)
  if not content_hash:  # pragma: no cover
    raise ndb.Return(None)
  content = yield get_config_by_hash_async(content_hash)
  raise ndb.Return(content)


@ndb.tasklet
def get_latest_multi_async(config_sets, path, hashes_only=False):
  """Returns latest contents of all <config_set>:<path> config files.

  Returns:
    A a list of dicts with keys 'config_set', 'revision', 'content_hash' and
    'content', 'url'. Content is not available if |hashes_only| is True.
  """
  assert path
  assert not path.startswith('/')

  config_set_keys = [ndb.Key(ConfigSet, cs) for cs in config_sets]
  config_set_entities = yield ndb.get_multi_async(config_set_keys)
  config_set_entities = filter(None, config_set_entities)

  file_keys = [
    ndb.Key(ConfigSet, cs.key.id(), Revision, cs.latest_revision, File, path)
    for cs in config_set_entities
  ]
  file_entities = yield ndb.get_multi_async(file_keys)

  blob_futures = {}
  if not hashes_only:
    blob_futures = {
      f.content_hash: ndb.Key(Blob, f.content_hash).get_async()
      for f in file_entities
      if f
    }
    yield blob_futures.values()

  results = []
  for cs, f in zip(config_set_entities, file_entities):
    if not f:
      continue
    url = None
    if cs.latest_revision_url:
      base = cs.latest_revision_url
      if not base.endswith('/'):
        base += '/'
      url = urlparse.urljoin(base, path)
    blob_fut = blob_futures.get(f.content_hash)
    results.append({
      'config_set': f.key.parent().parent().id(),
      'content': blob_fut.get_result().content if blob_fut else None,
      'content_hash': f.content_hash,
      'revision': f.key.parent().id(),
      'url': url,
    })
  raise ndb.Return(results)


@ndb.tasklet
def get_latest_as_message_async(config_set, path, message_factory):
  """Reads latest config file as a text-formatted protobuf message.

  |message_factory| is a function that creates a message. Typically the message
  type itself. Values found in the retrieved config file are merged into the
  return value of the factory.

  Memcaches results.
  """
  msg = message_factory()
  cache_key = 'get_latest_as_message(%r, %r)' % (config_set, path)
  ctx = ndb.get_context()
  cached = yield ctx.memcache_get(cache_key)
  if cached:
    msg.ParseFromString(cached)
    raise ndb.Return(msg)

  text = yield get_latest_async(config_set, path)
  if text:
    text_format.Merge(text, msg)
  yield ctx.memcache_set(cache_key, msg.SerializeToString(), time=60)
  raise ndb.Return(msg)


@utils.cache
def get_self_config_set():
  return 'services/%s' % app_identity.get_application_id()


def get_self_config_async(path, message_factory):
  """Parses a config file in the app's config set into a protobuf message."""
  return get_latest_as_message_async(
      get_self_config_set(), path, message_factory)


def compute_hash(content):
  """Computes Blob id by its content.

  See Blob docstring for Blob id format.
  """
  sha = hashlib.sha1()
  sha.update('blob %d\0' % len(content))
  sha.update(content)
  return 'v1:%s' % sha.hexdigest()


@ndb.tasklet
def import_blob_async(content, content_hash=None):
  """Saves |content| to a Blob entity.

  Returns:
    Content hash.
  """
  content_hash = content_hash or compute_hash(content)

  # pylint: disable=E1120
  if not Blob.get_by_id(content_hash):
    yield Blob(id=content_hash, content=content).put_async()
  raise ndb.Return(content_hash)


def import_blob(content, content_hash=None):
  return import_blob_async(content, content_hash=content_hash).get_result()

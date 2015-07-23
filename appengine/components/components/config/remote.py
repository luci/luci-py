# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""remote.Provider reads configs from a remote config service."""

import base64
import datetime
import json
import logging
import urllib

from google.appengine.ext import ndb

from components import auth
from components import net
from components import utils

from . import common
from . import validation


MEMCACHE_PREFIX = 'components.config/v1/'
# Delete LastGoodConfig if it was not accessed for more than a week.
CONFIG_MAX_TIME_SINCE_LAST_ACCESS = datetime.timedelta(days=7)
# Update LastGoodConfig.last_access_ts if it will be deleted next day.
UPDATE_LAST_ACCESS_TIME_FREQUENCY = datetime.timedelta(days=1)


class LastGoodConfig(ndb.Model):
  """Last-known valid config.

  Not used to store intermediate/old versions.

  Entity key:
    Root entity. Id is "<config_set>:<path>".
  """
  content = ndb.BlobProperty()
  content_hash = ndb.StringProperty()
  revision = ndb.StringProperty()
  last_access_ts = ndb.DateTimeProperty()


class Provider(object):
  """Configuration provider that fethes configs from a config service.

  See api._get_config_provider for context.
  """

  def __init__(self, service_hostname):
    assert service_hostname
    self.service_hostname = service_hostname

  @ndb.tasklet
  def _api_call_async(self, path, allow_not_found=True, **kwargs):
    assert path
    url = 'https://%s/_ah/api/config/v1/%s' % (self.service_hostname, path)
    kwargs.setdefault('scopes', net.EMAIL_SCOPE)
    try:
      response = yield net.json_request_async(url, **kwargs)
      raise ndb.Return(response)
    except net.NotFoundError as ex:
      if allow_not_found:
        raise ndb.Return(None)
      logging.warning('404 response: %s', ex.response)
      raise

  @ndb.tasklet
  def get_config_by_hash_async(self, content_hash):
    """Returns a config blob by its hash. Optionally memcaches results."""
    assert content_hash
    cache_key = '%sconfig_by_hash/%s' % (MEMCACHE_PREFIX, content_hash)
    ctx = ndb.get_context()
    content = yield ctx.memcache_get(cache_key)
    if content is not None:
      raise ndb.Return(content)

    res = yield self._api_call_async('config/%s' % content_hash)
    content = base64.b64decode(res.get('content')) if res else None
    if content is not None:
      yield ctx.memcache_set(cache_key, content)
    raise ndb.Return(content)

  @ndb.tasklet
  def get_config_hash_async(
      self, config_set, path, revision=None, use_memcache=True):
    """Returns tuple (revision, content_hash). Optionally memcaches results.

    If |revision| is not specified, memcaches for only 1 min.
    """
    assert config_set
    assert path

    get_latest = not revision

    content_hash = None
    if use_memcache:
      cache_key = (
          '%sconfig_hash/%s:%s@%s' %
          (MEMCACHE_PREFIX, config_set, path, revision or '!latest'))
      ctx = ndb.get_context()
      revision, content_hash = (
          (yield ctx.memcache_get(cache_key)) or (revision, None))

    if not content_hash:
      url_path = format_url('config_sets/%s/config/%s', config_set, path)
      params = {'hash_only': True}
      if revision:
        params['revision'] = revision
      res = yield self._api_call_async(url_path, params=params)
      if res:
        revision = res['revision']
        content_hash = res['content_hash']
        if content_hash and use_memcache:
          yield ctx.memcache_set(
              cache_key, (revision, content_hash), time=60 if get_latest else 0)
    raise ndb.Return((revision, content_hash))

  @ndb.tasklet
  def get_async(self, config_set, path, revision=None, store_last_good=None):
    """Returns tuple (revision, content).

    See api.get_async for more info.
    """
    assert config_set
    assert path

    if store_last_good:
      last_good = yield _get_last_good_async(config_set, path)
      raise ndb.Return((last_good.revision, last_good.content))

    revision, content_hash = yield self.get_config_hash_async(
        config_set, path, revision=revision)
    content = None
    if content_hash:
      content = yield self.get_config_by_hash_async(content_hash)
    raise ndb.Return((revision, content))

  @ndb.tasklet
  def _get_configs_multi(self, url_path):
    """Returns a map config_set -> (revision, content)."""
    assert url_path

    # Response must return a dict with 'configs' key which is a list of configs.
    # Each config has keys 'config_set', 'revision' and 'content_hash'.
    res = yield self._api_call_async(
        url_path, params={'hashes_only': True}, allow_not_found=False)

    # Load config contents. Most of them will come from memcache.
    for cfg in res['configs']:
      cfg['project_id'] = cfg['config_set'].split('/', 1)[1]
      cfg['get_content_future'] = self.get_config_by_hash_async(
          cfg['content_hash'])

    for cfg in res['configs']:
      cfg['content'] = yield cfg['get_content_future']
      if not cfg['content']:
        logging.error(
            'Config content for %s was not loaded by hash %r',
            cfg['config_set'], cfg['content_hash'])

    raise ndb.Return({
      cfg['config_set']: (cfg['revision'], cfg['content'])
      for cfg in res['configs']
      if cfg['content']
    })

  def get_project_configs_async(self, path):
    """Reads a config file in all projects.

    Returns:
      {"config_set -> (revision, content)} map.
    """
    return self._get_configs_multi(format_url('configs/projects/%s', path))

  def get_ref_configs_async(self, path):
    """Reads a config file in all refs of all projects.

    Returns:
      {"config_set -> (revision, content)} map.
    """
    return self._get_configs_multi(format_url('configs/refs/%s', path))

  @ndb.tasklet
  def get_projects_async(self):
    res = yield self._api_call_async('projects', allow_not_found=False)
    raise ndb.Return(res.get('projects', []))

  @ndb.tasklet
  def get_config_set_location_async(self, config_set):
    """Returns URL of where configs for given config set are stored.

    Returns:
      URL or None if no such config set.
    """
    assert config_set
    res = yield self._api_call_async(
        'mapping', params={'config_set': config_set})
    if not res:
      raise ndb.Return(None)
    for entry in res.get('mappings', []):
      if entry.get('config_set') == config_set:
        raise ndb.Return(entry.get('location'))
    raise ndb.Return(None)

  @ndb.tasklet
  def _update_last_good_config_async(self, config_key):
    now = utils.utcnow()
    current = yield config_key.get_async()
    earliest_access_ts = now - CONFIG_MAX_TIME_SINCE_LAST_ACCESS
    if current.last_access_ts < earliest_access_ts:
      # Last access time was too long ago.
      yield current.key.delete_async()
      return

    config_set, path = config_key.id().split(':', 1)
    revision, content_hash = yield self.get_config_hash_async(
        config_set, path, use_memcache=False)
    if not revision:
      logging.warning(
          'Could not fetch hash of latest %s', config_key.id())
      return
    if current.revision == revision:
      assert current.content_hash == content_hash
      return

    content = None
    if current.content_hash != content_hash:
      content = yield self.get_config_by_hash_async(content_hash)
      if content is None:
        logging.warning(
            'Could not fetch config content %s by hash %s',
            config_key.id(), content_hash)
        return
      logging.debug('Validating %s:%s@%s', config_set, path, revision)
      ctx = validation.Context.logging()
      validation.validate(config_set, path, content, ctx=ctx)
      if ctx.result().has_errors:
        logging.exception(
            'Invalid config %s:%s@%s is ignored', config_set, path, revision)
        return

    @ndb.transactional_tasklet
    def update():
      config = yield config_key.get_async()
      config.revision = revision
      if config.content_hash != content_hash:
        if content is None:
          # Config was updated between content_hash was resolved and
          # the transaction has started. Do nothing.
          return
        config.content_hash = content_hash
        config.content = content
      yield config.put_async()
      logging.info(
          'Updated last good config %s to %s',
          config_key.id(), revision)
    yield update()


@ndb.tasklet
def _get_last_good_async(config_set, path):
  """Fetches LastGoodConfig and updates last_access_ts if needed."""
  now = utils.utcnow()
  # By inserting an entity we tell a Cron job to fetch it from a config service.
  last_good_id = '%s:%s' % (config_set, path)
  last_good = yield LastGoodConfig.get_or_insert_async(
      last_good_id, last_access_ts=now)

  # Update last_access_ts, but not on each call.
  if (last_good.last_access_ts is None or
      now - last_good.last_access_ts > UPDATE_LAST_ACCESS_TIME_FREQUENCY):
    # pylint does not like this usage of transactional_tasklet
    # pylint: disable=no-value-for-parameter
    @ndb.transactional_tasklet(propagation=ndb.TransactionOptions.INDEPENDENT)
    def update():
      last_good = yield LastGoodConfig.get_or_insert_async(
          last_good_id, last_access_ts=now)
      last_good.last_access_ts = now
      yield last_good.put_async()
    yield update()
  raise ndb.Return(last_good)


def format_url(url_format, *args):
  return url_format % tuple(urllib.quote(a, '') for a in args)


def get_provider():
  """Returns True if config service hostname is set."""
  settings = common.ConfigSettings.cached()
  if settings and settings.service_hostname:
    return Provider(settings.service_hostname)
  return None


def cron_update_last_good_configs():
  provider = get_provider()
  if provider:
    f = LastGoodConfig.query().map_async(
        provider._update_last_good_config_async, keys_only=True)
    f.check_success()

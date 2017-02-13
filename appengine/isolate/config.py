# Copyright 2013 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Instance specific settings."""

import logging
import posixpath
import re

from google.appengine.api import app_identity
from google.appengine.api import modules
from google.appengine.ext import ndb

from components import config
from components import gitiles
from components import net
from components import utils
from components.config import validation
from components.datastore_utils import config as ds_config

from proto import config_pb2


ConfigApi = config.ConfigApi


### Public code.

class Config(object):
  """A join of Datastore and config_service backed configs."""
  def __init__(self, ds_cfg, cfg):
    self._ds_cfg = ds_cfg
    self._cfg = cfg

  def __getattr__(self, name):
    if hasattr(self._ds_cfg, name):
      return getattr(self._ds_cfg, name)
    return getattr(self._cfg, name)


# TODO(sergeyberezin): remove all fields except gs_private_key. Switch
# the other fields to use luci-config.
class GlobalConfig(ds_config.GlobalConfig):
  """Application wide settings."""
  # The number of seconds a cache entry must be kept for before it is evicted.
  default_expiration = ndb.IntegerProperty(indexed=False, default=30*24*60*60)

  # This determines the number of initial letters from the ContentEntry hash
  # value to use as buckets in ContentShard. This is to even out writes across
  # multiple entity groups. The goal is to get into the range of ~1 write per
  # second per bucket.
  #
  # Each letter represent 4 bits of information, so the number of ContentShard
  # will be 16**N. Nominal values are:
  #   1: 16 buckets
  #   2: 256 buckets
  #   3: 4096 buckets
  #   4: 65536 buckets
  sharding_letters = ndb.IntegerProperty(indexed=False, default=4)

  # The Google Cloud Storage bucket where to save the data. By default it's the
  # name of the application instance.
  gs_bucket = ndb.StringProperty(indexed=False)

  # Email address of Service account used to access Google Storage.
  gs_client_id_email = ndb.StringProperty(indexed=False, default='')

  # Secret key used to sign Google Storage URLs: base64 encoded *.der file.
  gs_private_key = ndb.StringProperty(indexed=False, default='')

  # id to inject into pages if applicable.
  google_analytics = ndb.StringProperty(indexed=False, default='')

  # Enable ts_mon based monitoring.
  enable_ts_monitoring = ndb.BooleanProperty(indexed=False, default=False)

  # Group with read and write access.
  full_access_group = ndb.StringProperty(indexed=False,
                                         default='isolate-access')

  # Group with read-only access.
  readonly_access_group = ndb.StringProperty(indexed=False,
                                             default='isolate-readonly-access')

  def set_defaults(self):
    self.gs_bucket = app_identity.get_application_id()


def settings(fresh=False):
  """Loads GlobalConfig or a default one if not present.

  If fresh=True, a full fetch from NDB is done.
  """
  if fresh:
    GlobalConfig.clear_cache()
    cfg = _get_settings_with_defaults()[1]
  else:
    cfg = _get_settings_cached()
  ds_cfg = GlobalConfig.cached()

  cfg.auth.full_access_group = ds_cfg.full_access_group
  cfg.auth.readonly_access_group = ds_cfg.readonly_access_group

  return Config(ds_cfg, cfg)


def settings_info():
  """Returns information about the settings file.

  Returns a dict with keys:
    'cfg': parsed SettingsCfg message
    'rev': revision of cfg
    'rev_url': URL of a human-consumable page that displays the config
    'config_service_url': URL of the config_service.
  """
  GlobalConfig.clear_cache()
  ds_cfg = GlobalConfig.cached()
  rev, cfg = _get_settings_with_defaults()
  rev_url = _gitiles_url(_get_configs_url(), rev, _SETTINGS_CFG_FILENAME)
  cfg_service_hostname = config.config_service_hostname()
  return {
    'cfg': ds_cfg,
    'config_service_url':
       'https://%s' % cfg_service_hostname if cfg_service_hostname else '',
    'luci_cfg': cfg,
    'rev': rev,
    'rev_url': rev_url,
  }


def get_local_dev_server_host():
  """Returns 'hostname:port' for a default module on a local dev server."""
  assert utils.is_local_dev_server()
  return modules.get_hostname(module='default')


def warmup():
  """Precaches configuration in local memory, to be called from warmup handler.

  This call is optional. Everything works even if 'warmup' is never called.
  """
  settings()
  utils.get_task_queue_host()
  utils.get_app_version()


### Private code.

_SETTINGS_CFG_FILENAME = 'settings.cfg'
_GS_BUCKET_RE = re.compile(r'^[a-z0-9A-Z\-]+$')
_EMAIL_RE = re.compile(r'^[a-z0-9A-Z\-\._+]+@[a-z0-9A-Z\-\._]+$')


@validation.self_rule(_SETTINGS_CFG_FILENAME, config_pb2.SettingsCfg)
def _validate_settings(cfg, ctx):
  """Validates settings.cfg file against proto message schema."""
  with ctx.prefix('default_expiration '):
    if cfg.default_expiration < 0:
      ctx.error('cannot be negative')

  with ctx.prefix('sharding_letters '):
    if not (0 <= cfg.sharding_letters and cfg.sharding_letters <= 5):
      ctx.error('must be within [0..5]')

  with ctx.prefix('gs_bucket '):
    if not _GS_BUCKET_RE.match(cfg.gs_bucket):
      ctx.error('invalid value: %s', cfg.gs_bucket)

  if cfg.HasField('gs_client_id_email'):
    with ctx.prefix('gs_client_id_email '):
      if not _EMAIL_RE.match(cfg.gs_client_id_email):
        ctx.error('invalid value: %s', cfg.gs_client_id_email)


@utils.memcache('config:get_configs_url', time=60)
def _get_configs_url():
  """Returns URL where luci-config fetches configs from."""
  try:
    return config.get_config_set_location(config.self_config_set())
  except net.Error:
    logging.info(
        'Could not get configs URL. Possibly config directory for this '
        'instance of swarming does not exist')


def _gitiles_url(configs_url, rev, path):
  """URL to a directory in gitiles -> URL to a file at concrete revision."""
  try:
    loc = gitiles.Location.parse(configs_url or '')
    return str(loc._replace(
        treeish=rev or loc.treeish,
        path=posixpath.join(loc.path, path)))
  except ValueError:
    # Not a gitiles URL, return as is.
    return configs_url


def _get_settings():
  """Returns (rev, cfg) where cfg is a parsed SettingsCfg message.

  If config does not exists, returns (None, None).

  Mock this method in tests to inject changes to the defaults.
  """
  # store_last_good=True tells config component to update the config file
  # in a cron job. Here we just read from the datastore.
  return config.get_self_config(
      _SETTINGS_CFG_FILENAME, config_pb2.SettingsCfg, store_last_good=True)


def _get_settings_with_defaults():
  """Returns (rev, cfg) where cfg is a parsed SettingsCfg message.

  If config does not exists, returns (None, <cfg with defaults>).

  The config is cached in the datastore.
  """
  rev, cfg = _get_settings()
  cfg = cfg or config_pb2.SettingsCfg()
  cfg.default_expiration = cfg.default_expiration or 30*24*60*60
  cfg.sharding_letters = cfg.sharding_letters or 4
  cfg.gs_bucket = cfg.gs_bucket or app_identity.get_application_id()
  cfg.auth.full_access_group = cfg.auth.full_access_group or 'isolate-access'
  cfg.auth.readonly_access_group = \
      cfg.auth.readonly_access_group or 'isolate-readonly-access'
  return rev, cfg


@utils.cache_with_expiration(60)
def _get_settings_cached():
  """Loads settings from an NDB-based cache or a default one if not present."""
  return _get_settings_with_defaults()[1]

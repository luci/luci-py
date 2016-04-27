# Copyright 2013 The LUCI Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Instance specific settings."""

import logging
import posixpath

from components import config
from components import gitiles
from components import net
from components import utils
from components.config import validation

from proto import config_pb2

SETTINGS_CFG_FILENAME = 'settings.cfg'


ConfigApi = config.ConfigApi


@validation.self_rule(SETTINGS_CFG_FILENAME, config_pb2.SettingsCfg)
def validate_settings(cfg, ctx):
  """Validates settings.cfg file against proto message schema."""
  def within_year(value):
    if value < 0:
      ctx.error('cannot be negative')
    elif value > 60 * 60 * 24 * 365:
      ctx.error('cannot be more than a year')

  with ctx.prefix('bot_death_timeout_secs '):
    within_year(cfg.bot_death_timeout_secs)
  with ctx.prefix('reusable_task_age_secs '):
    within_year(cfg.reusable_task_age_secs)


@utils.memcache('config:get_configs_url', time=60)
def _get_configs_url():
  """Returns URL where luci-config fetches configs from."""
  url = None
  try:
    url = config.get_config_set_location(config.self_config_set())
  except net.Error:
    logging.info(
        'Could not get configs URL. Possibly config directory for this '
        'instance of swarming does not exist')
  return url or 'about:blank'


def _gitiles_url(configs_url, rev, path):
  """URL to a directory in gitiles -> URL to a file at concrete revision."""
  try:
    loc = gitiles.Location.parse(configs_url)
    return str(loc._replace(
        treeish=rev or loc.treeish,
        path=posixpath.join(loc.path, path)))
  except ValueError:
    # Not a gitiles URL, return as is.
    return configs_url


def _get_settings():
  """Returns (rev, cfg) where cfg is a parsed SettingsCfg message.

  If config does not exists, returns (None, <cfg with defaults>).

  The config is cached in the datastore.
  """
  rev = None
  cfg = None
  try:
    # store_last_good=True tells config component to update the config file
    # in a cron job. Here we just read from datastore.
    rev, cfg = config.get_self_config(
        SETTINGS_CFG_FILENAME, config_pb2.SettingsCfg, store_last_good=True)
  except config.CannotLoadConfigError as ex:
    logging.info('Could not load settings.cfg: %s; using defaults', ex)
  if not cfg:
    cfg = config_pb2.SettingsCfg(
        reusable_task_age_secs=7*24*60*60,
        bot_death_timeout_secs=10*60)
  return rev, cfg


def settings_info():
  """Returns information about the settings file.

  Returns a dict with keys:
    'cfg': parsed SettingsCfg message
    'rev': revision of cfg
    'rev_url': URL of a human-consumable page that displays the config
    'config_service_url': URL of the config_service.
  """
  rev, cfg = _get_settings()
  rev_url = _gitiles_url(_get_configs_url(), rev, SETTINGS_CFG_FILENAME)
  cfg_service_hostname = config.config_service_hostname()
  return {
    'cfg': cfg,
    'rev': rev,
    'rev_url': rev_url,
    'config_service_url': (
        'https://%s' % cfg_service_hostname if cfg_service_hostname else ''
    ),
  }


def settings():
  """Loads settings from an NDB-based cache or a default one if not present."""
  return _get_settings()[1]

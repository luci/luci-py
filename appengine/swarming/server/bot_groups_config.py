# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Functions to fetch and interpret bots.cfg file with list of bot groups."""

import collections
import hashlib
import logging
import re

from components import auth
from components import config
from components import utils
from components.config import validation

from proto import bots_pb2
from server import config as local_config


BOTS_CFG_FILENAME = 'bots.cfg'


# Configuration that applies to some group of bots. Derived from BotsCfg and
# BotGroup in bots.proto. See comments there. This tuple contains already
# validated values.
BotGroupConfig = collections.namedtuple('BotGroupConfig', [
  # The hash of the rest of the data in this tuple, see _gen_version.
  'version',

  # If True, the bot is expected to authenticate as "bot:<bot-id>.*".
  'require_luci_machine_token',

  # If set to non empty string, the bot is expected to authenticate as
  # "user:<service_account>".
  'require_service_account',

  # If set to non empty string, name of IP whitelist that's expected to contain
  # bot's IP.
  'ip_whitelist',

  # Tuple with emails of bot owners.
  'owners',

  # Dict {key => list of values}. Always contains all the keys specified by
  # 'trusted_dimensions' set in BotsCfg. If BotGroup doesn't define some
  # dimension from that set, the list of value for it will be empty. Key and
  # values are unicode strings.
  'dimensions',
])


# Post-processed and validated read-only form of bots.cfg config. Its structure
# is optimized for fast lookup of BotGroupConfig by bot_id.
_BotGroups = collections.namedtuple('_BotGroups', [
  'direct_matches', # dict bot_id => BotGroupConfig
  'prefix_matches', # list of pairs (bot_id_prefix, BotGroupConfig)
  'machine_types',  # dict machine_type.name => BotGroupConfig
  'default_group',  # fallback BotGroupConfig or None if not defined
])


# Default config to use on unconfigured server.
_DEFAULT_BOT_GROUPS = _BotGroups(
    direct_matches={},
    prefix_matches=[],
    machine_types={},
    default_group=BotGroupConfig(
        version='default',
        require_luci_machine_token=False,
        require_service_account=None,
        ip_whitelist=auth.BOTS_IP_WHITELIST,
        owners=(),
        dimensions={}))


def _gen_version(fields):
  """Looks at BotGroupConfig fields and derives a digest that summarizes them.

  This digest is going to be sent to the bot in /handshake, and bot would
  include it in its state (and thus send it with each /poll). If server detects
  that the bot is using older version of the config, it would ask the bot
  to restart.

  Args:
    fields: dict with BotGroupConfig fields (without 'version').

  Returns:
    A string that going to be used as 'version' field of BotGroupConfig tuple.
  """
  # Just hash JSON representation (with sorted keys). Assumes it is stable
  # enough. Add a prefix and trim a bit, to clarify that is it not git hash or
  # anything like that, but just a dumb hash of the actual config.
  digest = hashlib.sha1(utils.encode_to_json(fields)).hexdigest()
  return 'hash:' + digest[:14]


def _make_bot_group_config(**fields):
  """Instantiates BotGroupConfig properly deriving 'version' field."""
  return BotGroupConfig(version=_gen_version(fields), **fields)


def get_bot_group_config(bot_id, machine_type):
  """Returns BotGroupConfig for a bot with given ID or machine type.

  Returns:
    BotGroupConfig or None if not found.
  """
  cfg = _fetch_bot_groups()

  if machine_type and cfg.machine_types.get(machine_type):
    return cfg.machine_types[machine_type]

  gr = cfg.direct_matches.get(bot_id)
  if gr is not None:
    return gr

  for prefix, gr in cfg.prefix_matches:
    if bot_id.startswith(prefix):
      return gr

  return cfg.default_group


def _bot_group_proto_to_tuple(msg, trusted_dimensions):
  """bots_pb2.BotGroup => BotGroupConfig.

  Assumes body of bots_pb2.BotGroup is already validated (logs inconsistencies,
  but does not fail).
  """
  dimensions = {unicode(k): set() for k in trusted_dimensions}
  for dim_kv_pair in msg.dimensions:
    # In validated config 'dim_kv_pair' is always 'key:value', but be cautious.
    parts = unicode(dim_kv_pair).split(':', 1)
    if len(parts) != 2:
      logging.error('Invalid dimension in bots.cfg - "%s"', dim_kv_pair)
      continue
    k, v = parts[0], parts[1]
    dimensions.setdefault(k, set()).add(v)

  auth_cfg = msg.auth or bots_pb2.BotAuth()

  return _make_bot_group_config(
    require_luci_machine_token=auth_cfg.require_luci_machine_token,
    require_service_account=auth_cfg.require_service_account,
    ip_whitelist=auth_cfg.ip_whitelist,
    owners=tuple(msg.owners),
    dimensions={k: sorted(v) for k, v in dimensions.iteritems()})


def _expand_bot_id_expr(expr):
  """Expands string with bash-like sets (if they are there).

  E.g. takes "vm{1..3}-m1" and yields "vm1-m1", "vm2-m1", "vm3-m1". Also
  supports list syntax ({1,2,3}). Either one should be used, but not both, e.g.
  following WILL NOT work: {1..3,4,5}.

  Yields original string if it doesn't have '{...}' section.

  Raises ValueError if expression has invalid format.
  """
  if not expr:
    raise ValueError('empty bot_id is not allowed')

  left = expr.find('{')
  right = expr.rfind('}')

  if left == -1 and right == -1:
    yield expr
    return

  if expr.count('{') > 1 or expr.count('}') > 1 or left > right:
    raise ValueError('bad bot_id set expression')

  prefix, body, suffix = expr[:left], expr[left+1:right], expr[right+1:]

  # An explicit list?
  if ',' in body:
    # '..' is probably a mistake then.
    if '..' in body:
      raise ValueError(
          '".." is appearing alongside "," in "%s", probably a mistake' % body)
    for itm in body.split(','):
      yield prefix + itm + suffix
    return

  # A range then ('<start>..<end>').
  start, sep, end = body.partition('..')
  if sep != '..':
    raise ValueError('Invalid set "%s", not a list and not a range' % body)
  try:
    start = int(start)
  except ValueError:
    raise ValueError('Not a valid range start "%s"' % start)
  try:
    end = int(end)
  except ValueError:
    raise ValueError('Not a valid range end "%s"' % end)
  for i in xrange(start, end+1):
    yield prefix + str(i) + suffix


@utils.cache_with_expiration(60)
def fetch_machine_types():
  """Returns a dict of MachineTypes contained in bots.cfg.

  Returns:
    A dict mapping the name of a MachineType to a bots_pb2.MachineType.
  """
  cfg = _fetch_bots_config()
  if not cfg:
    return {}

  machine_types = {}
  for bot_group in cfg.bot_group:
    for mt in bot_group.machine_type:
      machine_types[mt.name] = mt

  return machine_types


def _fetch_bots_config():
  """Fetches bots.cfg."""
  # store_last_good=True tells config components to update the config file
  # in a cron job. Here we juts read from the datastore. In case it's the first
  # call ever, or config doesn't exist, it returns (None, None).
  rev, cfg = config.get_self_config(
      BOTS_CFG_FILENAME, bots_pb2.BotsCfg, store_last_good=True)
  if cfg:
    logging.debug('Using bots.cfg at rev %s', rev)
    # Callers can assume the config is already validated (as promised by
    # components.config). There should be no error at this point.
  return cfg


@utils.cache_with_expiration(60)
def _fetch_bot_groups():
  """Loads bots.cfg and parses it into _BotGroups struct.

  If bots.cfg doesn't exist, returns default config that allows any caller from
  'bots' IP whitelist to act as a bot.
  """
  cfg = _fetch_bots_config()
  if not cfg:
    logging.info('Didn\'t find bots.cfg, using default')
    return _DEFAULT_BOT_GROUPS

  direct_matches = {}
  prefix_matches = []
  machine_types = {}
  default_group = None

  for entry in cfg.bot_group:
    group_cfg = _bot_group_proto_to_tuple(entry, cfg.trusted_dimensions or [])

    for bot_id_expr in entry.bot_id:
      try:
        for bot_id in _expand_bot_id_expr(bot_id_expr):
          # This should not happen in validated config. If it does, log the
          # error, but carry on, since dying here will bring service offline.
          if bot_id in direct_matches:
            logging.error(
                'Bot "%s" is specified in two different bot groups', bot_id)
            continue
          direct_matches[bot_id] = group_cfg
      except ValueError as exc:
        logging.error('Invalid bot_id expression "%s": %s', bot_id_expr, exc)

    for bot_id_prefix in entry.bot_id_prefix:
      if not bot_id_prefix:
        logging.error('Skipping empty bot_id_prefix')
        continue
      prefix_matches.append((bot_id_prefix, group_cfg))

    for machine_type in entry.machine_type:
      machine_types[machine_type.name] = group_cfg

    # Default group?
    if not entry.bot_id and not entry.bot_id_prefix and not entry.machine_type:
      if default_group is not None:
        logging.error('Default bot group is specified twice')
      else:
        default_group = group_cfg

  return _BotGroups(
      direct_matches, prefix_matches, machine_types, default_group)


@validation.self_rule(BOTS_CFG_FILENAME, bots_pb2.BotsCfg)
def validate_settings(cfg, ctx):
  """Validates bots.cfg file."""
  with ctx.prefix('trusted_dimensions: '):
    for dim_key in cfg.trusted_dimensions:
      if not local_config.validate_dimension_key(dim_key):
        ctx.error('invalid dimension key %r', dim_key)

  # Explicitly mentioned bot_id => index of a group where it was mentioned.
  bot_ids = {}
  # bot_id_prefix => index of a group where it was defined.
  bot_id_prefixes = {}
  # machine_type names.
  machine_type_names = set()

  for i, entry in enumerate(cfg.bot_group):
    with ctx.prefix('bot_group #%d: ', i):
      # Validate bot_id field and make sure bot_id groups do not intersect.
      for bot_id_expr in entry.bot_id:
        try:
          for bot_id in _expand_bot_id_expr(bot_id_expr):
            if bot_id in bot_ids:
              ctx.error(
                  'bot_id "%s" was already mentioned in group #%d',
                  bot_id, bot_ids[bot_id])
              continue
            bot_ids[bot_id] = i
        except ValueError as exc:
          ctx.error('bad bot_id expression "%s" - %s', bot_id_expr, exc)

      # Validate bot_id_prefix. Later (when we know all the prefixes) we will
      # check that they do not intersect.
      for bot_id_prefix in entry.bot_id_prefix:
        if not bot_id_prefix:
          ctx.error('empty bot_id_prefix is not allowed')
          continue
        if bot_id_prefix in bot_id_prefixes:
          ctx.error(
              'bot_id_prefix "%s" is already specified in group #%d',
              bot_id_prefix, bot_id_prefixes[bot_id_prefix])
          continue
        bot_id_prefixes[bot_id_prefix] = i

      # Validate machine_type.
      for i, machine_type in enumerate(entry.machine_type):
        with ctx.prefix('machine_type #%d: ', i):
          if not machine_type.name:
            ctx.error('name is required')
            continue
          if machine_type.name in machine_type_names:
            ctx.error('reusing name "%s"', machine_type.name)
            continue
          machine_type_names.add(machine_type.name)
          if not machine_type.lease_duration_secs:
            ctx.error('lease_duration_secs is required')
            continue
          if machine_type.lease_duration_secs < 0:
            ctx.error('lease_duration_secs must be positive')
            continue
          if not machine_type.mp_dimensions:
            ctx.error('at least one dimension is required')
            continue
          for j, dim in enumerate(machine_type.mp_dimensions):
            with ctx.prefix('mp_dimensions #%d: ', j):
              if ':' not in dim:
                ctx.error('bad dimension "%s", not a key:value pair', dim)
                continue
          if not machine_type.target_size:
            ctx.error('target_size is required')
            continue
          if machine_type.target_size < 0:
            ctx.error('target_size must be positive')
            continue

      # Validate 'auth' field.
      a = entry.auth
      if a.require_luci_machine_token and a.require_service_account:
        ctx.error(
            'require_luci_machine_token and require_service_account can\'t '
            'both be used at the same time')
      if not a.require_luci_machine_token and not a.require_service_account:
        if not a.ip_whitelist:
          ctx.error(
            'if both require_luci_machine_token and require_service_account '
            'are unset, ip_whitelist is required')
      if a.require_service_account:
        try:
          auth.Identity(auth.IDENTITY_USER, a.require_service_account)
        except ValueError:
          ctx.error(
              'invalid service account email "%s"', a.require_service_account)
      if a.ip_whitelist and not auth.is_valid_ip_whitelist_name(a.ip_whitelist):
        ctx.error('invalid ip_whitelist name "%s"', a.ip_whitelist)

      # Validate 'owners'. Just check they are emails.
      for own in entry.owners:
        try:
          auth.Identity(auth.IDENTITY_USER, own)
        except ValueError:
          ctx.error('invalid owner email "%s"', own)

      # Validate 'dimensions'.
      for dim in entry.dimensions:
        if not local_config.validate_flat_dimension(dim):
          ctx.error('bad dimension %r', dim)

  # Now verify bot_id_prefix is never a prefix of other prefix. It causes
  # ambiguities.
  for smaller, s_idx in bot_id_prefixes.iteritems():
    for larger, l_idx in bot_id_prefixes.iteritems():
      if smaller == larger:
        continue # we've already checked prefixes have no duplicated
      if larger.startswith(smaller):
        ctx.error(
            'bot_id_prefix "%s", defined in group #%d, is subprefix of "%s", '
            'defined in group #%d; it makes group assigned for bots with '
            'prefix "%s" ambigious', smaller, s_idx, larger, l_idx, larger)

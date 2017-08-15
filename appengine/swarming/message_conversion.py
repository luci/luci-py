# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""This module facilitates conversion from dictionaries to ProtoRPC messages.

Given a dictionary whose keys' names and values' types comport with the
fields defined for a protorpc.messages.Message subclass, this module tries to
generate a Message instance that corresponds to the provided dictionary. The
"normal" use case is for ndb.Models which need to be represented as a
ProtoRPC.
"""

import datetime
import json

import swarming_rpcs

from components import utils
from server import task_pack
from server import task_request
from server import task_result


### Private API.


def _string_pairs_from_dict(dictionary):
  return [
    swarming_rpcs.StringPair(key=k, value=v)
    for k, v in sorted((dictionary or {}).iteritems())
  ]


def _string_list_pairs_from_dict(dictionary):
  return [
    swarming_rpcs.StringListPair(key=k, value=v)
    for k, v in sorted((dictionary or {}).iteritems())
  ]


def _ndb_to_rpc(cls, entity, **overrides):
  members = (f.name for f in cls.all_fields())
  kwargs = {m: getattr(entity, m) for m in members if not m in overrides}
  kwargs.update(overrides)
  return cls(**{k: v for k, v in kwargs.iteritems() if v is not None})


def _rpc_to_ndb(cls, entity, **overrides):
  kwargs = {
    m: getattr(entity, m) for m in cls._properties if not m in overrides
  }
  kwargs.update(overrides)
  return cls(**{k: v for k, v in kwargs.iteritems() if v is not None})


### Public API.


def epoch_to_datetime(value):
  """Converts a messages.FloatField that represents a timestamp since epoch in
  seconds to a datetime.datetime.

  Returns None when input is 0 or None.
  """
  if not value:
    return None
  try:
    return utils.timestamp_to_datetime(value*1000000.)
  except OverflowError as e:
    raise ValueError(e)


def bot_info_to_rpc(entity, now, deleted=False):
  """"Returns a swarming_rpcs.BotInfo from a bot.BotInfo."""
  return _ndb_to_rpc(
      swarming_rpcs.BotInfo,
      entity,
      bot_id=entity.id,
      deleted=deleted,
      dimensions=_string_list_pairs_from_dict(entity.dimensions),
      is_dead=entity.is_dead(now),
      machine_type=entity.machine_type,
      state=json.dumps(entity.state, sort_keys=True, separators=(',', ':')))


def bot_event_to_rpc(entity):
  """"Returns a swarming_rpcs.BotEvent from a bot.BotEvent."""
  return _ndb_to_rpc(
      swarming_rpcs.BotEvent,
      entity,
      dimensions=_string_list_pairs_from_dict(entity.dimensions),
      state=json.dumps(entity.state, sort_keys=True, separators=(',', ':')),
      task_id=entity.task_id if entity.task_id else None)


def task_request_to_rpc(entity):
  """"Returns a swarming_rpcs.TaskRequest from a task_request.TaskRequest."""
  assert entity.__class__ is task_request.TaskRequest
  cipd_input = None
  if entity.properties.cipd_input:
    client_package = None
    if entity.properties.cipd_input.client_package:
      client_package = _ndb_to_rpc(
          swarming_rpcs.CipdPackage,
          entity.properties.cipd_input.client_package)
    cipd_input = _ndb_to_rpc(
        swarming_rpcs.CipdInput,
        entity.properties.cipd_input,
        client_package=client_package,
        packages=[
          _ndb_to_rpc(swarming_rpcs.CipdPackage, p)
          for p in entity.properties.cipd_input.packages
        ])

  inputs_ref = None
  if entity.properties.inputs_ref:
    inputs_ref = _ndb_to_rpc(
        swarming_rpcs.FilesRef, entity.properties.inputs_ref)

  props = entity.properties
  properties = _ndb_to_rpc(
      swarming_rpcs.TaskProperties,
      props,
      caches=[_ndb_to_rpc(swarming_rpcs.CacheEntry, c) for c in props.caches],
      cipd_input=cipd_input,
      secret_bytes='<REDACTED>' if props.has_secret_bytes else None,
      dimensions=_string_pairs_from_dict(props.dimensions),
      env=_string_pairs_from_dict(props.env),
      inputs_ref=inputs_ref)

  return _ndb_to_rpc(
      swarming_rpcs.TaskRequest,
      entity,
      authenticated=entity.authenticated.to_bytes(),
      properties=properties)


def new_task_request_from_rpc(msg, now):
  """"Returns a (task_request.TaskRequest, task_request.SecretBytes) from
  a swarming_rpcs.NewTaskRequest.

  If secret_bytes were not included in the rpc, the SecretBytes entity will be
  None.
  """
  assert msg.__class__ is swarming_rpcs.NewTaskRequest
  props = msg.properties
  if not props:
    raise ValueError('properties is required')

  cipd_input = None
  if props.cipd_input:
    client_package = None
    if props.cipd_input.client_package:
      client_package = _rpc_to_ndb(
          task_request.CipdPackage, props.cipd_input.client_package)
    cipd_input = _rpc_to_ndb(
        task_request.CipdInput,
        props.cipd_input,
        client_package=client_package,
        packages=[
          _rpc_to_ndb(task_request.CipdPackage, p)
          for p in props.cipd_input.packages
        ])

  inputs_ref = None
  if props.inputs_ref:
    inputs_ref = _rpc_to_ndb(task_request.FilesRef, props.inputs_ref)

  secret_bytes = None
  if props.secret_bytes:
    secret_bytes = task_request.SecretBytes(secret_bytes=props.secret_bytes)

  if len(set(i.key for i in props.dimensions)) != len(props.dimensions):
    raise ValueError('same dimension key cannot be specified twice')
  if len(set(i.key for i in props.env)) != len(props.env):
    raise ValueError('same environment variable key cannot be specified twice')

  properties = _rpc_to_ndb(
      task_request.TaskProperties,
      props,
      caches=[_rpc_to_ndb(task_request.CacheEntry, c) for c in props.caches],
      cipd_input=cipd_input,
      # Passing command=None is supported at API level but not at NDB level.
      command=props.command or [],
      has_secret_bytes=secret_bytes is not None,
      secret_bytes=None, # ignore this, it's handled out of band
      dimensions={i.key: i.value for i in props.dimensions},
      env={i.key: i.value for i in props.env},
      inputs_ref=inputs_ref)

  req = _rpc_to_ndb(
      task_request.TaskRequest,
      msg,
      created_ts=now,
      expiration_ts=now+datetime.timedelta(seconds=msg.expiration_secs),
      # It is set in task_request.init_new_request().
      authenticated=None,
      properties=properties,
      # It is set in task_request.init_new_request().
      properties_hash=None,
      # Need to convert it to 'str', it is BlobProperty. _rpc_to_ndb raises an
      # error otherwise. TODO(crbug.com/731847): Remove this.
      service_account_token=
          str(msg.service_account_token) if msg.service_account_token else None)

  return req, secret_bytes


def task_result_to_rpc(entity, send_stats):
  """"Returns a swarming_rpcs.TaskResult from a task_result.TaskResultSummary or
  task_result.TaskRunResult.
  """
  outputs_ref = (
      _ndb_to_rpc(swarming_rpcs.FilesRef, entity.outputs_ref)
      if entity.outputs_ref else None)
  cipd_pins = None
  if entity.cipd_pins:
    cipd_pins = swarming_rpcs.CipdPins(
      client_package=(
        _ndb_to_rpc(swarming_rpcs.CipdPackage,
                    entity.cipd_pins.client_package)
        if entity.cipd_pins.client_package else None
      ),
      packages=[
        _ndb_to_rpc(swarming_rpcs.CipdPackage, pkg)
        for pkg in entity.cipd_pins.packages
      ] if entity.cipd_pins.packages else None
    )
  performance_stats = None
  if send_stats and entity.performance_stats.is_valid:
      def op(entity):
        if entity:
          return _ndb_to_rpc(swarming_rpcs.OperationStats, entity)

      performance_stats = _ndb_to_rpc(
          swarming_rpcs.PerformanceStats,
          entity.performance_stats,
          isolated_download=op(entity.performance_stats.isolated_download),
          isolated_upload=op(entity.performance_stats.isolated_upload))
  kwargs = {
    'bot_dimensions': _string_list_pairs_from_dict(entity.bot_dimensions or {}),
    'cipd_pins': cipd_pins,
    'outputs_ref': outputs_ref,
    'performance_stats': performance_stats,
    'state': swarming_rpcs.StateField(entity.state),
  }
  if entity.__class__ is task_result.TaskRunResult:
    kwargs['costs_usd'] = []
    if entity.cost_usd is not None:
      kwargs['costs_usd'].append(entity.cost_usd)
    kwargs['properties_hash'] = None
    kwargs['tags'] = []
    kwargs['user'] = None
    kwargs['run_id'] = entity.task_id
  else:
    assert entity.__class__ is task_result.TaskResultSummary, entity
    kwargs['properties_hash'] = (
        entity.properties_hash.encode('hex')
        if entity.properties_hash else None)
    # This returns the right value for deduped tasks too.
    k = entity.run_result_key
    kwargs['run_id'] = task_pack.pack_run_result_key(k) if k else None
  return _ndb_to_rpc(swarming_rpcs.TaskResult, entity, **kwargs)

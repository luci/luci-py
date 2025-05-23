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
import logging

import swarming_rpcs

from components import utils
from server import task_pack
from server import task_request
from server import task_result


### Private API.


def _string_pairs_from_dict(dictionary):
  # For key: value items like env.
  return [
    swarming_rpcs.StringPair(key=k, value=v)
    for k, v in sorted((dictionary or {}).items())
  ]


def _duplicate_string_pairs_from_dict(dictionary):
  # For compatibility due to legacy swarming_rpcs.TaskProperties.dimensions.
  out = []
  for k, values in (dictionary or {}).items():
    assert isinstance(values, (list, tuple)), dictionary
    for v in values:
      out.append(swarming_rpcs.StringPair(key=k, value=v))
  return out


def _string_list_pairs_from_dict(dictionary):
  # For key: values items like bot dimensions.
  return [
    swarming_rpcs.StringListPair(key=k, value=v)
    for k, v in sorted((dictionary or {}).items())
  ]


def _ndb_to_rpc(cls, entity, **overrides):
  members = (f.name for f in cls.all_fields())
  kwargs = {m: getattr(entity, m) for m in members if not m in overrides}
  kwargs.update(overrides)
  return cls(**{k: v for k, v in kwargs.items() if v is not None})


def _rpc_to_ndb(cls, entity, **overrides):
  kwargs = {
    m: getattr(entity, m) for m in cls._properties if not m in overrides
  }
  kwargs.update(overrides)
  return cls(**{k: v for k, v in kwargs.items() if v is not None})


def _taskproperties_from_rpc(props):
  """Converts a swarming_rpcs.TaskProperties to a task_request.TaskProperties.
  """
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

  containment = task_request.Containment()
  if props.containment:
    containment = task_request.Containment(
        containment_type=int(props.containment.containment_type or 0))

  cas_input_root = None
  if props.cas_input_root:
    digest = _rpc_to_ndb(task_request.Digest, props.cas_input_root.digest)
    cas_input_root = _rpc_to_ndb(
        task_request.CASReference, props.cas_input_root, digest=digest)

  secret_bytes = None
  if props.secret_bytes:
    secret_bytes = task_request.SecretBytes(secret_bytes=props.secret_bytes)

  if len(set(i.key for i in props.env)) != len(props.env):
    raise ValueError('same environment variable key cannot be specified twice')
  if len(set(i.key for i in props.env_prefixes)) != len(props.env_prefixes):
    raise ValueError('same environment prefix key cannot be specified twice')
  dims = {}
  for i in props.dimensions:
    dims.setdefault(i.key, []).append(i.value)
  out = _rpc_to_ndb(
      task_request.TaskProperties,
      props,
      caches=[_rpc_to_ndb(task_request.CacheEntry, c) for c in props.caches],
      cipd_input=cipd_input,
      # Passing command=None is supported at API level but not at NDB level.
      command=props.command or [],
      containment=containment,
      has_secret_bytes=secret_bytes is not None,
      secret_bytes=None,  # ignore this, it's handled out of band
      dimensions=None,  # it's named dimensions_data
      dimensions_data=dims,
      env={i.key: i.value
           for i in props.env},
      env_prefixes={i.key: i.value
                    for i in props.env_prefixes},
      inputs_ref=None,  # TODO(crbug.com/1255535): Deprecated.
      cas_input_root=cas_input_root)
  return out, secret_bytes


def _taskproperties_to_rpc(props):
  """Converts a task_request.TaskProperties to a swarming_rpcs.TaskProperties.
  """
  cipd_input = None
  if props.cipd_input:
    client_package = None
    if props.cipd_input.client_package:
      client_package = _ndb_to_rpc(
          swarming_rpcs.CipdPackage,
          props.cipd_input.client_package)
    cipd_input = _ndb_to_rpc(
        swarming_rpcs.CipdInput,
        props.cipd_input,
        client_package=client_package,
        packages=[
          _ndb_to_rpc(swarming_rpcs.CipdPackage, p)
          for p in props.cipd_input.packages
        ])

  containment = swarming_rpcs.Containment()
  if props.containment:
    containment = swarming_rpcs.Containment(
        containment_type=swarming_rpcs.ContainmentType(
            props.containment.containment_type or 0))

  cas_input_root = None
  if props.cas_input_root:
    digest = _ndb_to_rpc(swarming_rpcs.Digest, props.cas_input_root.digest)
    cas_input_root = _ndb_to_rpc(
        swarming_rpcs.CASReference, props.cas_input_root, digest=digest)

  return _ndb_to_rpc(
      swarming_rpcs.TaskProperties,
      props,
      caches=[_ndb_to_rpc(swarming_rpcs.CacheEntry, c) for c in props.caches],
      cipd_input=cipd_input,
      containment=containment,
      secret_bytes='<REDACTED>' if props.has_secret_bytes else None,
      dimensions=_duplicate_string_pairs_from_dict(props.dimensions),
      env=_string_pairs_from_dict(props.env),
      env_prefixes=_string_list_pairs_from_dict(props.env_prefixes or {}),
      cas_input_root=cas_input_root)


def _taskslice_from_rpc(msg):
  """Converts a swarming_rpcs.TaskSlice to a task_request.TaskSlice."""
  props, secret_bytes = _taskproperties_from_rpc(msg.properties)
  out = _rpc_to_ndb(task_request.TaskSlice,
                    msg,
                    properties=props,
                    properties_hash=None)
  return out, secret_bytes


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


def bot_info_to_rpc(entity, deleted=False):
  """"Returns a swarming_rpcs.BotInfo from a bot.BotInfo."""
  return _ndb_to_rpc(
      swarming_rpcs.BotInfo,
      entity,
      bot_id=entity.id,
      deleted=deleted,
      dimensions=_string_list_pairs_from_dict(entity.dimensions),
      is_dead=entity.is_dead,
      # Deprecated. TODO(crbug/897355): Remove.
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
  slices = []
  for i in range(entity.num_task_slices):
    t = entity.task_slice(i)
    slices.append(
        _ndb_to_rpc(
            swarming_rpcs.TaskSlice,
            t,
            properties=_taskproperties_to_rpc(t.properties)))

  resultdb = None
  if entity.resultdb:
    resultdb = _ndb_to_rpc(swarming_rpcs.ResultDBCfg, entity.resultdb)

  return _ndb_to_rpc(
      swarming_rpcs.TaskRequest,
      entity,
      authenticated=entity.authenticated.to_bytes(),
      # For some amount of time, the properties will be copied into the
      # task_slices and vice-versa, to give time to the clients to update.
      properties=slices[0].properties,
      task_slices=slices,
      resultdb=resultdb)


def new_task_request_from_rpc(msg, now):
  """"Returns a (task_request.TaskRequest, task_request.SecretBytes,
  task_request.TemplateApplyEnum) from a swarming_rpcs.NewTaskRequest.

  If secret_bytes were not included in the rpc, the SecretBytes entity will be
  None.
  """
  assert msg.__class__ is swarming_rpcs.NewTaskRequest
  if msg.task_slices and msg.properties:
    raise ValueError('Specify one of properties or task_slices, not both')

  if msg.properties:
    logging.info('Properties is still used')
    if not msg.expiration_secs:
      raise ValueError('missing expiration_secs')
    props, secret_bytes = _taskproperties_from_rpc(msg.properties)
    slices = [
      task_request.TaskSlice(
          properties=props, expiration_secs=msg.expiration_secs),
    ]
  elif msg.task_slices:
    if msg.expiration_secs:
      raise ValueError(
          'When using task_slices, do not specify a global expiration_secs')
    secret_bytes = None
    slices = []
    for t in (msg.task_slices or []):
      sl, se = _taskslice_from_rpc(t)
      slices.append(sl)
      if se:
        if secret_bytes and se != secret_bytes:
          raise ValueError(
              'When using secret_bytes multiple times, all values must match')
        secret_bytes = se
  else:
    raise ValueError('Specify one of properties or task_slices')

  pttf = swarming_rpcs.PoolTaskTemplateField
  template_apply = {
    pttf.AUTO: task_request.TEMPLATE_AUTO,
    pttf.CANARY_NEVER: task_request.TEMPLATE_CANARY_NEVER,
    pttf.CANARY_PREFER: task_request.TEMPLATE_CANARY_PREFER,
    pttf.SKIP: task_request.TEMPLATE_SKIP,
  }[msg.pool_task_template]

  resultdb = None
  if msg.resultdb:
    resultdb = _rpc_to_ndb(task_request.ResultDBCfg, msg.resultdb)

  req = _rpc_to_ndb(
      task_request.TaskRequest,
      msg,
      created_ts=now,
      expiration_ts=None,
      # It is set in task_request.init_new_request().
      authenticated=None,
      properties=None,
      task_slices=slices,
      # 'tags' is now generated from manual_tags plus automatic tags.
      tags=None,
      manual_tags=msg.tags,
      # This is internal field not settable via RPC.
      root_task_id=None,
      realms_enabled=None,
      resultdb_update_token=None,
      resultdb=resultdb,
      pool_task_template=None,  # handled out of band
      has_build_task=False,
      scheduling_algorithm=None,
      rbe_instance=None,
      txn_uuid=None,
      disable_external_scheduler=None)

  return req, secret_bytes, template_apply


def task_result_to_rpc(entity, send_stats):
  """"Returns a swarming_rpcs.TaskResult from a task_result.TaskResultSummary or
  task_result.TaskRunResult.
  """
  cas_output_root = None
  if entity.cas_output_root:
    digest = _ndb_to_rpc(swarming_rpcs.Digest, entity.cas_output_root.digest)
    cas_output_root = _ndb_to_rpc(
        swarming_rpcs.CASReference, entity.cas_output_root, digest=digest)
  cipd_pins = None
  if entity.cipd_pins:
    cipd_pins = swarming_rpcs.CipdPins(
        client_package=(_ndb_to_rpc(swarming_rpcs.CipdPackage,
                                    entity.cipd_pins.client_package)
                        if entity.cipd_pins.client_package else None),
        packages=[
            _ndb_to_rpc(swarming_rpcs.CipdPackage, pkg)
            for pkg in entity.cipd_pins.packages
        ] if entity.cipd_pins.packages else [])

  resultdb_info = None
  if entity.resultdb_info:
    resultdb_info = swarming_rpcs.ResultDBInfo(
        hostname=entity.resultdb_info.hostname,
        invocation=entity.resultdb_info.invocation,
    )

  missing_cas = None
  if entity.missing_cas:
    missing_cas = []
    for pkg in entity.missing_cas:
      digest = _ndb_to_rpc(swarming_rpcs.Digest, pkg.digest)
      missing_cas.append(
          _ndb_to_rpc(swarming_rpcs.CASReference, pkg, digest=digest)
      )

  missing_cipd = None
  if entity.missing_cipd:
    missing_cipd = [
      _ndb_to_rpc(swarming_rpcs.CipdPackage, pkg)
      for pkg in entity.missing_cipd
    ]

  performance_stats = None
  if send_stats and entity.performance_stats.is_valid:

    def op(entity):
      if not entity:
        return None
      return _ndb_to_rpc(swarming_rpcs.OperationStats, entity)

    def cas_op(entity):
      if not entity:
        return None
      return _ndb_to_rpc(swarming_rpcs.CASOperationStats, entity)

    perf_entity = entity.performance_stats
    performance_stats = _ndb_to_rpc(
        swarming_rpcs.PerformanceStats,
        entity.performance_stats,
        cache_trim=op(perf_entity.cache_trim),
        package_installation=op(perf_entity.package_installation),
        named_caches_install=op(perf_entity.named_caches_install),
        named_caches_uninstall=op(perf_entity.named_caches_uninstall),
        isolated_download=cas_op(perf_entity.isolated_download),
        isolated_upload=cas_op(perf_entity.isolated_upload),
        cleanup=op(perf_entity.cleanup))
  kwargs = {
      'bot_dimensions':
          _string_list_pairs_from_dict(entity.bot_dimensions or {}),
      'cipd_pins':
          cipd_pins,
      'cas_output_root':
          cas_output_root,
      'performance_stats':
          performance_stats,
      'state':
          swarming_rpcs.TaskState(entity.state),
      'resultdb_info':
          resultdb_info,
      'missing_cas':
          missing_cas,
      'missing_cipd':
          missing_cipd,
  }
  if entity.__class__ is task_result.TaskRunResult:
    kwargs['costs_usd'] = []
    if entity.cost_usd is not None:
      kwargs['costs_usd'].append(entity.cost_usd)
    kwargs['tags'] = []
    kwargs['user'] = None
    kwargs['run_id'] = entity.task_id
  else:
    assert entity.__class__ is task_result.TaskResultSummary, entity
    # This returns the right value for deduped tasks too.
    k = entity.run_result_key
    kwargs['run_id'] = task_pack.pack_run_result_key(k) if k else None
  return _ndb_to_rpc(
      swarming_rpcs.TaskResult,
      entity,
      **kwargs)

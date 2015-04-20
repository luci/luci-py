# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""This module facilitates conversion from dictionaries to ProtoRPC messages.

Given a dictionary whose keys' names and values' types comport with the
fields defined for a protorpc.messages.Message subclass, this module tries to
generate a Message instance that corresponds to the provided dictionary. The
"normal" use case is for ndb.Models which need to be represented as a
ProtoRPC.
"""

import datetime
import functools
import logging
import re

import endpoints
from protorpc import message_types
from protorpc import messages
from protorpc import remote

import swarming_rpcs

from components import utils


def _parse_date(date):
  if isinstance(date, datetime.datetime):
    return date
  for f in utils.VALID_DATETIME_FORMATS:
    try:
      return datetime.datetime.strptime(date, f)
    except ValueError:
      continue
  return None


def _populate_rpc(message_class, entity_dict, field_map, name=None):
  if name is None:
    name = message_class.__name__
  result = message_class()
  for field, (functor, required) in field_map.iteritems():
    # "normal" case: fields match and all is well
    if field in entity_dict:

      # just pass the argument if no functor is supplied
      if functor is None:
        functor = lambda x: x
      value = entity_dict[field]

      # values of None are to be omitted
      if value is not None:
        value = functor(value)
        setattr(result, field, value)

    # ignore optional absent fields; raise exception for required absent fields
    elif required:
      raise endpoints.BadRequestException(
          '%s is missing required field %s.' % (name, field))
  return result


def _data_from_dict(pair_list):
  return [swarming_rpcs.StringPair(key=k, value=v) for k, v in pair_list]


def _string_pairs_from_dict(dictionary):
  return [swarming_rpcs.StringPair(
      key=k, value=v) for k, v in sorted(dictionary.iteritems())]


def _string_list_pairs_from_dict(dictionary):
  return [swarming_rpcs.StringListPair(
      key=k, value=v) for k, v in sorted(dictionary.iteritems())]


def _timestamp_to_secs(start, finish):
  start = _parse_date(start)
  finish = _parse_date(finish)
  return int((finish - start).total_seconds())


def _wrap_in_list(item):
  if isinstance(item, (list, tuple)):
    return item
  return [item]


def properties_from_dict(entity_dict):
  entity_dict = entity_dict.copy()
  entity_dict['command'] = entity_dict['commands'][0]
  field_map = {
    'command': (None, False),
    'data': (_data_from_dict, False),
    'dimensions': (_string_pairs_from_dict, False),
    'env': (_string_pairs_from_dict, False),
    'execution_timeout_secs': (None, False),
    'grace_period_secs': (None, False),
    'idempotent': (None, False),
    'io_timeout_secs': (None, False)}
  return _populate_rpc(swarming_rpcs.TaskProperty, entity_dict, field_map)


def bot_info_from_dict(entity_dict):
  field_map = {
    'dimensions': (_string_list_pairs_from_dict, False),
    'external_ip': (None, False),
    'first_seen_ts': (_parse_date, False),
    'id': (None, False),
    'is_busy': (None, False),
    'last_seen_ts': (_parse_date, False),
    'quarantined': (None, False),
    'task_id': (None, False),
    'task_name': (None, False),
    'version': (None, False)}
  return _populate_rpc(swarming_rpcs.BotInfo, entity_dict, field_map)


def task_request_from_dict(entity_dict):
  entity_dict = entity_dict.copy()
  for field in ['created_ts', 'expiration_ts']:
    if entity_dict.get(field) is None:
      raise endpoints.BadRequestException(
          'TaskRequest is missing required field %s.' % field)
  entity_dict['expiration_secs'] = _timestamp_to_secs(
      entity_dict['created_ts'], entity_dict['expiration_ts'])
  entity_dict['tag'] = entity_dict.pop('tags', [])
  field_map = {
    'authenticated': (
      lambda x: '='.join(x) if isinstance(x, (list, tuple)) else x, False),
    'created_ts': (_parse_date, False),
    'expiration_secs': (None, True),
    'name': (None, True),
    'parent_task_id': (None, False),
    'properties': (properties_from_dict, True),
    'priority': (None, False),
    'tag': (None, False),
    'user': (None, False)}
  return _populate_rpc(swarming_rpcs.TaskRequest, entity_dict, field_map)


def _task_result_common_from_dict(
    entity_dict, entity_type, additional_fields):
  entity_dict = entity_dict.copy()
  durations = entity_dict.get('durations')
  exit_codes = entity_dict.get('exit_codes')
  if durations:
    entity_dict['duration'] = durations[0]
  if exit_codes:
    entity_dict['exit_code'] = exit_codes[0]

  # field map of shared attributes
  field_map = {
    'abandoned_ts': (_parse_date, False),
    'bot_id': (None, False),
    'children_task_ids': (None, False),
    'completed_ts': (_parse_date, False),
    'cost_saved_usd': (None, False),
    'created_ts': (_parse_date, False),
    'deduped_from': (None, False),
    'duration': (None, False),
    'exit_code': (None, False),
    'failure': (None, False),
    'id': (None, False),
    'internal_failure': (None, False),
    'modified_ts': (_parse_date, False),
    'started_ts': (_parse_date, False),
    'state': (swarming_rpcs.StateField, False),
    'try_number': (None, False)}

  # add any additional attributes
  field_map.update(additional_fields)
  return _populate_rpc(entity_type, entity_dict, field_map)


def task_result_summary_from_dict(entity_dict):
  field_map = {
    'costs_usd': (_wrap_in_list, False),
    'name': (None, False),
    'user': (None, False)}
  return _task_result_common_from_dict(
      entity_dict, swarming_rpcs.TaskResultSummary, field_map)


def task_run_result_from_dict(entity_dict):
  field_map = {'cost_usd': (None, False)}
  return _task_result_common_from_dict(
      entity_dict, swarming_rpcs.TaskRunResult, field_map)

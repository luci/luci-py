# Copyright 2022 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.
"""Converts database entities to and from swarming api prpc objects to ndb
database objects.
"""

import json
from datetime import datetime
from google.protobuf.timestamp_pb2 import Timestamp
import proto.api_v2.swarming_pb2 as swarming


def _string_pairs_from_dict(dictionary):
  """Used for items like environment variables.
  Expects a dictionary with the form <string, string> and returns
  a list of swarming.StringPair. Will also sort the list by key.
  """
  return [
      swarming.StringPair(key=k, value=v)
      for k, v in sorted((dictionary or {}).items())
  ]


def _duplicate_string_pairs_from_dict(dictionary):
  """Expects a dictionary with the type of string, list[string]. It flattens
  dictionary into a list of StringPair.

  For example:
  a: [b, c]
  b: [c, d]

  would be converted to:

  [(a, b), (a, c), (b, c), (b, d)]
  """
  # For compatibility due to legacy swarming_rpcs.TaskProperties.dimensions.
  # TaskProperties dimensions are stored as a list of StringPair instead of
  # deduplicated StringListPair. Having dimensions in this form is legacy
  # behaviour from old protorpc api.
  out = []
  for k, values in (dictionary or {}).items():
    assert isinstance(values, (list, tuple)), dictionary
    out.extend(swarming.StringPair(key=k, value=v) for v in values)
  return out


def _string_list_pairs_from_dict(dictionary):
  # For key: values items like bot dimensions.
  return [
      swarming.StringListPair(key=k, value=v)
      for k, v in sorted((dictionary or {}).items())
  ]


def date(ts):
  """Converts datetime.timestamp into google.protobuf.timestamp_pb2.Timestamp
  """
  if ts is None:
    return None
  stamp = Timestamp()
  stamp.FromDatetime(ts)
  return stamp


def _state(state_dict):
  return json.dumps(state_dict or {}, sort_keys=True, separators=(',', ':'))


def bot_info_to_proto(bot_info, deleted=False):
  """Converts a ndb BotInfo object into a BotInfoResponse for pRPC api
  """
  return swarming.BotInfo(external_ip=bot_info.external_ip,
                          authenticated_as=bot_info.authenticated_as,
                          is_dead=bot_info.is_dead,
                          quarantined=bot_info.quarantined,
                          maintenance_msg=bot_info.maintenance_msg,
                          task_id=bot_info.task_id,
                          task_name=bot_info.task_name,
                          version=bot_info.version,
                          first_seen_ts=date(bot_info.first_seen_ts),
                          last_seen_ts=date(bot_info.last_seen_ts),
                          state=_state(bot_info.state),
                          bot_id=bot_info.id,
                          dimensions=_string_list_pairs_from_dict(
                              bot_info.dimensions),
                          deleted=deleted)


def _bot_event_response(event):
  """Converts a ndb BotEvent entity to a BotEvent response of pRPC"""
  # must have a value because ts is indexed on
  assert event.ts
  return swarming.BotEventResponse(
      ts=date(event.ts),
      event_type=event.event_type,
      message=event.message,
      external_ip=event.external_ip,
      authenticated_as=event.authenticated_as,
      version=event.version,
      quarantined=event.quarantined,
      maintenance_msg=event.maintenance_msg,
      task_id=event.task_id,
      dimensions=_string_list_pairs_from_dict(event.dimensions),
      state=_state(event.state),
  )


def bot_events_response(items, cursor):
  return swarming.BotEventsResponse(
      now=date(datetime.utcnow()),
      items=[_bot_event_response(event) for event in items],
      cursor=cursor)

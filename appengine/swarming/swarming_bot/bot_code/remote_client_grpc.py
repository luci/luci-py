# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

# This is a reimplementation of RemoteClientNative but it uses (will use)
# a gRPC method to communicate with a server instead of REST.

import json

import grpc
import google.protobuf.json_format
from proto import swarming_bot_pb2


class RemoteClientGrpc(object):
  """RemoteClientGrpc knows how to make calls via gRPC.
  """

  def __init__(self, server):
    print "Communicating with host %s via gRPC" % server
    self._channel = grpc.insecure_channel(server)
    self._stub = swarming_bot_pb2.BotServiceStub(self._channel)

  def is_grpc(self):
    return True

  def initialize(self, quit_bit):
    pass

  @property
  def uses_auth(self):
    return False

  def get_authentication_headers(self):
    return {}

  def post_bot_event(self, event_type, message, attributes):
    del event_type, attributes # Not used yet
    print "Posting bot event: %s" % message

  def post_task_update(self, task_id, bot_id, params,
                       stdout_and_chunk=None, exit_code=None):
    del exit_code # Not used yet
    print "Posting task update for task %s, bot %s: %s" % \
      (task_id, bot_id, params)
    if stdout_and_chunk != None:
      print "stdout: %s" % stdout_and_chunk[0]
    return True

  def post_task_error(self, task_id, bot_id, message):
    del task_id, bot_id, message # Not used yet
    print "Posting task error"
    return True

  def do_handshake(self, attributes):
    request = swarming_bot_pb2.HandshakeRequest()
    request.attributes.version = attributes['version']
    dims = attributes['dimensions']
    for k in dims:
      pair = request.attributes.dimensions.add()
      pair.name = k
      for v in dims[k]:
        pair.values.append(v)
    create_state_proto(attributes['state'], request.attributes.state)
    response = self._stub.Handshake(request)
    resp = {
        'server_version': response.server_version,
        'bot_version': response.bot_version,
        'bot_group_cfg_version': response.bot_group_cfg_version,
        'bot_group_cfg': {
            'dimensions': {
                d.name: d.values for d in response.bot_group_cfg.dimensions
            },
        },
    }
    print "Completed handshake: %s" % resp
    return resp

  def poll(self, attributes):
    del attributes # Not used yet
    print "Polling!"
    request = swarming_bot_pb2.PollRequest()
    request.test = True
    response = self._stub.Poll(request)
    manifest = {
      'task_id': '42',
      'dimensions': {},
      'isolated': False,
      'extra_args': '',
      'cipd_input': '',
      'env': {},
      'grace_period': None,
      'hard_timeout': None,
      'io_timeout': None,

      # required in task_runner, why does the server need to send?
      'bot_id': 'fake_bot',

      #Splitting should be done server-side:
      'command': response.manifest.split(),
    }
    print "Will execute manifest: %s" % manifest
    return ('run', manifest)

  def get_bot_code(self, new_zip_fn, bot_version, bot_id):
    raise NotImplementedError("what are you doing?")

  def ping(self):
    pass


def create_state_proto(state_dict, message):
  """ Constructs a State message out of a state dict.

  Inspired by https://github.com/davyzhang/dict-to-protobuf, but all sub-dicts
  need to be encoded as google.protobuf.Structs because only Structs can handle
  free-form key-value pairs (and the mount points, for example, are not known
  at compile time).

  Why not use Struct for the *entire* message? It's because json_format.Parse
  expects the json to have a very specific format (all lists must be wrapped in
  a field called "values") that is too hard to enforce here. So we only use
  Structs where they're needed, and rely on the format of the State proto being
  correct for everything else.
  """
  for k, v in state_dict.iteritems():
    if isinstance(v, dict):
      sub_msg = getattr(message, k)
      json_val = json.dumps(v)
      google.protobuf.json_format.Parse(json_val, sub_msg)
    elif isinstance(v, list):
      l = getattr(message, k)
      l.extend(v)
    elif v != None:
      # setattr doesn't like setting "None" state_dict. Other falsy values are
      # ok. Also, setting something to its default value apparently has no
      # effect, so be ready to deal with it on the receiving side.
      #
      # Warning: setattr will throw if attr doesn't exist.
      # TODO(aludwin): catch in sane way
      setattr(message, k, v)

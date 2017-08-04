# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

# This is a reimplementation of RemoteClientNative but it uses (will use)
# a gRPC method to communicate with a server instead of REST.

import copy
import json
import logging
import time

import google.protobuf.json_format
from proto_bot import swarming_bot_pb2
from remote_client_errors import InternalError
from remote_client_errors import MintOAuthTokenError
from remote_client_errors import PollError
from utils import net
from utils import grpc_proxy


class RemoteClientGrpc(object):
  """RemoteClientGrpc knows how to make calls via gRPC.

  Any non-scalar value that is returned that references values from the proto
  messages should be deepcopy'd since protos make use of weak references and
  might be garbage-collected before the values are used.
  """

  def __init__(self, server, fake_proxy=None):
    logging.info('Communicating with host %s via gRPC', server)
    if fake_proxy:
      self._proxy = fake_proxy
    else:
      self._proxy = grpc_proxy.Proxy(server, swarming_bot_pb2.BotServiceStub)
    self._server = server
    self._log_is_asleep = False

  def is_grpc(self):
    return True

  def initialize(self, quit_bit=None):
    pass

  @property
  def uses_auth(self):
    return False

  def get_authentication_headers(self):
    return {}

  def post_bot_event(self, _event_type, message, _attributes):
    logging.warning('Not yet implemented: posting bot event: %s', message)

  def post_task_update(self, task_id, bot_id, params,
                       stdout_and_chunk=None, exit_code=None):
    request = swarming_bot_pb2.TaskUpdateRequest()

    # Insert all custom/optional data
    if params.has_key('bot_id') or params.has_key('task_id'):
      raise InternalError('params has bot_id or task_id')
    request.id = bot_id
    request.task_id = task_id
    if stdout_and_chunk and stdout_and_chunk[0]:
      # Preserving prior behaviour: empty stdout is not transmitted
      request.output_chunk.data = stdout_and_chunk[0]
      request.output_chunk.offset = stdout_and_chunk[1]
    if exit_code != None:
      request.exit_status.code = exit_code

    # Insert everything else. Note that the b64-encoded strings in the dict
    # are automatically decoded by ParseDict.
    google.protobuf.json_format.ParseDict(params, request)

    # Perform update
    response = self._proxy.call_unary('TaskUpdate', request)
    logging.debug('post_task_update() = %s', request)
    if response.error:
      raise InternalError(response.error)
    return not response.must_stop

  def post_task_error(self, task_id, bot_id, message):
    request = swarming_bot_pb2.TaskErrorRequest()
    request.bot_id = bot_id
    request.task_id = task_id
    request.msg = message
    logging.error('post_task_error() = %s', request)

    response = self._proxy.call_unary('TaskError', request)
    return response.ok

  def _attributes_json_to_proto(self, json_attr, msg):
    msg.version = json_attr['version']
    for k, values in sorted(json_attr['dimensions'].iteritems()):
      pair = msg.dimensions.add()
      pair.name = k
      pair.values.extend(values)
    create_state_proto(json_attr['state'], msg.state)

  def do_handshake(self, attributes):
    request = swarming_bot_pb2.HandshakeRequest()
    self._attributes_json_to_proto(attributes, request.attributes)
    response = self._proxy.call_unary('Handshake', request)
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
    logging.info('Completed handshake: %s', resp)
    return copy.deepcopy(resp)

  def poll(self, attributes):
    request = swarming_bot_pb2.PollRequest()
    self._attributes_json_to_proto(attributes, request.attributes)
    # TODO(aludwin): gRPC-specific exception handling (raise PollError).
    response = self._proxy.call_unary('Poll', request)

    if response.cmd == swarming_bot_pb2.PollResponse.UPDATE:
      return 'update', response.version

    if response.cmd == swarming_bot_pb2.PollResponse.SLEEP:
      if not self._log_is_asleep:
        logging.info('Going to sleep')
        self._log_is_asleep = True
      return 'sleep', response.sleep_time

    if response.cmd == swarming_bot_pb2.PollResponse.TERMINATE:
      logging.info('Terminating!')
      return 'terminate', response.terminate_taskid

    if response.cmd == swarming_bot_pb2.PollResponse.RESTART:
      logging.info('Rebooting the host: %s', response.restart_message)
      return 'host_reboot', response.restart_message

    if response.cmd == swarming_bot_pb2.PollResponse.RUN:
      protoManifest = response.manifest
      # IsolateServerGrpc in client/isolate_storage.py only supports
      # the default namespace.
      assert protoManifest.isolated.namespace == 'default-gzip'
      manifest = {
        'bot_id': protoManifest.bot_id,
        'dimensions' : {
            key: val for key, val in protoManifest.dimensions.items()
        },
        'env': {
            key: val for key, val in protoManifest.env.items()
        },
        'grace_period': protoManifest.grace_period,
        'hard_timeout': protoManifest.hard_timeout,
        'io_timeout': protoManifest.io_timeout,
        'isolated': {
            'namespace': protoManifest.isolated.namespace,
            'input' : protoManifest.isolated.input,
            'server': self._server,
        },
        'outputs': [ o for o in protoManifest.outputs ],
        'task_id': protoManifest.task_id,

        # These keys are only needed by raw commands. While this method
        # only supports isolated commands, the keys need to exist to avoid
        # missing key errors.
        'command': None,
        'extra_args': None,
      }
      logging.info('Received job manifest: %s', manifest)
      self._log_is_asleep = False
      return 'run', copy.deepcopy(manifest)

    raise PollError('Unknown command in response: %s' % response)

  def get_bot_code(self, new_zip_fn, bot_version, _bot_id):
    # TODO(aludwin): exception handling, pass bot_id
    logging.info('Updating to version: %s', bot_version)
    request = swarming_bot_pb2.BotUpdateRequest()
    request.bot_version = bot_version
    response = self._proxy.call_unary('BotUpdate', request)
    with open(new_zip_fn, 'wb') as f:
      f.write(response.bot_code)

  def ping(self):
    pass

  def mint_oauth_token(self, task_id, bot_id, account_id, scopes):
    # pylint: disable=unused-argument
    raise MintOAuthTokenError(
        'mint_oauth_token is not supported in grpc protocol')


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
      insert_dict_as_submessage(message, k, v)
    elif isinstance(v, list):
      l = getattr(message, k)
      l.extend(v)
    elif v != None:
      # setattr doesn't like setting "None" values. Other falsy values are
      # ok. Also, setting something to its default value apparently has no
      # effect, so be ready to deal with it on the receiving side.
      #
      # Warning: setattr will throw if attr doesn't exist.
      # TODO(aludwin): catch in sane way
      setattr(message, k, v)


def insert_dict_as_submessage(message, keyname, value):
  """Encodes a dict as a Protobuf message.

  The keyname for the message field is passed in to simplify the creation
  of the submessage in the first place - you need to say getattr and not
  simply refer to message.keyname since the former actually creates the
  submessage while the latter does not.
  """
  sub_msg = getattr(message, keyname)
  google.protobuf.json_format.Parse(json.dumps(value), sub_msg)



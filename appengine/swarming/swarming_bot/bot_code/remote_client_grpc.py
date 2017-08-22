# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

# This is a reimplementation of RemoteClientNative but it uses (will use)
# a gRPC method to communicate with a server instead of REST.

import copy
import json
import logging
import time

from google.protobuf import json_format
from proto_bot import worker_manager_pb2
from proto_bot import reservation_manager_pb2
from proto_bot import task_manager_pb2
from remote_client_errors import InternalError
from remote_client_errors import MintOAuthTokenError
from remote_client_errors import PollError
from utils import net
from utils import grpc_proxy

try:
  from proto_bot import worker_manager_pb2_grpc
  from proto_bot import reservation_manager_pb2_grpc
  from proto_bot import task_manager_pb2_grpc
except ImportError:
  # This happens legitimately during unit tests
  worker_manager_pb2_grpc = None
  reservation_manager_pb2_grpc = None
  task_manager_pb2_grpc = None


class RemoteClientGrpc(object):
  """RemoteClientGrpc knows how to make calls via gRPC.

  Any non-scalar value that is returned that references values from the proto
  messages should be deepcopy'd since protos make use of weak references and
  might be garbage-collected before the values are used.
  """

  def __init__(self, server, fake_proxy=None):
    logging.info('Communicating with host %s via gRPC', server)
    if fake_proxy:
      self._proxy_wm = fake_proxy
      self._proxy_rm = fake_proxy
      self._proxy_tm = fake_proxy
    else:
      self._proxy_wm = grpc_proxy.Proxy(server,
          worker_manager_pb2_grpc.WorkerManagerStub)
      self._proxy_rm = grpc_proxy.Proxy(server,
          reservation_manager_pb2_grpc.ReservationManagerStub)
      self._proxy_tm = grpc_proxy.Proxy(server,
          task_manager_pb2_grpc.TaskManagerStub)
    self._server = server
    self._log_is_asleep = False

  @property
  def server(self):
    return self._server

  @property
  def is_grpc(self):
    return True

  def initialize(self, quit_bit=None):
    pass

  @property
  def uses_auth(self):
    return False

  def get_authentication_headers(self):
    return {}

  def ping(self):
    pass

  def mint_oauth_token(self, task_id, bot_id, account_id, scopes):
    # pylint: disable=unused-argument
    raise MintOAuthTokenError(
        'mint_oauth_token is not supported in grpc protocol')

  def post_bot_event(self, _event_type, message, _attributes):
    # pylint: disable=unused-argument
    logging.warning('post_bot_event(%s): not yet implemented', message)

  def post_task_update(self, task_id, bot_id, params,
                       stdout_and_chunk=None, exit_code=None):
    # pylint: disable=unused-argument
    logging.warning('post_task_update(%s): not yet implemented', params)

  def post_task_error(self, task_id, bot_id, message):
    # pylint: disable=unused-argument
    logging.warning('post_task_error(%s): not yet implemented', message)

  def do_handshake(self, attributes):
    logging.info('do_handshake(%s)', attributes)
    request = worker_manager_pb2.CreateWorkerRequest()
    request.parent = self._proxy_wm.prefix
    self._attributes_to_worker(attributes, request.worker)
    logging.info('sending request: %s', request)
    worker = self._proxy_wm.call_unary('CreateWorker', request)
    props = json_format.MessageToDict(worker.properties, False, True)
    resp = {
        'server_version': 'unknown', # TODO(aludwin): fill in or ignore
        'bot_version': props['bot_version'],
        'bot_group_cfg_version': 'unknown', #TODO(aludwin): fill in or ignore
        'bot_group_cfg': {
            'dimensions': _device_to_new_dimensions(attributes, worker.device),
        },
    }

    logging.info('Completed handshake: %s', resp)
    return copy.deepcopy(resp)

  def poll(self, attributes):
    # pylint: disable=unused-argument
    logging.warning('Not yet implemented: poll')

  def get_bot_code(self, new_zip_fn, bot_version, _bot_id):
    # pylint: disable=unused-argument
    logging.warning('Not yet implemented: get_bot_code')

  def _attributes_to_worker(self, attributes, worker):
    """Creates a proto Worker message from  bot attributes."""
    worker.name = (self._proxy_wm.prefix + '/workers/' +
                   attributes['dimensions']['id'][0])
    _dimensions_to_device(attributes['dimensions'], worker.device)
    attributes['state']['bot_version'] = attributes['version']
    json_format.ParseDict(attributes['state'], worker.properties)


def _dimensions_to_device(dims, device):
  """Converts botattribute dims to a Worker device."""
  properties = device.properties
  for k, values in sorted(dims.iteritems()):
    for v in sorted(values):
      prop = properties.add()
      prop.key.custom = k
      prop.value = v


def _device_to_new_dimensions(attributes, device):
  """Detects what new dimensions have been returned."""
  # Make a record of what old values existed
  old_dims = {}
  for k, values in attributes['dimensions'].iteritems():
    old_dims[k] = {}
    for v in values:
      old_dims[k][v] = True
  # Record all missing kvps
  dims = {}
  for prop in device.properties:
    k = prop.key.custom
    v = prop.value
    if not old_dims.get(k) or not old_dims[k].get(v):
      dims[k] = dims.get(k, [])
      dims[k].append(prop.value)
  return dims

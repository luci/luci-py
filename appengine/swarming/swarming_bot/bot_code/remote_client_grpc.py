# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

# This is a reimplementation of RemoteClientNative but it uses gRPC to
# communicate with a server instead of REST.
#
# TODO(aludwin): remove extremely verbose logging throughout when this is
# actually working.

import copy
import json
import logging
import time

from google.protobuf import json_format
from proto_bot import command_pb2
from proto_bot import reservation_manager_pb2
from proto_bot import task_manager_pb2
from proto_bot import worker_manager_pb2
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
    logging.warning('post_task_update(%s, %s, %s): not yet implemented',
                    task_id, bot_id, params)

  def post_task_error(self, task_id, bot_id, message):
    # pylint: disable=unused-argument
    logging.warning('post_task_error(%s): not yet implemented', message)

  def do_handshake(self, attributes):
    logging.info('do_handshake(%s)', attributes)
    request = worker_manager_pb2.CreateWorkerRequest()
    request.parent = self._proxy_wm.prefix
    self._attributes_to_worker(attributes, request.worker)
    worker = self._proxy_wm.call_unary('CreateWorker', request)
    props = json_format.MessageToDict(worker.properties, False, True)
    resp = {
        'bot_version': props['bot_version'],
        'bot_group_cfg': {
            'dimensions': _device_to_new_dimensions(attributes, worker.device),
        },
        # Don't fill in server_version or bot_group_cfg_version, even with dummy
        # values, as the server will expect them to be correct and will tell the
        # bot to restart itself.
        # TODO(aludwin): fill these values in once we can retrieve them in a
        # reasonable way.
    }

    logging.info('Completed handshake: %s', resp)
    return copy.deepcopy(resp)

  def poll(self, attributes):
    logging.info('poll(%s)', attributes)
    # In the experimental worker API, this is two operations: updating the
    # worker and getting a lease. First, get a description of the current
    # worker.
    worker = worker_manager_pb2.Worker()
    self._attributes_to_worker(attributes, worker)

    # Next, send the update.
    worker = self._update_worker(worker)
    # TODO(aludwin): if there are any admin actions that need doing, do them
    # now.

    # Actually get a lease
    lease = self._poll_for_lease(worker)
    if not lease:
      # Nothing to do at the moment. Sleep briefly and then call again because
      # the server supports long-polls. If the server is overwhelmed, it should
      # reply with UNAVAILABLE.
      # TODO(aludwin): handle UNAVAILABLE.
      if not self._log_is_asleep:
        logging.info('Going to sleep')
        self._log_is_asleep = True
      return 'sleep', 1

    self._log_is_asleep = False
    if lease.client:
      raise ValueError('managers on different endpoints are not supported')
    if not lease.exclusive:
      raise ValueError('received nonexclusive lease')
    if not lease.task:
      raise ValueError('task must be included in lease')
    return ('run', self._lease_to_manifest(worker, lease))

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

  def _bot_id_from_worker(self, worker):
    prefix = self._proxy_wm.prefix + '/workers/'
    if not worker.name.startswith(prefix):
      raise ValueError('worker %s does not start with expected prefix %s' %
                       (worker.name, prefix))
    return worker.name[len(prefix):]

  def _update_worker(self, worker):
    req = worker_manager_pb2.UpdateWorkerRequest()
    req.worker.CopyFrom(worker)
    # TODO(aludwin): use the worker returned by UpdateWorker, but right now
    # UpdateWorker isn't returning a valid worker, so just go with what we
    # passed in for now.
    self._proxy_wm.call_unary('UpdateWorker', req)
    return worker

  def _poll_for_lease(self, worker):
    """Gets a lease from the server.

    In this implementation, the device in the poll request is identical to that
    of the worker as a whole because we do not support partial leases of a
    worker.
    """
    req = reservation_manager_pb2.PollRequest()
    req.parent = self._proxy_rm.prefix
    req.worker_name = worker.name
    req.device.CopyFrom(worker.device)
    req.accept_exclusive_leases = True
    res = None
    try:
      # TODO(aludwin): pass in quit_bit? Pressing ctrl-c doesn't actually
      # interrupt either the long poll or the retry process, so we need to wait
      # a while to exit cleanly.
      res = self._proxy_rm.call_unary('Poll', req)
    except grpc_proxy.grpc.RpcError as g:
      # It's fine for the deadline to expire; this simply means that no tasks
      # were received while long-polling. Any other exception cannot be
      # recovered here.
      if g.code() is not grpc_proxy.grpc.StatusCode.DEADLINE_EXCEEDED:
        raise
    logging.info('received poll response: %s', res)
    # res may be None if we got DEADLINE_EXCEEDED; res.lease appears to always
    # be present but if it has its zero value, the name will be the empty
    # string.
    if not res or not res.lease.name:
      return None
    return res.lease

  def _lease_to_manifest(self, worker, lease):
    """Convert the lease into a manifest.

    The only supported leases are the ones that include a task.
    """
    command = command_pb2.CommandTask()
    lease.task.Unpack(command)
    manifest = {
      'bot_id': self._bot_id_from_worker(worker),
      'dimensions' : {
        prop.key.custom: prop.value for prop in lease.requirements.properties
      },
      'env': {
        env.name: env.value for env in command.inputs.environment_variables
      },
      # proto duration uses long integers; clients expect ints
      'grace_period': int(command.timeouts.shutdown.seconds),
      'hard_timeout': int(command.timeouts.execution.seconds),
      'io_timeout': int(command.timeouts.io.seconds),
      'isolated': {
        'namespace': 'default-gzip',
        'input' : command.inputs.files[0].hash,
        'server': self._server,
      },
      'outputs': [o for o in command.outputs.files],
      'task_id': lease.name,
      # These keys are only needed by raw commands. While this method
      # only supports isolated commands, the keys need to exist to avoid
      # missing key errors.
      'command': None,
      'extra_args': None,
    }
    logging.info('returning manifest: %s', manifest)
    return manifest


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

# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

# This is a reimplementation of RemoteClientNative but it uses (will use)
# a gRPC method to communicate with a server instead of REST.
import grpc
import workers_pb2

class RemoteClientGrpc(object):
  """RemoteClientGrpc knows how to make calls via gRPC.
  """

  def __init__(self, server):
    del server # Not used yet
    self._channel = grpc.insecure_channel("localhost:90")
    self._stub = workers_pb2.WorkerServiceStub(self._channel)

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
    del attributes # Not used yet
    print "Handshaking"
    resp = {
        'bot_version': 'test',
    }
    return resp

  def poll(self, attributes):
    del attributes # Not used yet
    print "Polling!"
    platform = workers_pb2.WorkerPlatform()
    action = self._stub.RequestTask(platform)
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
      'command': action.command.split(),
    }
    print "Will execute manifest: %s" % manifest
    return ('run', manifest)

  def get_bot_code(self, new_zip_fn, bot_version, bot_id):
    raise NotImplementedError("what are you doing?")

  def ping(self):
    pass

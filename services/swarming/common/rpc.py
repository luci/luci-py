# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import logging


class RPCError(Exception):
  """Failed to decode an RPC packet."""
  pass


def BuildRPC(func_name, args):
  """Builds a dictionary of an operation that needs to be executed.

  Args:
    func_name: a string of the function name to execute on the remote host.
    args: arguments to be passed to the function.

  Returns:
    A dictionary containing them function name and args.
  """
  return {'function': func_name, 'args': args}


def ParseRPC(rpc):
  """Parses RPC created by BuildRPC into a tuple.

  Args:
    rpc: dictionary containing function name and args.

  Returns:
    A tuple of (str, args) of function name and args.

  Raises:
    SlaveError: with human readable string.
  """
  if not isinstance(rpc, dict):
    raise RPCError('Invalid RPC container')
  expected = ['args', 'function']
  if expected != sorted(rpc):
    raise RPCError('Invalid RPC: expected %s, got %s' % (expected, sorted(rpc)))

  function = rpc['function']
  if not function or not isinstance(function, basestring):
    raise RPCError('Invalid RPC call function name')

  args = rpc['args']
  logging.debug('RPC %s(%s)', function, args)
  return (function, args)

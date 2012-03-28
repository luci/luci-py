#!/usr/bin/python2.4
#
# Copyright 2011 Google Inc. All Rights Reserved.

"""A base class for machine providers to be used by the machine manager."""





class MachineProviderException(Exception):
  """An Exception class dedicated to this MachineProvider.

  Attributes:
    message: A user-readable descrition of the error.
    error_code: A numeric application error returned by the Hive server.
  """

  def __init__(self, message, error_code=None):
    Exception.__init__(self)
    self.message = message
    self.error_code = error_code

  def __str__(self):
    return self.message


class MachineStatus(object):
  """An enumeration class to identify a machine status."""
  # The requested machine is not ready yet, it's being prepared.
  WAITING = 0
  # The requested machine is ready and can be used.
  READY = 1
  # The machine has been stopped for some reason and will remain stopped until
  # the machince provider says it can be used again.
  STOPPED = 2
  # The machine is done and can't be used anymore.
  DONE = 3


class MachineInfo(object):
  """An object providing information about a provided machine.

  Attributes:
    status: A value from MachineStatus, defaults to WAITING.
    host: The host name of the machine, e.g., it's IP address.
  """

  def __init__(self, status=MachineStatus.WAITING, host=None):
    self._status = status
    self._host = host

  def Status(self):
    return self._status

  def Host(self):
    return self._host


class BaseMachineProvider(object):
  """Base class/API to give access to specialized Machine Providers."""

  def RequestMachine(self, pool, config_dimensions, life_span):
    """Reserves a specific machine for the caller and return its ID.

    Args:
      pool: UNUSED.
      config_dimensions: A configuration dimensions dictionary.
          See http://code.google.com/p/swarming/wiki/ConfigurationDimensions
          The key of the dict is a dimension name, like 'os' or 'browser' or
          'cpu'. The value of the map should be taken from the web page above.
      life_span: A datetime.timedelta value identifying the length of the
          acquisition so that the machine provider can claim back the machine
          once this time elapsed.

    Returns:
      A unique identifier for the machine being requested.

    Raises:
      MachineProviderException when no machine can be acquired.
    """
    pass

  def GetMachineInfo(self, machine_id):
    """Returns information about a specific machine.

    Args:
      machine_id: The unique identifier of the machine to get info for.

    Returns:
      A MachineInfo object exposing a status() and a host() method.

    Raises:
      MachineProviderException when machine info can not be returned.
    """
    pass

  def ReleaseMachine(self, machine_id):
    """Releases the specified machine and makes it available again.

    Args:
      machine_id: The unique identifier of the machine to be released.

    Raises:
      MachineProviderException when machine can not be released.
    """
    pass

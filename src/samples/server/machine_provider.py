#!/usr/bin/python2.4
# Copyright 2011 Google Inc. All Rights Reserved.

"""This implements a machine provider using localhost:8001/api."""


try:  # pylint: disable-msg=C6205
  import simplejson as json  # pylint: disable-msg=C6204
except ImportError:
  import json  # pylint: disable-msg=C6204
import logging
import urllib
import urllib2

from server import base_machine_provider


class MachineProvider(base_machine_provider.BaseMachineProvider):
  """A Machine Provider delegating to a localhost server API."""

  def __init__(self, url='http://localhost:8001/api'):
    """Constructor for MachineProvider.

    Args:
      url: The url pointing to a machine provider server API.
    """
    self._url = url

  def RequestMachine(self, unused_pool, config_dimensions, unused_life_span):
    """Reserves a specific machine for the caller and returns its ID.

    Args:
      unused_pool: UNUSED.
      config_dimensions: A configuration dimensions dictionary.
          These must fit with the dimensions supported by the localhost server.
      unused_life_span: A datetime.timedelta value identifying the length of the
          acquisition so that the machine provider can claim back the machine
          once this time elapsed.

    Returns:
      A unique integer identifier for the machine being requested.

    Raises:
      base_machine_provider.MachineProviderException when no machine can be
      acquired.
    """
    # TODO(user): Catch urllib2 and json errors.
    # TODO(user): Implement life span.
    u = urllib2.urlopen(self._url, data=urllib.urlencode(
        {'command': 'request', 'dimensions': json.dumps(config_dimensions)}))
    result = json.loads(u.read())
    if result['available'] == 0:
      raise base_machine_provider.MachineProviderException(
          message='No machines available', error_code=-1)
    return result['id']

  def GetMachineInfo(self, machine_id):
    """Returns information about a specific machine.

    Args:
      machine_id: The unique integer identifier of the machine to get info for.

    Returns:
      A MachineInfo object exposing a status() and a host() method.

    Raises:
      base_machine_provider.MachineProviderException when machine info can not
      be returned.
    """
    # TODO(user): Catch urllib2 and json errors.
    u = urllib2.urlopen(self._url, data=urllib.urlencode(
        {'command': 'info', 'id': json.dumps(machine_id)}))
    response = json.loads(u.read())
    if 'error' in response:
      raise base_machine_provider.MachineProviderException(
          message=response['error'], error_code=-1)
    if response['status'] == 'ERROR':
      raise base_machine_provider.MachineProviderException(
          message='Machine is in an error state', error_code=-1)
    elif response['status'] == 'READY':
      status = base_machine_provider.MachineStatus.READY
    elif response['status'] == 'WAITING':
      status = base_machine_provider.MachineStatus.WAITING
    elif response['status'] == 'STOPPED':
      status = base_machine_provider.MachineStatus.STOPPED
    elif response['status'] == 'DONE':
      status = base_machine_provider.MachineStatus.DONE
    else:
      raise base_machine_provider.MachineProviderException(
          message='Machine is in an unknown state', error_code=-1)
    logging.info('returning machine with status: %s', status)
    machine_info = base_machine_provider.MachineInfo(
        status=status, host=response['host'])
    return machine_info

  def ReleaseMachine(self, machine_id):
    """Releases the specified machine and makes it available again.

    Args:
      machine_id: The unique identifier of the machine to be released.

    Raises:
      base_machine_provider.MachineProviderException when machine can not be
          released.
    """
    # TODO(user): Catch urllib and json errors.
    u = urllib2.urlopen(self._url, data=urllib.urlencode(
        {'command': 'release', 'id': json.dumps(machine_id)}))
    response_str = u.read()
    logging.debug(response_str)
    response = json.loads(response_str)
    if 'error' in response:
      raise base_machine_provider.MachineProviderException(
          message=response['error'], error_code=-1)

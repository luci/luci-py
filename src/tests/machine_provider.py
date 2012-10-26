#!/usr/bin/python2.7
#
# Copyright 2011 Google Inc. All Rights Reserved.

"""This implements a mock machine provider for testing."""





from server import base_machine_provider


class MachineProvider(base_machine_provider.BaseMachineProvider):
  """A Machine Provider for testing."""

  def __init__(self):
    """Constructor for MachineProvider.

    Args:
    """
    pass

  def RequestMachine(self, unused_pool, unused_config_dimensions,
                     unused_life_span):
    """Reserves a specific machine for the caller and return it's ID.

    Args:
      unused_pool: UNUSED.
      unused_config_dimensions: A configuration dimensions dictionary.
          See http://code.google.com/p/swarming/wiki/ConfigurationDimensions
          The key of the dict is a dimension name, like 'os' or 'browser' or
          'cpu'. The value of the map should be taken from the web page above.
      unused_life_span: A datetime.timedelta value identifying the length of the
          acquisition so that the machine provider can claim back the machine
          once this time elapsed.

    Returns:
      A unique identifier for the machine being request.

    Raises:
      base_machine_provider.MachineProviderException when no machine can be
      acquired.
    """
    raise base_machine_provider.MachineProviderException(
        'deprecated functionality')

  def GetMachineInfo(self, machine_id):  # pylint: disable-msg=W0613
    """Returns information about a specific machine.

    Args:
      machine_id: The unique identifier of the machine to get info for.

    Returns:
      A MachineInfo object exposing a status() and a host() method.

    Raises:
      base_machine_provider.MachineProviderException when machine info can not
      be returned.
    """
    raise base_machine_provider.MachineProviderException(
        'deprecated functionality')

  def ReleaseMachine(self, machine_id):  # pylint: disable-msg=W0613
    """Releases the specified machine and make it available again.

    Args:
      machine_id: The unique identifier of the machine to be released.

    Raises:
      MachineProviderException when machine can not be released.
    """
    raise base_machine_provider.MachineProviderException(
        'deprecated functionality')

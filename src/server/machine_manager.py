#!/usr/bin/python2.4
#
# Copyright 2011 Google Inc. All Rights Reserved.

"""Manages a set of remote machines.

The file contains classes to help manage a set of remote machines used for
running tests by the Test Request Server.  Machines with specific dimensions
can be acquired, their status checked, and released when no longer needed.
"""



import datetime
import logging


from google.appengine.ext import db
from server import base_machine_provider


class Machine(db.Expando):
  """Represents the acquisition of one machine.

  This class is used by the MachineManager class (declared below) to keep track
  of machine acquisition requests made by users.  It derives from db.Expando so
  that dynamically added dimensions can be saved to stable storage using the
  appengine store API without having to hard code an explicit set of dimensions
  which may change in the future.
  """
  # The machine provider id of this machine.
  id = db.IntegerProperty()

  # The machine provider status of this machine.  Can be one of the the
  # base_machine_provider.MachineStatus enum values.
  status = db.IntegerProperty()

  # The hostname or IP address of the machine.  This property is not valid
  # until the state is ready.
  host = db.StringProperty()

  _RESERVERED_NAMES = ['id', 'status', 'host']

  def SetMachineInfo(self, machine_info):
    if machine_info:
      self.host = machine_info.Host()
      self.status = machine_info.Status()

  def SetDimensions(self, config_dimensions):
    """Sets the dimensions from a given config dimensions dictionary.

    Args:
      config_dimensions: A configuration dimensions dictionary.

    Returns:
      True if all went well, and False if an invalid name was used.
    """
    for (dimension_name, config_dimension_value) in config_dimensions.items():
      if (not isinstance(dimension_name, str) or
          dimension_name in self._RESERVERED_NAMES):
        logging.error('Illegal name for a dimension: %s.', dimension_name)
        return False
      if (not isinstance(config_dimension_value, str) and
          (not isinstance(config_dimension_value, (list, tuple)) or
           sum([not isinstance(i, str) for i in config_dimension_value]))):
        logging.error('Illegal name for a dimension: %s.', dimension_name)
        return False
      # Dimensions are documented to be lists only, yet we accept str.
      if not isinstance(config_dimension_value, (list, tuple)):
        config_dimension_value = [config_dimension_value]
      # db.Expando doesn't like empty arrays.
      if config_dimension_value:
        setattr(self, dimension_name, config_dimension_value)
    return True

  def GetDimensions(self):
    """Returns a dictionary version of our data.

    Returns:
      A dictionary version of our data.
    """
    return dict([(n, getattr(self, n)) for n in self.dynamic_properties()])


class MachineManager(object):
  """Manages a set of remote machines.

  The Machine Manager caches the state of all acquired machines and keeps
  an eye on their state.  Registered listeners can be notified of machine
  state changes.

  The Machine Manager is meant to be run in the appengine environment, and as
  such uses the appengine stable storage API for its own internal persistence.
  See data.py in the same directory for a description of this persistence.
  """

  # TODO(user): Fine tune this value and implement proper machine
  # releasing when not in used long enough or about to expire.
  _MACHINE_REQUEST_EXPIRATION_DELTA = datetime.timedelta(days=3)

  def __init__(self, machine_provider):
    """Constructor for MachineManager.

    Args:
      machine_provider: The object to use for providing machines. The consumer
          of the machine manager can choose to use its own provider or
          the one in Chrome Golo, or even a mock one for testing.
    """
    logging.debug('MM starting')

    assert machine_provider is not None
    self._machine_provider = machine_provider
    self._listeners = set()

    self.ValidateMachines()
    logging.debug('MM created')

  def ValidateMachines(self):
    """Validate all known machines.

    Validate the status of all machines that have been acquired so far.  If the
    status has changed since the last time we check, inform any registered
    listeners of the change.

    This function is called at the startup of the machine manager to make sure
    the persisted state is still accurate.  It's also called periodically since
    some machine providers don't provide a notification mechanism.  Its
    expected that an appengine cron job (or something similar) has been setup
    for the periodic calls.
    """
    logging.debug('MM: Start validation')

    query = Machine.all()
    for machine in query:
      # Validate the machine, and send out a notification if needed.
      if self._ValidateMachine(machine):
        self._SendNotification(machine)

  def _ValidateMachine(self, machine):
    """Validate the Machine object against provider's current state.

    Args:
      machine: an instance of Machine to validate..

    Returns:
      True if the machine was updated, false otherwise.
    """
    new_status = machine.status

    # Get the current status of the machine.  If it has changed, tell any
    # registered users.

    try:
      # If the machine status is AVAILABLE, then this means we tried to release
      # this machine already, but failed.  So try again now.
      if machine.status == base_machine_provider.MachineStatus.AVAILABLE:
        self._machine_provider.ReleaseMachine(machine.id)
        machine.delete()
      else:
        machine_info = self._machine_provider.GetMachineInfo(machine.id)
        new_status = machine_info.Status()
    except base_machine_provider.MachineProviderException, e:
      # We differentiate between transient and non-transient errors.  Because
      # we periodically poll the machine provider, we can simply ignore
      # transient errors here and we will check again later.  For non-transient
      # errors we want to forget about this machine.
      if not self._IsTransientError(e):
        logging.exception('MachineProviderException: %s', e)
        if machine.status == base_machine_provider.MachineStatus.AVAILABLE:
          machine.delete()
          return False

        new_status = base_machine_provider.MachineStatus.AVAILABLE
      else:
        logging.exception('MM: Provider error while validating id=%d: %s (%d)',
                          machine.id, e.message, e.error_code)
        return False

    # If the status is ACQUIRED, transfer the configuration properties to
    # the machine object.  Note that once the machine becomes ready these
    # properties don't change, so we only need to transfer configuration for
    # status ACQUIRED..
    if new_status == base_machine_provider.MachineStatus.ACQUIRED:
      self._InitMachine(machine, machine_info)

    if new_status != machine.status:
      machine.status = new_status
      machine.put()
      logging.debug('MM: machine id=%d info saved', machine.id)
      return True

    return False

  def _IsTransientError(self, e):
    """Is the exception a transient error?

    Some errors that will be thrown from the Machine Provider API represent
    transient errors, like network failures.  Others represent real failures,
    like specifying an invalid request Id.  This function returns True for
    transient errors.  The machine manager will retry operations when receiving
    transient errors, and not otherwise.

    Args:
      e: A Machine Provider exception to be examined for transiency.

    Returns:
      True if we consider the error transient, False otherwise.
    """
    # TODO(user): may need to tweak this for specific error cases.
    return e.error_code > 0

  def _InitMachine(self, machine, machine_info):
    """Transfers information from the provider info to the machine object.

    Args:
      machine: a Machine object to save the information into.
      machine_info: a machine provider info from the GetMachineInfo function.
    """
    machine.host = machine_info.Host()
    logging.debug('MM: machine id=%d using ip=%s', machine.id, machine.host)

  def _SendNotification(self, machine):
    """Send a status change notification for the given machine.

    Calls each listener with the specified machine.

    Args:
      machine: an instance of Machine representing the machine whose
          status has changed.
    """
    for listener in self._listeners:
      listener.MachineStatusChanged(machine)

  def RegisterStatusChangeListener(self, listener):
    """Add a machine status change listener.

    Adding a listener more than once has no effect, it will be called only
    once during notifications.  The listener is an object that implements the
    following function:

        def MachineStatusChanged(self, machine)

    where machine contains the new status of the machine that changed.  The
    listener should not modify the received object.

    Args:
      listener: a function that takes one Machine argument.
    """
    # TODO(user): do we have to worry about enum during this operation?
    self._listeners.add(listener)

  def UnregisterStatusChangeListener(self, listener):
    """Remove a machine status change listener.

    This will remove a listener regardless of how many times that listeners
    was registered.

    Args:
      listener: a function that takes one Machine argument.
    """
    # TODO(user): do we have to worry about enum during this operation?
    self._listeners.remove(listener)

  def RequestMachine(self, pool, config_dimensions):
    """Requests a new machine.

    Requests a new machine from the specified pool, and returns the provider's
    machine id for this request if a machine is available.  Machine acquisition
    may be sync or async (as decided by the machince provider), so the machine
    might not be ready immediately.  The status of the machine can be inquired
    using the GetMachineStatus() function or by registering a callback function
    for machine status changes.

    Args:
      pool: the pool from which to allocate the machine.  Use None for
          the default pool.
      config_dimensions: a configuration dimensions dictionary.
          See http://code.google.com/p/swarming/wiki/ConfigurationDimensions
          for possible values.  The key of the dict is a dimension name,
          like 'os' or 'browser' or 'cpu'.
          The value of the map should be taken from the web page above.

    Returns:
      An id that represents this machine, or -1 if a machine was not available.
    """
    try:
      machine_id = self._machine_provider.RequestMachine(
          pool, config_dimensions, self._MACHINE_REQUEST_EXPIRATION_DELTA)
    except base_machine_provider.MachineProviderException, e:
      logging.warning('Can\'t open request, exception: %s (%d)',
                      e.message, e.error_code)
      return -1

    # Attempting to add a constructor to Machine that takes in MachineInfo
    # results in an odd crash when calling put on Machine afterwards, not
    # excatly sure why.
    machine = Machine()
    machine.SetMachineInfo(self._machine_provider.GetMachineInfo(machine_id))
    if not machine.SetDimensions(config_dimensions):
      logging.error('Invalid configuration: %s', str(config_dimensions))
      return -1

    machine.id = machine_id
    machine.put()

    logging.debug('MM instance created id=%d dim=%s',
                  machine_id, str(config_dimensions))
    self._SendNotification(machine)
    return machine_id

  def GetMachineInfo(self, machine_id):
    """Gets information about a machine.

    Gets information about a machine given its id as returned by
    AcquireMachine().

    Args:
      machine_id: a machine id returned by AcquireMachine().

    Returns:
      A Machine object.  Callers should not modify the returned object.  If no
      machine with the given id is found, None is returned.
    """
    # -1 is not a valid ID.
    assert machine_id is not -1
    machine = Machine.gql('WHERE id = :1 AND status != :2', machine_id,
                          base_machine_provider.MachineStatus.AVAILABLE).get()
    if machine:
      logging.debug('MM: GetMachineInfo status=%d dim=%s', machine.status,
                    machine.GetDimensions())
    return machine

  def ReleaseMachine(self, machine_id):
    """Release a machine that is no longer needed.

    Releases a machine and puts it back into the pool.  Any data and
    programs running on that machine will be lost.  Releasing a machine is
    an async operation; getting the machine's state before its finally released
    will return base_machine_provider.MachineStatus.STOPPED.

    Args:
      machine_id: a machine id returned by AcquireMachine().

    Returns:
      True if we successfully released the machine.
      False otherwise.
    """
    # -1 is not a valid ID.
    assert machine_id is not -1

    success = False
    machine = Machine.gql('WHERE id = :1 AND status != :2', machine_id,
                          base_machine_provider.MachineStatus.AVAILABLE).get()
    if machine:
      logging.info('MM releasing machine id=%d', machine_id)

      # Mark the machine as being released.  We will persist the machine
      # in this state just in case the CloseRequest() below fails.  If it
      # does, the next time we validate will will try again.
      machine.status = base_machine_provider.MachineStatus.AVAILABLE
      machine.put()
      self._SendNotification(machine)

      # If this fails, the machine will still be marked as STOPPED so that we
      # don't try to use it again.
      try:
        self._machine_provider.ReleaseMachine(machine_id)

        # We successfully closed this request, so now its safe to delete
        # the machine entity.
        machine.delete()
        success = True
      except base_machine_provider.MachineProviderException, e:
        logging.warning('Failed to release machine id=%d: %s (%d)',
                        machine_id, e.message, e.error_code)

        if not self._IsTransientError(e):
          # If the machine id is not found, then we treat this as a successful
          # machine release.
          machine.delete()
          success = True
    else:
      logging.info('MM releasing machine id=%d does not exist', machine_id)

    return success

  def ListAcquiredMachines(self):
    """List all machine acquired through the machine manager.

    Use this function as follows:

      manager = MachineManager()
      for machine in manager.ListAcquiredMachines():
        # do something with machine

    Returns:
      Returns a google.appengine.ext.db.Query of Machine objects.
    """
    return Machine.gql('WHERE status != :1',
                       base_machine_provider.MachineStatus.AVAILABLE)

  def GetMachineCount(self):
    """Returns the number of machines acquired for testing.

    This method is exposed only for unit tests, production code should not
    need to use this.

    Returns:
      The number of machines acquired for testing.
    """
    return Machine.gql('WHERE status != :1',
                       base_machine_provider.MachineStatus.AVAILABLE).count()

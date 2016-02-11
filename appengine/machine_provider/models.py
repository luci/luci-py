# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Datastore models for the Machine Provider messages."""

import hashlib

from google.appengine.ext import ndb
from google.appengine.ext.ndb import msgprop

from protorpc import messages
from protorpc import protobuf
from protorpc.remote import protojson

from components import auth
from components.machine_provider import rpc_messages

import utils


class Enum(frozenset):
  def __getattr__(self, attr):
    if attr in self:
      return attr
    raise AttributeError(attr)


class LeaseRequest(ndb.Model):
  """Datastore representation of a LeaseRequest.

  Key:
    id: Hash of the client + client-generated request ID which issued the
      original rpc_messages.LeaseRequest instance. Used for easy deduplication.
    kind: LeaseRequest. This root entity does not reference any parents.
  """
  # DateTime indicating original datastore write time.
  created_ts = ndb.DateTimeProperty(auto_now_add=True)
  # Checksum of the rpc_messages.LeaseRequest instance. Used to compare incoming
  # LeaseRequets for deduplication.
  deduplication_checksum = ndb.StringProperty(required=True, indexed=False)
  # ID of the CatalogMachineEntry provided for this lease.
  machine_id = ndb.StringProperty()
  # auth.model.Identity of the issuer of the original request.
  owner = auth.IdentityProperty(required=True)
  # Whether this lease request has been released voluntarily by the owner.
  released = ndb.BooleanProperty()
  # rpc_messages.LeaseRequest instance representing the original request.
  request = msgprop.MessageProperty(rpc_messages.LeaseRequest, required=True)
  # rpc_messages.LeaseResponse instance representing the current response.
  # This field will be updated as the request is being processed.
  response = msgprop.MessageProperty(
      rpc_messages.LeaseResponse, indexed_fields=['state'])

  @classmethod
  def compute_deduplication_checksum(cls, request):
    """Computes the deduplication checksum for the given request.

    Args:
      request: The rpc_messages.LeaseRequest instance to deduplicate.

    Returns:
      The deduplication checksum.
    """
    return hashlib.sha1(protobuf.encode_message(request)).hexdigest()

  @classmethod
  def generate_key(cls, user, request):
    """Generates the key for the given request initiated by the given user.

    Args:
      user: An auth.model.Identity instance representing the requester.
      request: The rpc_messages.LeaseRequest sent by the user.

    Returns:
      An ndb.Key instance.
    """
    # Enforces per-user request ID uniqueness
    return ndb.Key(
        cls,
        hashlib.sha1('%s\0%s' % (user, request.request_id)).hexdigest(),
    )

  @classmethod
  def query_untriaged(cls):
    """Queries for untriaged LeaseRequests.

    Yields:
      Untriaged LeaseRequests in no guaranteed order.
    """
    for request in cls.query(
        cls.response.state == rpc_messages.LeaseRequestState.UNTRIAGED
    ):
      yield request


class CatalogEntry(ndb.Model):
  """Datastore representation of an entry in the catalog."""
  # rpc_messages.Dimensions describing this machine.
  dimensions = msgprop.MessageProperty(
      rpc_messages.Dimensions,
      indexed_fields=[
          field.name for field in rpc_messages.Dimensions.all_fields()
      ],
  )


class CatalogCapacityEntry(CatalogEntry):
  """Datastore representation of machine capacity in the catalog.

  Key:
    id: Hash of the non-None dimensions, where backend is the only required
      dimension. Used to enforce per-backend dimension uniqueness.
    kind: CatalogCapacityEntry. This root entity does not reference any parents.
  """
  # The amount of capacity with these dimensions that can be provided.
  count = ndb.IntegerProperty(indexed=True, required=True)
  # Whether there is available capacity or not.
  has_capacity = ndb.ComputedProperty(lambda self: self.count > 0)

  @classmethod
  def create_and_put(cls, dimensions, count):
    """Creates a new CatalogEntry entity and puts it in the datastore.

    Args:
      dimensions: rpc_messages.Dimensions describing this capacity.
      count: Amount of capacity with the given dimensions.
    """
    cls(
        count=count,
        dimensions=dimensions,
        key=cls.generate_key(dimensions),
    ).put()

  @classmethod
  def generate_key(cls, dimensions):
    """Generates the key for a CatalogEntry with the given dimensions.

    Args:
      dimensions: rpc_messages.Dimensions describing this machine.

    Returns:
      An ndb.Key instance.
    """
    # Enforces per-backend dimension uniqueness.
    assert dimensions.backend is not None
    return ndb.Key(
        cls,
        hashlib.sha1(utils.fingerprint(dimensions)).hexdigest()
    )

  @classmethod
  def query_available(cls, *filters):
    """Queries for available capacity.

    Args:
      *filters: Any additional filters to include in the query.

    Yields:
      CatalogCapacityEntry keys in no guaranteed order.
    """
    for capacity in cls.query(cls.has_capacity == True, *filters).fetch(
        keys_only=True,
    ):
      yield capacity


CatalogMachineEntryStates = Enum(['AVAILABLE', 'LEASED', 'NEW', 'SUBSCRIBING'])


class CatalogMachineEntry(CatalogEntry):
  """Datastore representation of a machine in the catalog.

  Key:
    id: Hash of the backend + hostname dimensions. Used to enforce per-backend
      hostname uniqueness.
    kind: CatalogMachineEntry. This root entity does not reference any parents.
  """
  # ID of the LeaseRequest this machine is provided for.
  lease_id = ndb.StringProperty()
  # DateTime indicating lease expiration time.
  lease_expiration_ts = ndb.DateTimeProperty()
  # rpc_messages.Policies governing this machine.
  policies = msgprop.MessageProperty(rpc_messages.Policies)
  # Cloud Pub/Sub subscription the machine must listen to for instructions.
  pubsub_subscription = ndb.StringProperty(indexed=False)
  # Project the Cloud Pub/Sub subscription exists in.
  pubsub_subscription_project = ndb.StringProperty(indexed=False)
  # Cloud Pub/Sub topic the machine must be subscribed to.
  pubsub_topic = ndb.StringProperty(indexed=False)
  # Project the Cloud Pub/Sub topic exists in.
  pubsub_topic_project = ndb.StringProperty(indexed=False)
  # Element of CatalogMachineEntryStates giving the state of this entry.
  state = ndb.StringProperty(
      choices=CatalogMachineEntryStates,
      default=CatalogMachineEntryStates.AVAILABLE,
      indexed=True,
      required=True,
  )

  @classmethod
  def create_and_put(cls, dimensions, policies, state):
    """Creates a new CatalogEntry entity and puts it in the datastore.

    Args:
      dimensions: rpc_messages.Dimensions describing this machine.
      policies: rpc_messages.Policies governing this machine.
      state: Element of CatalogMachineEntryState describing this machine.
    """
    cls(
        dimensions=dimensions,
        policies=policies,
        state=state,
        key=cls.generate_key(dimensions),
    ).put()

  @classmethod
  def generate_key(cls, dimensions):
    """Generates the key for a CatalogEntry with the given dimensions.

    Args:
      dimensions: rpc_messages.Dimensions describing this machine.

    Returns:
      An ndb.Key instance.
    """
    # Enforces per-backend hostname uniqueness.
    assert dimensions.backend is not None
    assert dimensions.hostname is not None
    return ndb.Key(
        cls,
        hashlib.sha1(
            '%s\0%s' % (dimensions.backend, dimensions.hostname)
        ).hexdigest(),
    )

  @classmethod
  def query_available(cls, *filters):
    """Queries for available machines.

    Args:
      *filters: Any additional filters to include in the query.

    Yields:
      CatalogMachineEntry keys in no guaranteed order.
    """
    available = cls.state == CatalogMachineEntryStates.AVAILABLE
    for machine in cls.query(available, *filters).fetch(keys_only=True):
      yield machine

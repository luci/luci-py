# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Datastore models for the Machine Provider messages."""

import hashlib

from google.appengine.ext import ndb
from google.appengine.ext.ndb import msgprop

from protorpc import messages

from components import auth

import rpc_messages


class Enum(frozenset):
  def __getattr__(self, attr):
    if attr in self:
      return attr
    raise AttributeError(attr)


LeaseRequestStates = Enum(['UNTRIAGED', 'PENDING', 'FULFILLED', 'DENIED'])


class LeaseRequest(ndb.Model):
  """Datastore representation of a LeaseRequest.

  Key:
    id: Hash of the client + client-generated request ID which issued the
      original rpc_messages.LeaseRequest instance. Used for easy deduplication.
    kind: LeaseRequest. This root entity does not reference any parents.
  """
  # Checksum of the rpc_messages.LeaseRequest instance. Used to compare incoming
  # LeaseRequets for deduplication.
  deduplication_checksum = ndb.StringProperty(required=True, indexed=False)
  # auth.model.Identity of the issuer of the original request.
  owner = auth.IdentityProperty(required=True)
  # Element of LeaseRequestStates giving the state of this request.
  state = ndb.StringProperty(choices=LeaseRequestStates, required=True)
  # rpc_messages.LeaseRequest instance representing the original request.
  request = msgprop.MessageProperty(rpc_messages.LeaseRequest, required=True)
  # rpc_messages.LeaseResponse instance representing the current response.
  # This field will be updated as the request is being processed.
  response = msgprop.MessageProperty(rpc_messages.LeaseResponse)
  # DateTime indicating original datastore write time.
  created_ts = ndb.DateTimeProperty(auto_now_add=True)


class CatalogEntry(ndb.Model):
  """Datastore representation of a machine in the catalog."""
  # rpc_messages.Dimensions describing this machine.
  dimensions = msgprop.MessageProperty(
      rpc_messages.Dimensions,
      indexed_fields=[
          field.name for field in rpc_messages.Dimensions.all_fields()
      ],
  )

  @classmethod
  def compute_id(cls, dimensions):
    """Computes the ID of a CatalogEntry with the given dimensions.

    Args:
      dimensions: rpc_messages.Dimensions describing this machine.
    """
    assert dimensions.backend is not None
    if dimensions.hostname:
      return hashlib.sha1(
          '%s\0%s' % (dimensions.backend, dimensions.hostname)
      ).hexdigest()
    return None

  @classmethod
  def create_and_put(cls, dimensions):
    """Creates a new CatalogEntry entity and puts it in the datastore.

    Args:
      dimensions: rpc_messages.Dimensions describing this machine.
    """
    # CatalogEntries require the backend dimension to be specified, but leave
    # the hostname dimension optional. However, CatalogEntries must enforce
    # per-backend hostname uniqueness when hostname is defined.
    assert dimensions.backend is not None
    if dimensions.hostname:
      cls(dimensions=dimensions, id=cls.compute_id(dimensions)).put()
    else:
      cls(dimensions=dimensions).put()

  def has_exact_dimensions(self, dimensions):
    """Returns whether this CatalogEntry has exactly the given dimensions.

    This will match unspecified/None-valued dimensions.

    Args:
      dimensions: rpc_messages.Dimensions to check for.

    Returns:
      True if this CatalogEntry has exactly the given dimensions, else False.
    """
    for field in dimensions.all_fields():
      if getattr(self.dimensions, field.name) != getattr(dimensions, field.name):
        return False
    return True

  @classmethod
  def query_by_exact_dimensions(cls, dimensions):
    """Queries for CatalogEntries exactly matching the given dimensions.

    This will match unspecifed/None-valued dimensions.

    Args:
      dimensions: rpc_messages.Dimensions to query for.

    Returns:
      A list of matching CatalogEntry instances.
    """
    filters = [
        getattr(cls.dimensions, field.name) == getattr(dimensions, field.name)
        for field in dimensions.all_fields()
    ]
    return [
        e for e in cls.query(*filters) if e.has_exact_dimensions(dimensions)
    ]

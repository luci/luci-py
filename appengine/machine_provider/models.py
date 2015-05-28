# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Datastore models for the Machine Provider messages."""

from google.appengine.ext import ndb
from google.appengine.ext.ndb import msgprop

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

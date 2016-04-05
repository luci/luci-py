# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Endpoints version of is_member API."""

import logging
import re

import endpoints
from protorpc import messages
from protorpc import remote

from google.appengine.ext import ndb

from . import acl
from .. import api
from .. import endpoints_support
from .. import model
from .. import openid


### ProtoRPC Messages


class MembershipRequest(messages.Message):
  group = messages.StringField(1, required=True)
  identity = messages.StringField(2, required=True)


class MembershipResponse(messages.Message):
  is_member = messages.BooleanField(1)


class OpenIDConfig(messages.Message):
  client_id = messages.StringField(1)
  client_secret = messages.StringField(2)
  redirect_uri = messages.StringField(3)


### API


@endpoints_support.endpoints_api(name='auth', version='v1')
class AuthService(remote.Service):
  """Verifies if a given identity is a member of a particular group."""

  @endpoints_support.endpoints_method(
      MembershipRequest, MembershipResponse,
      http_method='GET',
      path='/membership')
  @api.require(acl.has_access)
  def membership(self, request):
    identity = request.identity
    if ':' not in identity:
      identity = 'user:%s' % identity
    try:
      identity = model.Identity.from_bytes(identity)
    except ValueError as e:
      raise endpoints.BadRequestException('Invalid identity: %s.' % e)
    is_member = api.is_group_member(request.group, identity)
    return MembershipResponse(is_member=is_member)

  @endpoints_support.endpoints_method(
      OpenIDConfig, OpenIDConfig,
      http_method='POST',
      path='/admin/openid/config')
  @api.require(api.is_admin)
  def configure_openid(self, request):
    @ndb.transactional
    def txn():
      conf = openid.get_config()
      conf.populate(
          client_id=request.client_id or conf.client_id,
          client_secret=request.client_secret or conf.client_secret,
          redirect_uri=request.redirect_uri or conf.redirect_uri)
      conf.put()
      return conf
    conf = txn()
    return OpenIDConfig(
        client_id=conf.client_id,
        client_secret='*****' if conf.client_secret else '',
        redirect_uri=conf.redirect_uri)

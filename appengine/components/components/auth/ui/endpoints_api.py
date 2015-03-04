# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Endpoints version of is_member API."""

import logging
import re

import endpoints
from protorpc import messages
from protorpc import remote

from .. import api
from .. import endpoints_support
from .. import model


### ProtoRPC Messages


class MembershipRequest(messages.Message):
  group = messages.StringField(1, required=True)
  identity = messages.StringField(2, required=True)


class MembershipResponse(messages.Message):
  is_member = messages.BooleanField(1)


### Authorization


def has_read_access():
  """Returns True if current caller can read groups and other auth data.

  Used in @require(...) decorators of API handlers.
  """
  return api.is_admin() or api.is_group_member('groups-readonly-access')


def has_write_access():
  """Returns True if current caller can modify groups and other auth data.

  Used in @require(...) decorators of API handlers.
  """
  return api.is_admin()


### API


@endpoints_support.endpoints_api(name='auth', version='v1')
class AuthService(remote.Service):
  """Verifies if a given identity is a member of a particular group."""

  @endpoints_support.endpoints_method(
      MembershipRequest, MembershipResponse, http_method='GET',
      path='/membership')
  @api.require(has_read_access)
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

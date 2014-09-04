# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Configures includes (appstats and components.auth).

https://developers.google.com/appengine/docs/python/tools/appengineconfig
"""

def get_custom_authenticators():
  # Lazy import to break modules reference cycle.
  from server import acl
  return [acl.ip_whitelist_authentication]


appstats_CALC_RPC_COSTS = False

components_auth_UI_APP_NAME = 'Swarming'
components_auth_CUSTOM_AUTHENTICATORS_HOOK = get_custom_authenticators

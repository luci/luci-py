# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Configures includes (appstats and components.auth).

https://developers.google.com/appengine/docs/python/tools/appengineconfig
"""

appstats_CALC_RPC_COSTS = False

# Auth component UI is tweaked manually, see frontend/handlers.py.
components_auth_UI_CUSTOM_CONFIG = True

# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Configures appstats.

https://developers.google.com/appengine/docs/python/tools/appengineconfig
"""

# Enable appstats and optionally cost calculation.
# Change these values and upload again if you want to enable appstats.
enable_appstats = False
appstats_CALC_RPC_COSTS = False


def webapp_add_wsgi_middleware(app):
  """Overrides the wsgi application with appstats if enabled.

  https://developers.google.com/appengine/docs/python/tools/appstats
  """
  if enable_appstats:
    # pylint: disable=E0611,F0401
    from google.appengine.ext.appstats import recording
    return recording.appstats_wsgi_middleware(app)

  return app

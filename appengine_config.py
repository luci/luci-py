# Copyright 2013 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.

"""Configures appstats if enabled in the config.

See config.py for more information.

https://developers.google.com/appengine/docs/python/tools/appengineconfig
"""

_SETTINGS = __import__('config').settings()


appstats_CALC_RPC_COSTS = (
    _SETTINGS.enable_appstats and _SETTINGS.enable_appstats_cost)


def webapp_add_wsgi_middleware(app):
  """Overrides the wsgi application with appstats if enabled.

  https://developers.google.com/appengine/docs/python/tools/appstats
  """
  if _SETTINGS.enable_appstats:
    # pylint: disable=E0611,F0401
    from google.appengine.ext.appstats import recording
    return recording.appstats_wsgi_middleware(app)

  return app

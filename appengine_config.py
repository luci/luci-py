# Copyright 2013 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.

# The app engine headers are located locally, so don't worry about not finding
# them.
# pylint: disable=E0611,F0401
from google.appengine.ext.appstats import recording
# pylint: enable=E0611,F0401


def webapp_add_wsgi_middleware(app):
  return recording.appstats_wsgi_middleware(app)

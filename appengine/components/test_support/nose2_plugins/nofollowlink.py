# Copyright 2019 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import logging
import os

from nose2.events import Plugin

log = logging.getLogger('nose2.plugins.nofollowlink')


class NoFollowLink(Plugin):
  alwaysOn = True

  def handleDir(self, event):
    log.info('handleDir path=%s', event.path)
    if os.path.islink(event.path):
      log.info('ignoring link')
      event.handled = True

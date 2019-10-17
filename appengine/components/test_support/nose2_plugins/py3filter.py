# Copyright 2019 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import logging

from nose2.events import Plugin

log = logging.getLogger('nose2.plugins.py3filter')


class Py3Filter(Plugin):
  commandLineSwitch = (None, 'python3', 'filter python3 test files')

  def matchPath(self, event):
    log.debug('matchPath path=%s', event.path)

    event.handled = True
    return _has_py3_shebang(event.path)


def _has_py3_shebang(path):
  with open(path, 'r') as f:
    maybe_shebang = f.readline()
  return maybe_shebang.startswith('#!') and 'python3' in maybe_shebang

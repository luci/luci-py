# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import os

from components import template

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


def bootstrap():
  template.bootstrap({'ereporter2': os.path.join(ROOT_DIR, 'templates')})


def render(name, params=None):
  """Shorthand to render a template."""
  return template.render(name, params)

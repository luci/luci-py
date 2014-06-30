# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import os

from components import utils
from components import template

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


def bootstrap():
  global_env = {
    'app_version': utils.get_app_version(),
    'app_revision_url': utils.get_app_revision_url(),
  }
  template.bootstrap([os.path.join(ROOT_DIR, 'templates')], global_env, {})


def render(name, params):
  """Shorthand to render a template."""
  return template.render(name, params)

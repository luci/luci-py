# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Setups jinja2 environment."""

import os

import jinja2


ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

JINJA = jinja2.Environment(
    loader=jinja2.FileSystemLoader(os.path.join(ROOT_DIR, 'templates')),
    extensions=['jinja2.ext.autoescape'],
    autoescape=True)


# Registers library custom filters.


def datetimeformat(value, f='%Y-%m-%d %H:%M'):
  return value.strftime(f)


JINJA.filters['datetimeformat'] = datetimeformat


def get(*args, **kwargs):
  """Shorthand to load a template."""
  return JINJA.get_template(*args, **kwargs)

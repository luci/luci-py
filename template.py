# Copyright 2013 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.

import os

# The app engine headers are located locally, so don't worry about not finding
# them.
# pylint: disable=E0611,F0401
import jinja2
# pylint: enable=E0611,F0401

ROOT_DIR = os.path.dirname(__file__)

JINJA = jinja2.Environment(
    loader=jinja2.FileSystemLoader(os.path.join(ROOT_DIR, 'templates')),
    extensions=['jinja2.ext.autoescape'])


def get(*args, **kwargs):
  return JINJA.get_template(*args, **kwargs)

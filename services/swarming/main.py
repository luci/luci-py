# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Defines the application."""

from google.appengine.ext import ereporter

import handlers


def CreateApplication():
  ereporter.register_logger()
  return handlers.CreateApplication()


app = CreateApplication()

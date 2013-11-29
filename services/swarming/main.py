# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Defines the application."""

from components import ereporter2

import handlers


def CreateApplication():
  ereporter2.register_formatter()
  return handlers.CreateApplication()


app = CreateApplication()

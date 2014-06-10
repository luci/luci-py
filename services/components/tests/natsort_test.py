#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Unit tests for nasort.py."""

import doctest
import os
import sys

COMPONENTS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'components')
sys.path.insert(0, COMPONENTS_DIR)

import natsort


if __name__ == '__main__':
  doctest.testmod(natsort)

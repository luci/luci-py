#!/usr/bin/env python
# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Compiles all *.proto files it finds into *_pb2.py."""

import os
import sys

APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(APP_DIR, '..', 'components'))
sys.path.insert(0, os.path.join(APP_DIR, '..', 'third_party_local'))

from tools import compile_proto


if __name__ == '__main__':
  sys.exit(compile_proto.main(sys.argv[1:], APP_DIR))

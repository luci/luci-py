# Copyright 2015 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.

import os
import logging
import sys

third_party_dir = os.path.join(os.path.dirname(__file__), '..', 'third_party')
if third_party_dir not in sys.path:  # pragma: no cover
  sys.path.insert(0, third_party_dir)

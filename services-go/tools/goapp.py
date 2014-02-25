#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import sys

import find_gae_sdk

sys.exit(find_gae_sdk.run(['goapp'] + sys.argv[1:]))

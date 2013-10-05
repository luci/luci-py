# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Describes an app properties."""

import glob
import os

# Root application directory.
APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# List of paths to *.yaml files that define app modules, default first.
MODULES = ([os.path.join(APP_DIR, 'app.yaml')] +
    glob.glob(os.path.join(APP_DIR, 'module-*.yaml')))

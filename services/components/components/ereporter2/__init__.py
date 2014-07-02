# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Ereporter2 monitor exceptions and error logs and generates reports
automatically.

Inspired by google/appengine/ext/ereporter but crawls logservice instead of
using the DB. This makes this service works even if the DB is broken, as the
only dependency is logservice.
"""

# Wildcard import - pylint: disable=W0401
from .api import *
from .handlers import *
from .on_error import *
from .ui import *

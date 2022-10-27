# Copyright 2018 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Utility functions for Protocol Buffers."""

from .field_masks import *
from .multiline_proto import parse_multiline, MultilineParseError
from .protoutil import merge_dict

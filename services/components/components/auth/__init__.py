# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Authentication and authorization component.

Exports public API of 'auth' component. Each module in 'auth' package can
export a portion of public API by specifying exported symbols in its __all__.
"""

# Pylint doesn't like relative wildcard imports.
# pylint: disable=W0401,W0403

# Auth component is using google.protobuf package, it requires some python
# package magic hacking.
from components import utils
utils.fix_protobuf_package()

from api import *
from handler import *
from model import *
from tokens import *

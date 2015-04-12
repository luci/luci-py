# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Authentication and authorization component.

Exports public API of 'auth' component. Each module in 'auth' package can
export a portion of public API by specifying exported symbols in its __all__.
"""

# Pylint doesn't like relative wildcard imports.
# pylint: disable=W0401,W0403

from version import __version__

try:
  import endpoints
except ImportError:
  endpoints = None

# Auth component is using google.protobuf package, it requires some python
# package magic hacking.
from components import utils
utils.fix_protobuf_package()

from api import *
from handler import *
from host_token import *
from ipaddr import *
from model import *
from service_account import *
from signature import *
from tokens import *
from ui.app import *

# Endpoints support is optional, enabled only when endpoints library is
# specified in app.yaml.
if endpoints:
  from endpoints_support import *
  from ui.endpoints_api import AuthService

# Import 'config' to register lib_config hook.
import config

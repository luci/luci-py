# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Authentication and authorization component.

Exports public API of 'auth' component. Each module in 'auth' package can
export a portion of public API by specifying exported symbols in its __all__.
"""

from .version import __version__  # noqa: F401

try:
  import endpoints
except ImportError:
  endpoints = None

# Auth component is using google.protobuf package, it requires some python
# package magic hacking.
from components import utils

utils.fix_protobuf_package()

from api import *  # noqa: E402, F403
from delegation import *  # noqa: E402, F403
from exceptions import *  # noqa: E402, F403
from gce_vm_auth import *  # noqa: E402, F403
from handler import *  # noqa: E402, F403
from ipaddr import *  # noqa: E402, F403
from machine_auth import *  # noqa: E402, F403
from model import *  # noqa: E402, F403
from prpc import *  # noqa: E402, F403
from service_account import *  # noqa: E402, F403
from signature import *  # noqa: E402, F403
from tokens import *  # noqa: E402, F403
from ui.app import *  # noqa: E402, F403

# Endpoints support is optional, enabled only when endpoints library is
# specified in app.yaml.
if endpoints:
  from endpoints_support import *  # noqa: F403
  from ui.endpoints_api import AuthService  # noqa: F401

# Import 'config' to register lib_config hook.
import config  # noqa: E402, F401

# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Authentication and authorization component.

Exports public API of 'auth' component. Each module in 'auth' package can
export a portion of public API by specifying exported symbols in its __all__.
"""

from .version import __version__

from components import utils

utils.ensure_endpoints_on_path()
import endpoints

# Auth component is using google.protobuf package, it requires some python
# package magic hacking.
utils.fix_protobuf_package()

from api import *
from delegation import *
from exceptions import *
from gce_vm_auth import *
from handler import *
from ipaddr import *
from machine_auth import *
from model import *
from prpc import *
from service_account import *
from signature import *
from tokens import *
from ui.app import *

from endpoints_support import *
from ui.endpoints_api import AuthService

# Import 'config' to register lib_config hook.
import config

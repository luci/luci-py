#!/usr/bin/python
#
# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""Google Cloud Endpoints module."""

# pylint: disable=wildcard-import
from __future__ import absolute_import

import logging

from protorpc import message_types
from protorpc import messages
from protorpc import remote

from .api_config import api, method
from .api_config import AUTH_LEVEL, EMAIL_SCOPE
from .api_config import Issuer, LimitDefinition, Namespace
from .api_exceptions import *
from .apiserving import *
from .constants import API_EXPLORER_CLIENT_ID
from .endpoints_dispatcher import *
from . import message_parser
from .resource_container import ResourceContainer
from .users_id_token import get_current_user, get_verified_jwt, convert_jwks_uri
from .users_id_token import InvalidGetUserCall
from .users_id_token import SKIP_CLIENT_ID_CHECK

__version__ = '4.8.0'

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)

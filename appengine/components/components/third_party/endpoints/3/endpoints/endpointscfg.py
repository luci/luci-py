#!/usr/bin/python
# Copyright 2017 Google Inc. All Rights Reserved.
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

r"""Wrapper script to set up import paths for endpointscfg.

The actual implementation is in _endpointscfg_impl, but we have to set
up import paths properly before we can import that module.

See the docstring for endpoints._endpointscfg_impl for more
information about this script's capabilities.
"""

import sys

import _endpointscfg_setup  # pylint: disable=unused-import
from endpoints._endpointscfg_impl import main

if __name__ == '__main__':
  main(sys.argv)

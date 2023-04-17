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

"""Provide various constants needed by Endpoints Framework.

Putting them in this file makes it easier to avoid circular imports,
as well as keep from complicating tests due to importing code that
uses App Engine apis.
"""

from __future__ import absolute_import

__all__ = [
    'API_EXPLORER_CLIENT_ID',
]


API_EXPLORER_CLIENT_ID = '292824132082.apps.googleusercontent.com'

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

"""Provide various utility/container types needed by Endpoints Framework.

Putting them in this file makes it easier to avoid circular imports,
as well as keep from complicating tests due to importing code that
uses App Engine apis.
"""

from __future__ import absolute_import

import six

import attr

__all__ = [
    'OAuth2Scope', 'Issuer', 'LimitDefinition', 'Namespace',
]


@attr.s(frozen=True, slots=True)
class OAuth2Scope(object):
    scope = attr.ib(validator=attr.validators.instance_of(six.string_types))
    description = attr.ib(validator=attr.validators.instance_of(six.string_types))

    @classmethod
    def convert_scope(cls, scope):
        "Convert string scopes into OAuth2Scope objects."
        if isinstance(scope, cls):
            return scope
        return cls(scope=scope, description=scope)

    @classmethod
    def convert_list(cls, values):
        "Convert a list of scopes into a list of OAuth2Scope objects."
        if values is not None:
            return [cls.convert_scope(value) for value in values]

Issuer = attr.make_class('Issuer', ['issuer', 'jwks_uri'])
LimitDefinition = attr.make_class('LimitDefinition', ['metric_name',
                                                      'display_name',
                                                      'default_limit'])
Namespace = attr.make_class('Namespace', ['owner_domain',
                                          'owner_name',
                                          'package_path'])

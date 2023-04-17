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

"""A library for converting service configs to discovery directory lists."""

from __future__ import absolute_import

import collections
import json
import re
from six.moves import urllib

from . import util


class DirectoryListGenerator(object):
  """Generates a discovery directory list from a ProtoRPC service.

  Example:

    class HelloRequest(messages.Message):
      my_name = messages.StringField(1, required=True)

    class HelloResponse(messages.Message):
      hello = messages.StringField(1, required=True)

    class HelloService(remote.Service):

      @remote.method(HelloRequest, HelloResponse)
      def hello(self, request):
        return HelloResponse(hello='Hello there, %s!' %
                             request.my_name)

    api_config = DirectoryListGenerator().pretty_print_config_to_json(
        HelloService)

  The resulting document will be a JSON directory list describing the APIs
  implemented by HelloService.
  """

  def __init__(self, request=None):
    # The ApiRequest that called this generator
    self.__request = request

  def __item_descriptor(self, config):
    """Builds an item descriptor for a service configuration.

    Args:
      config: A dictionary containing the service configuration to describe.

    Returns:
      A dictionary that describes the service configuration.
    """
    descriptor = {
        'kind': 'discovery#directoryItem',
        'icons': {
            'x16': 'https://www.gstatic.com/images/branding/product/1x/'
                   'googleg_16dp.png',
            'x32': 'https://www.gstatic.com/images/branding/product/1x/'
                   'googleg_32dp.png',
        },
        'preferred': True,
    }

    description = config.get('description')
    root_url = config.get('root')
    name = config.get('name')
    version = config.get('api_version')
    relative_path = '/apis/{0}/{1}/rest'.format(name, version)

    if description:
      descriptor['description'] = description

    descriptor['name'] = name
    descriptor['version'] = version
    descriptor['discoveryLink'] = '.{0}'.format(relative_path)

    root_url_port = urllib.parse.urlparse(root_url).port

    original_path = self.__request.reconstruct_full_url(
        port_override=root_url_port)
    descriptor['discoveryRestUrl'] = '{0}/{1}/{2}/rest'.format(
        original_path, name, version)

    if name and version:
      descriptor['id'] = '{0}:{1}'.format(name, version)

    return descriptor

  def __directory_list_descriptor(self, configs):
    """Builds a directory list for an API.

    Args:
      configs: List of dicts containing the service configurations to list.

    Returns:
      A dictionary that can be deserialized into JSON in discovery list format.

    Raises:
      ApiConfigurationError: If there's something wrong with the API
        configuration, such as a multiclass API decorated with different API
        descriptors (see the docstring for api()), or a repeated method
        signature.
    """
    descriptor = {
        'kind': 'discovery#directoryList',
        'discoveryVersion': 'v1',
    }

    items = []
    for config in configs:
      item_descriptor = self.__item_descriptor(config)
      if item_descriptor:
        items.append(item_descriptor)

    if items:
      descriptor['items'] = items

    return descriptor

  def get_directory_list_doc(self, configs):
    """JSON dict description of a protorpc.remote.Service in list format.

    Args:
      configs: Either a single dict or a list of dicts containing the service
        configurations to list.

    Returns:
      dict, The directory list document as a JSON dict.
    """

    if not isinstance(configs, (tuple, list)):
      configs = [configs]

    util.check_list_type(configs, dict, 'configs', allow_none=False)

    return self.__directory_list_descriptor(configs)

  def pretty_print_config_to_json(self, configs):
    """JSON string description of a protorpc.remote.Service in a discovery doc.

    Args:
      configs: Either a single dict or a list of dicts containing the service
        configurations to list.

    Returns:
      string, The directory list document as a JSON string.
    """
    descriptor = self.get_directory_list_doc(configs)
    return json.dumps(descriptor, sort_keys=True, indent=2,
                      separators=(',', ': '))

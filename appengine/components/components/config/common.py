# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import re

from google.appengine.api import app_identity
from google.appengine.api import lib_config
from google.appengine.ext import ndb

# Config component is using google.protobuf package, it requires some python
# package magic hacking.
from components import utils
utils.fix_protobuf_package()

from google import protobuf

from components import utils
from components.datastore_utils import config


################################################################################
# Patterns


SERVICE_ID_PATTERN = r'[a-z0-9\-_]+'
SERVICE_ID_RGX = re.compile(r'^%s$' % SERVICE_ID_PATTERN)
SERVICE_CONFIG_SET_RGX = re.compile(r'^services/(%s)$' % SERVICE_ID_PATTERN)

PROJECT_ID_PATTERN = SERVICE_ID_PATTERN
PROJECT_ID_RGX = re.compile(r'^%s$' % PROJECT_ID_PATTERN)
PROJECT_CONFIG_SET_RGX = re.compile(r'^projects/(%s)$' % PROJECT_ID_PATTERN)

REF_CONFIG_SET_RGX = re.compile(
    r'^projects/(%s)/refs/.+$' % PROJECT_ID_PATTERN)

ALL_CONFIG_SET_RGX = [
  SERVICE_CONFIG_SET_RGX,
  PROJECT_CONFIG_SET_RGX,
  REF_CONFIG_SET_RGX,
]


################################################################################
# Settings


class ConstantConfig(object):
  # In filesystem mode, the directory where configs are read from.
  CONFIG_DIR = 'configs'


CONSTANTS = lib_config.register('components_config', ConstantConfig.__dict__)


class ConfigSettings(config.GlobalConfig):
  # Hostname of the config service.
  service_hostname = ndb.StringProperty(indexed=False)


################################################################################
# Config parsing


class ConfigFormatError(Exception):
  """A config file is malformed."""


def _validate_dest_type(dest_type):
  if dest_type is None:
    return
  if not issubclass(dest_type, protobuf.message.Message):
    raise NotImplementedError('%s type is not supported' % dest_type.__name__)


def _convert_config(content, dest_type):
  _validate_dest_type(dest_type)
  if dest_type is None or isinstance(content, dest_type):
    return content
  msg = dest_type()
  if content:
    try:
      protobuf.text_format.Merge(content, msg)
    except protobuf.text_format.ParseError as ex:
      raise ConfigFormatError(ex.message)
  return msg


################################################################################
# Rest


@utils.cache
def self_config_set():
  return 'services/%s' % app_identity.get_application_id()

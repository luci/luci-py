# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

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


class ConstantConfig(object):
  # In filesystem mode, the directory where configs are read from.
  CONFIG_DIR = 'configs'


class ConfigFormatError(Exception):
  """A config file is malformed."""


CONSTANTS = lib_config.register('components_config', ConstantConfig.__dict__)


class ConfigSettings(config.GlobalConfig):
  # Hostname of the config service.
  service_hostname = ndb.StringProperty(indexed=False)


@utils.cache
def self_config_set():
  return 'services/%s' % app_identity.get_application_id()


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

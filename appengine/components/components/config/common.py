# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

from google.appengine.api import lib_config
from google.appengine.ext import ndb

from components.datastore_utils import config


class ConstantConfig(object):
  # In filesystem mode, the directory where configs are read from.
  CONFIG_DIR = 'configs'


CONSTANTS = lib_config.register('components_config', ConstantConfig.__dict__)


class ConfigSettings(config.GlobalConfig):
  # Hostname of the config service.
  service_hostname = ndb.StringProperty(indexed=False)

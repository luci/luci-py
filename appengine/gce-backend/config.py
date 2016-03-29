# Copyright 2016 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Utilities for reading GCE Backend configuration."""

import logging

from components import utils
utils.fix_protobuf_package()

from google import protobuf
from google.appengine.ext import ndb

from components import config
from components import datastore_utils

from proto import config_pb2


class Configuration(datastore_utils.config.GlobalConfig):
  """Configuration for this service."""
  # Text-formatted proto.config_pb2.InstanceTemplateConfig.
  template_config = ndb.TextProperty()
  # Text-formatted proto.config_pb2.InstanceGroupManagerConfig.
  manager_config = ndb.TextProperty()
  # Revision of the configs.
  revision = ndb.StringProperty()


def update_config():
  """Updates the local configuration from the config service."""
  revision, template_config = config.get_self_config(
      'templates.cfg',
      dest_type=config_pb2.InstanceTemplateConfig,
  )
  _, manager_config = config.get_self_config(
      'managers.cfg',
      dest_type=config_pb2.InstanceGroupManagerConfig,
      revision=revision,
  )

  context = config.validation_context.Context.logging()
  validate_template_config(template_config, context)
  if context.result().has_errors:
    logging.error('Not updating configuration due to errors in templates.cfg')
    return

  context = config.validation_context.Context.logging()
  validate_manager_config(manager_config, context)
  if context.result().has_errors:
    logging.error('Not updating configuration due to errors in managers.cfg')
    return

  stored_config = Configuration.fetch()
  if stored_config.revision != revision:
    logging.info('Updating configuration to %s', revision)
    stored_config.modify(
      manager_config=protobuf.text_format.MessageToString(manager_config),
      revision=revision,
      template_config=protobuf.text_format.MessageToString(template_config),
    )


@config.validation.self_rule('templates.cfg', config_pb2.InstanceTemplateConfig)
def validate_template_config(config, context):
  """Validates an InstanceTemplateConfig instance."""
  # We don't do any GCE-specific validation here. Just require globally
  # unique base name because base name is used as the key in the datastore.
  base_names = set()
  for template in config.templates:
    if template.base_name in base_names:
      context.error('base_name %s is not globally unique.', template.base_name)
    else:
      base_names.add(template.base_name)


@config.validation.self_rule(
    'managers.cfg',
    config_pb2.InstanceGroupManagerConfig,
)
def validate_manager_config(config, context):
  """Validates an InstanceGroupManagerConfig instance."""
  pass

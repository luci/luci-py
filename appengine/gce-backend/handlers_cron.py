# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""GCE Backend cron jobs."""

import logging

import webapp2

from components import decorators

import config
import instance_group_managers
import instance_templates
import parse


class ConfigImportHandler(webapp2.RequestHandler):
  """Worker for importing the config."""

  @decorators.require_cronjob
  def get(self):
    config.update_config()


class ConfigProcessHandler(webapp2.RequestHandler):
  """Worker for processing the config."""

  @decorators.require_cronjob
  def get(self):
    template_config, manager_config = config.Configuration.load()
    parse.parse(
        template_config.templates,
        manager_config.managers,
        max_concurrent=10,
        max_concurrent_igm=10,
    )


class InstanceGroupManagerCreationHandler(webapp2.RequestHandler):
  """Worker for creating instance group managers."""

  @decorators.require_cronjob
  def get(self):
    instance_group_managers.schedule_creation()


class InstanceTemplateCreationHandler(webapp2.RequestHandler):
  """Worker for creating instance templates."""

  @decorators.require_cronjob
  def get(self):
    instance_templates.schedule_creation()


def create_cron_app():
  return webapp2.WSGIApplication([
      ('/internal/cron/create-instance-group-managers',
       InstanceGroupManagerCreationHandler),
      ('/internal/cron/create-instance-templates',
       InstanceTemplateCreationHandler),
      ('/internal/cron/import-config', ConfigImportHandler),
      ('/internal/cron/process-config', ConfigProcessHandler),
  ])

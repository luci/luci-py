# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""GCE Backend cron jobs."""

import logging

import webapp2

from components import decorators

import cleanup
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


class EntityCleanupHandler(webapp2.RequestHandler):
  """Worker for cleaning up datastore entities."""

  @decorators.require_cronjob
  def get(self):
    cleanup.cleanup_instance_group_managers()
    cleanup.cleanup_instance_template_revisions()
    cleanup.cleanup_instance_templates()


class InstanceGroupManagerCreationHandler(webapp2.RequestHandler):
  """Worker for creating instance group managers."""

  @decorators.require_cronjob
  def get(self):
    instance_group_managers.schedule_creation()


class InstanceGroupManagerDeletionHandler(webapp2.RequestHandler):
  """Worker for deleting instance group managers."""

  @decorators.require_cronjob
  def get(self):
    instance_group_managers.schedule_deletion()


class InstanceTemplateCreationHandler(webapp2.RequestHandler):
  """Worker for creating instance templates."""

  @decorators.require_cronjob
  def get(self):
    instance_templates.schedule_creation()


class InstanceTemplateDeletionHandler(webapp2.RequestHandler):
  """Worker for deleting instance templates."""

  @decorators.require_cronjob
  def get(self):
    instance_templates.schedule_deletion()


def create_cron_app():
  return webapp2.WSGIApplication([
      ('/internal/cron/cleanup-entities', EntityCleanupHandler),
      ('/internal/cron/create-instance-group-managers',
       InstanceGroupManagerCreationHandler),
      ('/internal/cron/create-instance-templates',
       InstanceTemplateCreationHandler),
      ('/internal/cron/delete-instance-group-managers',
       InstanceGroupManagerDeletionHandler),
      ('/internal/cron/delete-instance-templates',
       InstanceTemplateDeletionHandler),
      ('/internal/cron/import-config', ConfigImportHandler),
      ('/internal/cron/process-config', ConfigProcessHandler),
  ])

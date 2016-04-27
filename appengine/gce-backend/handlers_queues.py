# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Task queues for the GCE Backend."""

import json
import logging

from google.appengine.ext import ndb
import webapp2

from components import decorators

import catalog
import instance_group_managers
import instance_templates
import instances
import metadata
import pubsub


class InstanceCatalogHandler(webapp2.RequestHandler):
  """Worker for cataloging instances."""

  @decorators.require_taskqueue('catalog-instance')
  def post(self):
    """Adds an instance to the Machine Provider catalog.

    Args:
      key: URL-safe key for a models.Instance.
    """
    key = ndb.Key(urlsafe=self.request.get('key'))
    assert key.kind() == 'Instance', key
    catalog.catalog(key)


class InstanceFetchHandler(webapp2.RequestHandler):
  """Worker for fetching instances for an instance group manager."""

  @decorators.require_taskqueue('fetch-instances')
  def post(self):
    """Fetches instances for the given InstanceGroupManager.

    Params:
      key: URL-safe key for a models.InstanceGroupManager.
    """
    key = ndb.Key(urlsafe=self.request.get('key'))
    assert key.kind() == 'InstanceGroupManager', key
    instances.ensure_entities_exist(key)


class InstanceGroupManagerCreationHandler(webapp2.RequestHandler):
  """Worker for creating instance group managers from the config."""

  @decorators.require_taskqueue('create-instance-group-manager')
  def post(self):
    """Creates an instance group manager for the given InstanceGroupManager.

    Params:
      key: URL-safe key for a models.InstanceGroupManager.
    """
    key = ndb.Key(urlsafe=self.request.get('key'))
    assert key.kind() == 'InstanceGroupManager', key
    instance_group_managers.create(key)


class InstanceGroupManagerDeletionHandler(webapp2.RequestHandler):
  """Worker for deleting drained instance group managers."""

  @decorators.require_taskqueue('delete-instance-group-manager')
  def post(self):
    """Deletes the instance group manager for the given InstanceGroupManager.

    Params:
      key: URL-safe key for a models.InstanceGroupManager.
    """
    key = ndb.Key(urlsafe=self.request.get('key'))
    assert key.kind() == 'InstanceGroupManager', key
    instance_group_managers.delete(key)


class InstanceMetadataOperationCheckHandler(webapp2.RequestHandler):
  """Worker for checking an instance metadata operation."""

  @decorators.require_taskqueue('check-instance-metadata-operation')
  def post(self):
    """Checks a metadata operation for the given Instance.

    Params:
      key: URL-safe key for a models.Instance.
    """
    key = ndb.Key(urlsafe=self.request.get('key'))
    assert key.kind() == 'Instance', key
    metadata.check(key)


class InstanceMetadataUpdateHandler(webapp2.RequestHandler):
  """Worker for updating instance metadata."""

  @decorators.require_taskqueue('update-instance-metadata')
  def post(self):
    """Schedules a metadata update for the given Instance.

    Params:
      key: URL-safe key for a models.Instance.
    """
    key = ndb.Key(urlsafe=self.request.get('key'))
    assert key.kind() == 'Instance', key
    metadata.update(key)


class InstanceMetadataUpdatesCompressionHandler(webapp2.RequestHandler):
  """Worker for compressing pending instance metadata updates."""

  @decorators.require_taskqueue('compress-instance-metadata-updates')
  def post(self):
    """Schedules a pending metadata update compression for the given Instance.

    Params:
      key: URL-safe key for a models.Instance.
    """
    key = ndb.Key(urlsafe=self.request.get('key'))
    assert key.kind() == 'Instance', key
    metadata.compress(key)


class InstanceTemplateCreationHandler(webapp2.RequestHandler):
  """Worker for creating instance templates from the config."""

  @decorators.require_taskqueue('create-instance-template')
  def post(self):
    """Creates an instance template for the given InstanceTemplateRevision.

    Params:
      key: URL-safe key for a models.InstanceTemplateRevision.
    """
    key = ndb.Key(urlsafe=self.request.get('key'))
    assert key.kind() == 'InstanceTemplateRevision', key
    instance_templates.create(key)


class InstanceTemplateDeletionHandler(webapp2.RequestHandler):
  """Worker for deleting drained instance templates."""

  @decorators.require_taskqueue('delete-instance-template')
  def post(self):
    """Deletes the instance template for the given InstanceTemplateRevision.

    Params:
      key: URL-safe key for a models.InstanceTemplateRevision.
    """
    key = ndb.Key(urlsafe=self.request.get('key'))
    assert key.kind() == 'InstanceTemplateRevision', key
    instance_templates.delete(key)


class PubSubMessageProcessHandler(webapp2.RequestHandler):
  """Worker for polling and processing Pub/Sub messages."""

  @decorators.require_taskqueue('process-pubsub-messages')
  def post(self):
    """Polls and processes Pub/Sub messages."""
    pubsub.poll()


def create_queues_app():
  return webapp2.WSGIApplication([
      ('/internal/queues/catalog-instance', InstanceCatalogHandler),
      ('/internal/queues/check-instance-metadata-operation',
       InstanceMetadataOperationCheckHandler),
      ('/internal/queues/compress-instance-metadata-updates',
       InstanceMetadataUpdatesCompressionHandler),
      ('/internal/queues/create-instance-group-manager',
       InstanceGroupManagerCreationHandler),
      ('/internal/queues/create-instance-template',
       InstanceTemplateCreationHandler),
      ('/internal/queues/delete-instance-group-manager',
       InstanceGroupManagerDeletionHandler),
      ('/internal/queues/delete-instance-template',
       InstanceTemplateDeletionHandler),
      ('/internal/queues/fetch-instances', InstanceFetchHandler),
      ('/internal/queues/process-pubsub-messages', PubSubMessageProcessHandler),
      ('/internal/queues/update-instance-metadata',
       InstanceMetadataUpdateHandler),
  ])

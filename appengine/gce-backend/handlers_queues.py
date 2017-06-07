# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Task queues for the GCE Backend."""

import json
import logging

from google.appengine.ext import ndb
import webapp2

from components import decorators

import catalog
import cleanup
import instance_group_managers
import instance_templates
import instances
import metadata


class CatalogedInstanceRemovalHandler(webapp2.RequestHandler):
  """Worker for removing cataloged instances."""

  @decorators.require_taskqueue('remove-cataloged-instance')
  def post(self):
    """Removes a cataloged instance.

    Params:
      key: URL-safe key for a models.Instance.
    """
    key = ndb.Key(urlsafe=self.request.get('key'))
    assert key.kind() == 'Instance', key
    catalog.remove(key)


class CatalogedInstanceUpdateHandler(webapp2.RequestHandler):
  """Worker for updating information about cataloged instances."""

  @decorators.require_taskqueue('update-cataloged-instance')
  def post(self):
    """Updates information about a cataloged instance.

    Params:
      key: URL-safe key for a models.Instance.
    """
    key = ndb.Key(urlsafe=self.request.get('key'))
    assert key.kind() == 'Instance', key
    catalog.update_cataloged_instance(key)


class DeletedInstanceCheckHandler(webapp2.RequestHandler):
  """Worker for checking for deleted instances."""

  @decorators.require_taskqueue('check-deleted-instance')
  def post(self):
    """Checks whether an instance has been deleted.

    Params:
      key: URL-safe key for a models.Instance.
    """
    key = ndb.Key(urlsafe=self.request.get('key'))
    assert key.kind() == 'Instance', key
    cleanup.check_deleted_instance(key)


class DeletedInstanceCleanupHandler(webapp2.RequestHandler):
  """Worker for cleaning up deleted instances."""

  @decorators.require_taskqueue('cleanup-deleted-instance')
  def post(self):
    """Removes a deleted instance entity.

    Params:
      key: URL-safe key for a models.Instance.
    """
    key = ndb.Key(urlsafe=self.request.get('key'))
    assert key.kind() == 'Instance', key
    cleanup.cleanup_deleted_instance(key)


class DrainedInstanceCleanupHandler(webapp2.RequestHandler):
  """Worker for cleaning up drained instances."""

  @decorators.require_taskqueue('cleanup-drained-instance')
  def post(self):
    """Removes a drained instance entity.

    Params:
      key: URL-safe key for a models.Instance.
    """
    key = ndb.Key(urlsafe=self.request.get('key'))
    assert key.kind() == 'Instance', key
    cleanup.cleanup_drained_instance(key)


class InstanceCatalogHandler(webapp2.RequestHandler):
  """Worker for cataloging instances."""

  @decorators.require_taskqueue('catalog-instance')
  def post(self):
    """Adds an instance to the Machine Provider catalog.

    Params:
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


class InstanceGroupResizeHandler(webapp2.RequestHandler):
  """Worker for resizing managed instance groups."""

  @decorators.require_taskqueue('resize-instance-group')
  def post(self):
    """Resizes the instance group managed by the given InstanceGroupManager.

    Params:
      key: URL-safe key for a models.InstanceGroupManager.
    """
    key = ndb.Key(urlsafe=self.request.get('key'))
    assert key.kind() == 'InstanceGroupManager', key
    instance_group_managers.resize(key)


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


class InstancePendingDeletionDeletionHandler(webapp2.RequestHandler):
  """Worker for deleting instances pending deletion."""

  @decorators.require_taskqueue('delete-instance-pending-deletion')
  def post(self):
    """Deletes an instance pending deletion.

    Params:
      key: URL-safe key for a models.Instance.
    """
    key = ndb.Key(urlsafe=self.request.get('key'))
    assert key.kind() == 'Instance', key
    instances.delete_pending(key)


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


def create_queues_app():
  return webapp2.WSGIApplication([
      ('/internal/queues/catalog-instance', InstanceCatalogHandler),
      ('/internal/queues/check-deleted-instance',
       DeletedInstanceCheckHandler),
      ('/internal/queues/check-instance-metadata-operation',
       InstanceMetadataOperationCheckHandler),
      ('/internal/queues/cleanup-deleted-instance',
       DeletedInstanceCleanupHandler),
      ('/internal/queues/cleanup-drained-instance',
       DrainedInstanceCleanupHandler),
      ('/internal/queues/compress-instance-metadata-updates',
       InstanceMetadataUpdatesCompressionHandler),
      ('/internal/queues/create-instance-group-manager',
       InstanceGroupManagerCreationHandler),
      ('/internal/queues/create-instance-template',
       InstanceTemplateCreationHandler),
      ('/internal/queues/delete-instance-group-manager',
       InstanceGroupManagerDeletionHandler),
      ('/internal/queues/delete-instance-pending-deletion',
       InstancePendingDeletionDeletionHandler),
      ('/internal/queues/delete-instance-template',
       InstanceTemplateDeletionHandler),
      ('/internal/queues/fetch-instances', InstanceFetchHandler),
      ('/internal/queues/remove-cataloged-instance',
       CatalogedInstanceRemovalHandler),
      ('/internal/queues/resize-instance-group', InstanceGroupResizeHandler),
      ('/internal/queues/update-cataloged-instance',
       CatalogedInstanceUpdateHandler),
      ('/internal/queues/update-instance-metadata',
       InstanceMetadataUpdateHandler),
  ])

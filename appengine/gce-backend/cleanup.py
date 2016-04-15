# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Utilities for cleaning up GCE Backend."""

import logging

from google.appengine.ext import ndb

import instance_group_managers
import instance_templates
import models
import utils


@ndb.transactional_tasklet
def delete_instance_group_manager(key):
  """Attempts to delete the given InstanceGroupManager.

  Args:
    key: ndb.Key for a models.InstanceGroupManager entity.
  """
  entity = yield key.get_async()
  if not entity:
    logging.warning('InstanceGroupManager does not exist: %s', key)
    return

  if entity.url or entity.instances:
    return

  parent = yield key.parent().get_async()
  if not parent:
    logging.warning('InstanceTemplateRevision does not exist: %s', key.parent())
    return

  root = yield parent.key.parent().get_async()
  if not root:
    logging.warning('InstanceTemplate does not exist: %s', parent.key.parent())
    return

  # If the InstanceGroupManager is drained, we can delete it now.
  for i, drained_key in enumerate(parent.drained):
    if key.id() == drained_key.id():
      parent.drained.pop(i)
      yield parent.put_async()
      yield key.delete_async()
      return

  # If the InstanceGroupManager is implicitly drained, we can still delete it.
  if parent.key in root.drained:
    for i, drained_key in enumerate(parent.active):
      if key.id() == drained_key.id():
        parent.active.pop(i)
        yield parent.put_async()
        yield key.delete_async()


@ndb.transactional_tasklet
def delete_instance_template_revision(key):
  """Attempts to delete the given InstanceTemplateRevision.

  Args:
    key: ndb.Key for a models.InstanceTemplateRevision entity.
  """
  entity = yield key.get_async()
  if not entity:
    logging.warning('InstanceTemplateRevision does not exist: %s', key)
    return

  if entity.url or entity.active or entity.drained:
    return

  parent = yield key.parent().get_async()
  if not parent:
    logging.warning('InstanceTemplate does not exist: %s', key.parent())
    return

  for i, drained_key in enumerate(parent.drained):
    if key.id() == drained_key.id():
      parent.drained.pop(i)
      yield parent.put_async()
      yield key.delete_async()


@ndb.transactional_tasklet
def delete_instance_template(key):
  """Attempts to delete the given InstanceTemplate.

  Args:
    key: ndb.Key for a models.InstanceTemplate entity.
  """
  entity = yield key.get_async()
  if not entity:
    logging.warning('InstanceTemplate does not exist: %s', key)
    return

  if entity.active or entity.drained:
    return

  yield key.delete_async()


def cleanup_instance_group_managers(max_concurrent=50):
  """Deletes drained InstanceGroupManagers.

  Args:
    max_concurrent: Maximum number to delete concurrently.
  """
  utils._batch_process_async(
      instance_group_managers.get_drained_instance_group_managers(),
      delete_instance_group_manager,
      max_concurrent=max_concurrent,
  )


def cleanup_instance_template_revisions(max_concurrent=50):
  """Deletes drained InstanceTemplateRevisions.

  Args:
    max_concurrent: Maximum number to delete concurrently.
  """
  utils._batch_process_async(
      instance_templates.get_drained_instance_template_revisions(),
      delete_instance_template_revision,
      max_concurrent=max_concurrent,
  )


def cleanup_instance_templates(max_concurrent=50):
  """Deletes InstanceTemplates.

  Args:
    max_concurrent: Maximum number to delete concurrently.
  """
  utils._batch_process_async(
      models.InstanceTemplate.query().fetch(keys_only=True),
      delete_instance_template,
      max_concurrent=max_concurrent,
  )

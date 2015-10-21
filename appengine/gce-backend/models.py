# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Datastore models for the Machine Provider messages."""

import hashlib

from google.appengine.ext import ndb
from google.appengine.ext.ndb import msgprop

from components.machine_provider import rpc_messages


class Enum(frozenset):
  def __getattr__(self, attr):
    if attr in self:
      return attr
    raise AttributeError(attr)


OSFamilies = Enum(rpc_messages.OSFamily.names())


class InstanceTemplate(ndb.Model):
  """Datastore representation of a GCE instance template.

  Key:
    InstanceTemplate is a root entity.
    id: Hash of the instance template name and project.
  """
  # Initial size when newly creating the instance group manager.
  initial_size = ndb.IntegerProperty(required=True)
  # Name of the instance group manager that should be created from the template.
  instance_group_name = ndb.StringProperty(required=True)
  # Project the instance group manager should be created in.
  instance_group_project = ndb.StringProperty(required=True)
  # rpc_messages.OSFamily of this instance.
  os_family = ndb.StringProperty(choices=OSFamilies, required=True)
  # Name of the instance template.
  template_name = ndb.StringProperty(required=True)
  # Project containing the instance template.
  template_project = ndb.StringProperty(required=True)
  # Zone the instance group manager should be created in.
  zone = ndb.StringProperty(required=True)

  @classmethod
  def generate_key(cls, name, project):
    """Generates the key for an InstanceTemplate with the given name/project.

    Args:
      name: Name of this instance template.
      project: Project containing this instance template.

    Returns:
      An ndb.Key instance.
    """
    return ndb.Key(cls, hashlib.sha1('%s\0%s' % (name, project)).hexdigest())


InstanceStates = Enum([
  'CATALOGED',
  'DELETED',
  'NEW',
  'PENDING_CATALOG',
  'PENDING_DELETION',
])


class Instance(ndb.Model):
  """Structured property representing a GCE instance.

  Standalone Instance entities should not exist in the datastore.
  """
  # Name of this instance.
  name = ndb.StringProperty(required=True)
  # State of this instance.
  state = ndb.StringProperty(choices=InstanceStates, required=True)
  # URL for this instance.
  url = ndb.StringProperty(required=True)


class InstanceGroup(ndb.Model):
  """Datastore representation of a GCE instance group.

  Key:
    InstanceGroup is a root entity.
    id: Hash of the instance group name.
  """
  # rpc_messages.Dimensions describing members of this instance group.
  dimensions = msgprop.MessageProperty(rpc_messages.Dimensions, required=True)
  # Names of members of this instance group.
  members = ndb.LocalStructuredProperty(Instance, repeated=True)
  # Name of this instance group.
  name = ndb.StringProperty(required=True)
  # rpc_messages.Policies governing members of this instance group.
  policies = msgprop.MessageProperty(rpc_messages.Policies, required=True)
  # Name of the project this instance group exists in.
  project = ndb.StringProperty(required=True)
  # Zone the members of this instance group exist in. e.g. us-central1-f.
  zone = ndb.StringProperty(required=True)

  @classmethod
  def generate_key(cls, name):
    """Generates the key for an InstanceGroup with the given name.

    Args:
      name: Name of this instance group.

    Returns:
      An ndb.Key instance.
    """
    return ndb.Key(cls, hashlib.sha1(name).hexdigest())


class InstanceDeletions(ndb.Model):
  """Datastore representation of a set of instances scheduled for deletion.

  Only one instance of this entity should exist in the datastore.
  """
  ID = 'InstanceDeletions'
  # Names of instances scheduled for deletion.
  instances = ndb.StringProperty(repeated=True)

  def _pre_put_hook(self):
    super(InstanceDeletions, self)._pre_put_hook()
    self.instances.sort()

  @classmethod
  def add_instance(cls, instance_name):
    """Schedules an instance for deletion.

    Args:
      instance_name: Name of the instance to schedule for deletion.
    """
    key = cls.get_key()
    instance_deletions = key.get()
    if not instance_deletions:
      instance_deletions = InstanceDeletions(key=key, instances=[])
    instances = set(instance_deletions.instances)
    instances.add(instance_name)
    instance_deletions.instances = instances
    instance_deletions.put()

  @classmethod
  def discard_instances(cls, instance_names):
    """Discards instances scheduled for deletion.

    Args:
      instance_names: Names of the instances to discard.
    """
    instance_deletions = cls.get_key().get()
    instances = set(instance_deletions.instances) - set(instance_names)
    instance_deletions.instances = instances
    instance_deletions.put()

  @classmethod
  def get_instances(cls):
    """Gets the instances scheduled for deletion.

    Returns:
      A set of instances scheduled for deletion.
    """
    instance_deletions = cls.get_key().get()
    if not instance_deletions:
      return set()
    return set(instance_deletions.instances)

  @classmethod
  def get_key(cls):
    return ndb.Key(cls, hashlib.sha1(cls.ID).hexdigest())

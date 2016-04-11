# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Task queues for the GCE Backend."""

import json
import logging

from google.appengine.ext import ndb
import webapp2

from components import decorators

import instance_templates


class InstanceTemplateCreationHandler(webapp2.RequestHandler):
  """Worker for creating instance templates from the config."""

  @decorators.require_taskqueue('create-instance-template')
  def post(self):
    """Creates an instance templates for the given InstanceTemplateRevision.

    Params:
      key: URL-safe key for a models.InstanceTemplateRevision.
    """
    key = self.request.get('key')

    key = ndb.Key(urlsafe=key)
    entity = key.get()
    if not entity:
      logging.warning('InstanceTemplateRevision does not exist: %s', key)
      return

    if entity.url:
      logging.info(
          'Instance template exists for InstanceTemplateRevision: %s\nURL: %s',
          key,
          entity.url,
      )
      return

    instance_templates.create(key)


def create_queues_app():
  return webapp2.WSGIApplication([
      ('/internal/queues/create-instance-template',
       InstanceTemplateCreationHandler),
  ])

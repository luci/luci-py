# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""GCE Backend cron jobs."""

import json
import logging

from google.appengine.api import app_identity
import webapp2

from components import decorators
from components import gce


# TODO(smut): Make this modifiable at runtime (keep in datastore).
GCE_PROJECT_ID = app_identity.get_application_id()

# TODO(smut): Make this modifiable at runtime (keep in datastore).
# Minimum number of instances to keep in each instance group.
MIN_INSTANCE_GROUP_SIZE = 4

# TODO(smut): Support othre zones.
ZONE = 'us-central1-f'


class InstanceTemplateProcessor(webapp2.RequestHandler):
  """Worker for processing instance templates."""

  @decorators.require_cronjob
  def get(self):
    api = gce.Project(GCE_PROJECT_ID)
    logging.info('Retrieving instance templates')
    templates = api.get_instance_templates()
    logging.info('Retrieving instance groups')
    groups = api.get_instance_groups(ZONE)

    # For each template, ensure there exists a group implementing it.
    for template_name, template in templates.iteritems():
      logging.info(
          'Processing instance template:\n%s',
          json.dumps(template, indent=2),
      )
      if template_name not in groups:
        logging.info(
          'Creating instance group from instance template:\n%s',
          json.dumps(template, indent=2),
        )
        api.create_instance_group(template, MIN_INSTANCE_GROUP_SIZE, ZONE)
      else:
        logging.info(
            'Instance group already exists:\n%s',
            json.dumps(groups[template_name], indent=2),
        )


def create_cron_app():
  return webapp2.WSGIApplication([
      ('/internal/cron/process-instance-templates', InstanceTemplateProcessor),
  ])

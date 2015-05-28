# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Cron jobs for processing lease requests."""

import logging

import webapp2

from components import decorators

import models


class LeaseRequestProcessor(webapp2.RequestHandler):
  """Worker for processing lease requests."""

  @decorators.require_cronjob
  def get(self):
    for lease in models.LeaseRequest.query(
        models.LeaseRequest.state == models.LeaseRequestStates.UNTRIAGED
    ):
      # TODO: Actually process lease requests.
      logging.info('Not really processing untriaged LeaseRequest:\n%s', lease)


def create_backend_app():
  return webapp2.WSGIApplication([
      ('/internal/cron/process-lease-requests', LeaseRequestProcessor),
  ])

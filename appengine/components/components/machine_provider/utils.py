# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Helper functions for working with the Machine Provider."""

import logging

from google.appengine.ext import ndb

from components import net
from components import utils
from components.datastore_utils import config


MACHINE_PROVIDER_SCOPES = (
    'https://www.googleapis.com/auth/userinfo.email',
)


class MachineProviderConfiguration(config.GlobalConfig):
  """Configuration for talking to the Machine Provider."""
  # URL of the Machine Provider instance to use.
  instance_url = ndb.StringProperty(required=True)

  @classmethod
  def get_instance_url(cls):
    """Returns the URL of the Machine Provider instance."""
    return cls.cached().instance_url

  def set_defaults(self):
    """Sets default values used to initialize the config."""
    self.instance_url = 'https://machine-provider.appspot.com'


def add_machines(requests):
  """Add machines to the Machine Provider's Catalog.

  Args:
    requests: A list of rpc_messages.CatalogMachineAdditionRequest instances.
  """
  logging.info('Sending batched add_machines request')
  return net.json_request(
      '%s/_ah/api/catalog/v1/add_machines' %
          MachineProviderConfiguration.get_instance_url(),
      method='POST',
      payload=utils.to_json_encodable({'requests': requests}),
      scopes=MACHINE_PROVIDER_SCOPES,
 )


def lease_machines(requests):
  """Lease machines from the Machine Provider.

  Args:
    requests: A list of rpc_messages.LeaseRequest instances.
  """
  logging.info('Sending batched lease_machines request')
  return net.json_request(
      '%s/_ah/api/machine_provider/v1/batched_lease' %
          MachineProviderConfiguration.get_instance_url(),
      method='POST',
      payload=utils.to_json_encodable({'requests': requests}),
      scopes=MACHINE_PROVIDER_SCOPES,
  )

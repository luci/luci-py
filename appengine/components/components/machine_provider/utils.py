# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Helper functions for working with the Machine Provider."""

import logging

from components import net
from components import utils


MACHINE_PROVIDER_API_URL = 'https://machine-provider.appspot.com/_ah/api'

CATALOG_BASE_URL = '%s/catalog/v1' % MACHINE_PROVIDER_API_URL
MACHINE_PROVIDER_BASE_URL = '%s/machine_provider/v1' % MACHINE_PROVIDER_API_URL
MACHINE_PROVIDER_SCOPES = (
    'https://www.googleapis.com/auth/userinfo.email',
)


def add_machines(requests):
  """Add machines to the Machine Provider's Catalog.

  Args:
    requests: A list of rpc_messages.CatalogMachineAdditionRequest instances.
  """
  logging.info('Sending batched add_machines request')
  return net.json_request(
      '%s/add_machines' % CATALOG_BASE_URL,
      method='POST',
      payload=utils.to_json_encodable({'requests': requests}),
      scopes=MACHINE_PROVIDER_SCOPES,
  )

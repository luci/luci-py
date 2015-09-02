# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Helper functions for working with the Machine Provider."""

import logging

from components import net


MACHINE_PROVIDER_API_URL = 'https://machine-provider.appspot.com/_ah/api'

CATALOG_BASE_URL = '%s/catalog/v1' % MACHINE_PROVIDER_API_URL
MACHINE_PROVIDER_BASE_URL = '%s/machine_provider/v1' % MACHINE_PROVIDER_API_URL
MACHINE_PROVIDER_SCOPES = (
    'https://www.googleapis.com/auth/userinfo.email',
)


def add_machines(dimensions_list):
  """Add machines to the Machine Provider's Catalog.

  Args:
    dimensions_list: A list of dimensions.Dimensions instances describing the
      dimensions of the machines to add to the catalog.
  """
  logging.info('Sending batched add_machines request')
  return net.json_request(
      '%s/add_machines' % CATALOG_BASE_URL,
      method='POST',
      payload={
          'requests': [
              {'dimensions': dimensions} for dimensions in dimensions_list
          ],
      },
      scopes=MACHINE_PROVIDER_SCOPES,
  )

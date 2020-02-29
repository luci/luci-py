# Copyright 2020 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Loading and interpretation of realms.cfg config files."""

# Register the config validation hook.
from realms import validation
validation.register()


def refetch_config():
  """Called periodically in a cron job to reread and apply all realms.cfg."""
  # TODO(vadimsh): Implement.

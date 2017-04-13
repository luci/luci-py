# Copyright 2017 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Ambient task queues generated from the actual load.

This means that the task queues are deduced by the actual load, they are never
explicitly defined. They 'disapear' once the load stops, that is, no task with
the exact set of dimensions is triggered anymore.

Used to optimize scheduling.
"""

### Models.


### Private APIs.


### Public APIs.


def assert_bot(bot_dimensions):
  """Prepares the dimensions for the queues."""
  assert len(bot_dimensions[u'id']) == 1, bot_dimensions


def assert_task(request):
  """Prepares the dimensions for the queues.

  The generated entities are root entities.
  """
  del request

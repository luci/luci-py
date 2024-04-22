# Copyright 2024 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.
"""Functionality for changelog generation to be handled by Auth Service v2.

See
https://chromium.googlesource.com/infra/luci/luci-go/+/refs/heads/main/auth_service/
"""

from google.protobuf import json_format

from components import utils

from components.auth.proto import tasks_pb2


class ChangelogGenerationTriggerError(Exception):
  """Failed to enqueue a changelog generation task."""


def enqueue_v2_changelog_task(auth_db_rev):
  """Enqueues a task to Auth Service v2's 'changelog-generation' queue.

  Can be configured as the custom callback in appengine_config.py, to be called
  by the enqueue_process_change_task function in
  components/components/auth/change_log.py.

  Raises:
    ChangelogGenerationTriggerError if enqueuing failed.
  """
  payload = {
      'class':
      'process-change-task',
      'body':
      json_format.MessageToDict(
          tasks_pb2.ProcessChangeTask(auth_db_rev=auth_db_rev)),
  }
  ok = utils.enqueue_task(
      # The last path component is informational for nicer logs. All data
      # is transferred through `payload`.
      url='/internal/tasks/t/changelog-generation/%d' % auth_db_rev,
      queue_name='changelog-generation',
      transactional=True,
      payload=utils.encode_to_json(payload))
  if not ok:
    raise ChangelogGenerationTriggerError(
        'failed to enqueue v2 changelog task for rev %d' % auth_db_rev)

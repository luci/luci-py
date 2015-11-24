# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""GCE Backend instance state machine."""

import collections

import models


fields = [
    # Function which expects an instance of models.Instance and returns a
    # JSON-encodable to pass to the task queue that processes instances this
    # StateTransition is concerned with.
    'accumulator',
    # Name of the task queue which should process instances this
    # StateTransition is concerned with.
    'taskqueue',
    # URL of the task queue handler which should process instances this
    # StateTransition is concerned with.
    'url',
    # One of models.InstanceStates. The state to flip instances to if
    # enqueuing the task to process them succeeds.
    'success_state',
    # One of models.InstanceStates. The state to flip instances to if
    # enqueuing the task to process them fails.
    'failure_state',
]


StateTransition = collections.namedtuple('StateTransition', fields)


STATE_MACHINE = {
  models.InstanceStates.NEW: StateTransition(
      lambda instance: None,
      'prepare-instances',
      '/internal/queues/prepare-instances',
      models.InstanceStates.PREPARING,
      models.InstanceStates.NEW,
  ),
  models.InstanceStates.PENDING_CATALOG: StateTransition(
      lambda instance: instance.pubsub_service_account,
      'catalog-instance-group',
      '/internal/queues/catalog-instance-group',
      models.InstanceStates.CATALOGED,
      models.InstanceStates.PENDING_CATALOG,
  ),
  models.InstanceStates.PENDING_DELETION: StateTransition(
      lambda instance: instance.url,
      'delete-instances',
      '/internal/queues/delete-instances',
      models.InstanceStates.DELETING,
      models.InstanceStates.PENDING_DELETION,
  ),
  models.InstanceStates.PENDING_METADATA_OPERATION: StateTransition(
      lambda instance: instance.metadata_operation,
      'check-metadata-operation',
      '/internal/queues/check-metadata-operation',
      models.InstanceStates.CHECKING_METADATA,
      models.InstanceStates.PENDING_METADATA_OPERATION,
  ),
  models.InstanceStates.PENDING_METADATA_UPDATE: StateTransition(
      lambda instance: {
          'pubsub_service_account': instance.pubsub_service_account,
          'pubsub_subscription': instance.pubsub_subscription,
          'pubsub_subscription_project': instance.pubsub_subscription_project,
          'pubsub_topic': instance.pubsub_topic,
          'pubsub_topic_project': instance.pubsub_topic_project,
      },
      'update-instance-metadata',
      '/internal/queues/update-instance-metadata',
      models.InstanceStates.UPDATING_METADATA,
      models.InstanceStates.PENDING_METADATA_UPDATE,
  ),
}

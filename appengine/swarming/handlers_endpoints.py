# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""This module defines Swarming Server endpoints handlers."""

import endpoints
from protorpc import message_types
from protorpc import messages
from protorpc import remote

from components import auth


### Request Types


class ClientResult(messages.Message):
  task_id = messages.StringField(1, required=True)


### Response Types


class TaskPropertyDefinition(messages.Message):
  """Defines all the properties of a task to be run on the Swarming
  infrastructure.

  This entity is not saved in the DB as a standalone entity, instead it is
  embedded in a TaskRequest.

  This model is immutable.
  """
  # Hashing algorithm used to hash TaskProperties to create its key.
  HASHING_ALGO = hashlib.sha1

  # Commands to run. It is a list of lists. Each command is run one after the
  # other. Encoded json.
  commands = datastore_utils.DeterministicJsonProperty(
      validator=_validate_command, json_type=list, required=True)

  # List of (URLs, local file) for the bot to download. Encoded as json. Must be
  # sorted by URLs. Optional.
  data = datastore_utils.DeterministicJsonProperty(
      validator=_validate_data, json_type=list)

  # Filter to use to determine the required properties on the bot to run on. For
  # example, Windows or hostname. Encoded as json. Optional but highly
  # recommended.
  dimensions = datastore_utils.DeterministicJsonProperty(
      validator=_validate_dict_of_strings, json_type=dict)

  # Environment variables. Encoded as json. Optional.
  env = datastore_utils.DeterministicJsonProperty(
      validator=_validate_dict_of_strings, json_type=dict)

  execution_timeout_secs = messages.IntegerField(required=True)
  grace_period_secs = messages.IntegerField(default=30)
  io_timeout_secs = messages.IntegerField()
  idempotent = messages.BooleanField(default=False)


class TaskRequestEntity(messages.Message):
  """Representation of the TaskRequest ndb model."""

  # TODO(cmassaro): the alternative is just to contain a single StringField
  #   which embeds the JSON representation of the TaskRequest
  created_ts = messages.DateTimeField(1, required=True)
  name = messages.StringField(2, required=True)
  authenticated = messages.StringField(3)
  # TODO(cmassaro): subclass BytesField to include analogous DB model's
  #   complexity?
  properties = messages.MessageField(TaskProperty, 4)
  expiration_ts = messages.DateTimeField(5, required=True)
  tags = messages.StringField(6, repeated=True)
  parent_task_id = messages.StringField(7)


class ResultEntity(messages.Message):
  pass


### API


@auth.endpoints_api(name='swarmingservice', version='v1')
class SwarmingService(remote.Service):

  @auth.endpoints_method(ClientResult, ResultEntity)
  def client_task_result(self, request):
    """Return the task result corresponding to a task ID."""
    result = cls.get_result_entity(request.task_id)

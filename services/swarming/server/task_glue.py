# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Translation layer between the old API and the new one.

Implements the public API of test_manager.py, test_request.py and
test_runner.py used in handlers.py.

Plan of attack:
- Create a translation layer to keep mostly the same API for test_management.py
  vs task_scheduler.py. It won't be 100% compatible but should be close enough
  to make the switch over easy. In the meantime, it'll be quite cheezy since the
  paradigms do not match exactly.
- Convert swarm_bot to use the new versioned API.
- Make sure all bots are deployed with new bot API.
- Delete old bot code.
- Create new client API.
- Deploy servers.
- Switch client code to use new API.
- Roll client code into chromium.
- Wait 1 month.
- Remove old client code API.

The goal is to make tests/server_smoke_test.py pass, not handlers_test.py, since
handlers_test.py edits the DB directly.

Entities translation is:
  TestRequest -> TaskRequest
  TestRunner -> TaskShardResult
The translation is not 1:1 and has patches accordingly until we get rid of the
old ones.
"""

import logging
import os

from google.appengine.datastore import datastore_query
from google.appengine.ext import ndb

from google.appengine.datastore import datastore_query
from google.appengine.ext import ndb

from common import rpc
from common import test_request_message
from server import stats_new as stats
from server import task_request
from server import task_result
from server import task_shard_to_run
from server import task_scheduler
from server import test_management
from server import test_runner
from stats import machine_stats


# Temporary, as the API is refactored while the old code is being deleted.
# pylint: disable=W0613


def _convert_test_case(data):
  """Constructs a TaskProperties out of a test_request_message.TestCase.

  This code is kept for compatibility with the previous API. See new_request()
  for more details.
  """
  test_case = test_request_message.TestCase.FromJSON(data)
  # TODO(maruel): Add missing mapping and delete obsolete ones.
  assert len(test_case.configurations) == 1, test_case.configurations
  config = test_case.configurations[0]

  if test_case.tests:
    execution_timeout_secs = int(round(test_case.tests[0].hard_time_out))
    io_timeout_secs = int(round(test_case.tests[0].io_time_out))
  else:
    execution_timeout_secs = 2*60*60
    io_timeout_secs = 60*60

  # Ignore all the settings that are deprecated.
  return {
    'name': test_case.test_case_name,
    'user': test_case.requestor,
    'properties': {
      'commands': [c.action for c in test_case.tests],
      'data': test_case.data,
      'dimensions': config.dimensions,
      'env': test_case.env_vars,
      'number_shards': config.num_instances,
      'execution_timeout_secs': execution_timeout_secs,
      'io_timeout_secs': io_timeout_secs,
    },
    'priority': config.priority,
    'scheduling_expiration_secs': config.deadline_to_run,
  }


### test_manager.py public API.


def AbortRunner(runner_key_urlsafe, reason):
  try:
    shard_result_key = task_scheduler.unpack_shard_result_key(
        runner_key_urlsafe)
  except ValueError:
    return False
  return task_shard_to_run.abort_shard_to_run(shard_result_key.parent().get())


def AbortStaleRunners():
  # TODO(maruel): Not relevant on new API. The cron job is different.
  return True


def RegisterNewRequest(test_case):
  request_properties = _convert_test_case(test_case)
  request, shard_runs = task_scheduler.new_request(request_properties)

  def to_packed_key(i):
    return task_scheduler.pack_shard_result_key(
        task_result.shard_to_run_key_to_shard_result_key(
            shard_runs[i].key, 1))

  return {
    'test_case_name': request.name,
    'test_keys': [
      {
        'config_name': request.name,
        'instance_index': shard_index,
        'num_instances': request.properties.number_shards,
        'test_key': to_packed_key(shard_index),
      }
      for shard_index in xrange(request.properties.number_shards)
    ],
  }


def RequestWorkItem(attributes, server_url):
  attribs = test_management.ValidateAndFixAttributes(attributes)
  response = test_management.CheckVersion(attributes, server_url)
  if response:
    return response

  dimensions = attribs['dimensions']
  bot_id = attribs['id'] or dimensions['hostname']
  stats.add_entry(action='bot_active', bot_id=bot_id, dimensions=dimensions)
  request, shard_result = task_scheduler.bot_reap_task(dimensions, bot_id)
  if not request:
    try_count = attribs['try_count'] + 1
    return {
      'come_back': task_scheduler.exponential_backoff(try_count),
      'try_count': try_count,
    }

  runner_key = task_scheduler.pack_shard_result_key(shard_result.key)
  test_objects = [
    test_request_message.TestObject(
        test_name=str(i),
        action=command,
        hard_time_out=request.properties.execution_timeout_secs,
        io_time_out=request.properties.io_timeout_secs)
    for i, command in enumerate(request.properties.commands)
  ]
  # pylint: disable=W0212
  test_run = test_request_message.TestRun(
      test_run_name=request.name,
      env_vars=request.properties.env,
      instance_index=shard_result.shard_number,
      num_instances=request.properties.number_shards,
      configuration=test_request_message.TestConfiguration(
          config_name=request.name,
          num_instances=request.properties.number_shards),
      result_url=('%s/result?r=%s&id=%s' % (server_url,
                                            runner_key,
                                            shard_result.bot_id)),
      ping_url=('%s/runner_ping?r=%s&id=%s' % (server_url,
                                              runner_key,
                                              shard_result.bot_id)),
      ping_delay=(test_management._TIMEOUT_FACTOR /
          test_management._MISSED_PINGS_BEFORE_TIMEOUT),
      data=request.properties.data,
      tests=test_objects)
  test_run.ExpandVariables({
      'instance_index': shard_result.shard_number,
      'num_instances': request.properties.number_shards,
  })
  test_run.Validate()

  # See test_management._GetTestRunnerCommands() for the expected format.
  files_to_upload = [
      (test_run.working_dir or '', test_management._TEST_RUN_SWARM_FILE_NAME,
      test_request_message.Stringize(test_run, json_readable=True))
  ]

  # Define how to run the scripts.
  swarm_file_path = test_management._TEST_RUN_SWARM_FILE_NAME
  if test_run.working_dir:
    swarm_file_path = os.path.join(
        test_run.working_dir, test_management._TEST_RUN_SWARM_FILE_NAME)
  command_to_execute = [
      os.path.join('local_test_runner.py'),
      '-f', swarm_file_path,
  ]

  # TODO(maruel): Decide at what level we are going to support these flags.
  # Punted for the next CL. :)
  #test_case = runner.request.get().GetTestCase()
  #if test_case.verbose:
  #  command_to_execute.append('-v')
  #if test_case.restart_on_failure:
  #  command_to_execute.append('--restart_on_failure')

  # TODO(maruel): Always on?
  command_to_execute.append('--restart_on_failure')
  # TODO(maruel): Always off?
  #command_to_execute.append('-v')

  rpc_commands = [
    rpc.BuildRPC('StoreFiles', files_to_upload),
    rpc.BuildRPC('RunCommands', command_to_execute),
  ]
  # The Swarming bot uses an hand rolled RPC system and 'commands' is actual the
  # custom RPC commands. See test_management._BuildTestRun()
  return {
    'commands': rpc_commands,
    'result_url': test_run.result_url,
    'try_count': 0,
  }


def RetryRunner(runner_key_urlsafe):
  # Do not 'retry' the original request, duplicate the request, only changing
  # the ownership to the user that requested the retry. TaskProperties is
  # unchanged.
  try:
    shard_result_key = task_scheduler.unpack_shard_result_key(
        runner_key_urlsafe)
  except ValueError:
    return False
  request = task_result.shard_result_key_to_request_key(shard_result_key).get()
  # TODO(maruel): Decide if it will be supported, and if so create a proper
  # function to 'duplicate' a TaskRequest in task_request.py.
  # TODO(maruel): Delete.
  data = {
    'name': request.name,
    'user': request.user,
    'properties': {
      'commands': request.properties.commands,
      'data': request.properties.data,
      'dimensions': request.properties.dimensions,
      'env': request.properties.env,
      'number_shards': request.properties.number_shards,
      'execution_timeout_secs': request.properties.execution_timeout_secs,
      'io_timeout_secs': request.properties.io_timeout_secs,
    },
    'priority': request.priority,
    'scheduling_expiration_secs': request.scheduling_expiration_secs,
  }
  task_scheduler.new_request(data)
  return True



### test_request.py public API.


def UrlSafe(runner_key):
  """Returns an urlsafe encoded key. What it is depends on old vs new."""
  return task_scheduler.pack_shard_result_key(runner_key)


def GetAllMatchingTestCases(test_case_name):
  """Returns a list of TestRequest or TaskRequest."""
  # Find the matching TaskRequest, then return all the valid keys.
  q = task_request.TaskRequest.query().filter(
      task_request.TaskRequest.name == test_case_name)
  out = []
  for request in q:
    # Then return all the relevant shard_ids.
    # TODO(maruel): It's hacked up. This needs to fetch TaskResultSummary to get
    # the try_numbers.
    try_number = 1
    out.extend(
        '%x-%s' % (request.key.integer_id()+i+1, try_number)
        for i in xrange(request.properties.number_shards))
  return out


### test_runner.py public API.


ACCEPTABLE_SORTS =  {
  'created_ts': 'Created',
  'done_ts': 'Ended',
  'modified_ts': 'Last updated',
  'name': 'Name',
  'user': 'User',
}
DEFAULT_SORT = 'created_ts'

# These sort are done on the TaskRequest.
SORT_REQUEST = frozenset(('created_ts', 'name', 'user'))

# These sort are done on the TaskResultSummary.
SORT_RESULT = frozenset(('done_ts', 'modified_ts'))

assert SORT_REQUEST | SORT_RESULT == frozenset(ACCEPTABLE_SORTS)


TIME_BEFORE_RUNNER_HANGING_IN_MINS = (
    test_runner.TIME_BEFORE_RUNNER_HANGING_IN_MINS)


def GetRunnerFromUrlSafeKey(runner_key):
  """Returns a TaskShardResult."""
  shard_result_key = task_scheduler.unpack_shard_result_key(runner_key)
  return shard_result_key.get()


def GetTestRunnersWithFilters(
    sort_by, ascending, limit, offset, status, show_successfully_completed,
    test_name_filter, machine_id_filter):
  """Returns a ndb.Future that will return the items in the DB."""
  assert sort_by in ACCEPTABLE_SORTS, (
      'This should have been validated at a higher level')
  opts = ndb.QueryOptions(limit=limit, offset=offset)
  direction = (
      datastore_query.PropertyOrder.ASCENDING
      if ascending else datastore_query.PropertyOrder.DESCENDING)

  fetch_request = bool(sort_by in SORT_REQUEST)
  if fetch_request:
    base_type = task_request.TaskRequest
  else:
    base_type = task_result.TaskResultSummary

  # The future returned to the user:
  final = ndb.Future()

  # Phase 1: initiates the fetch.
  initial_future = base_type.query(default_options=opts).order(
      datastore_query.PropertyOrder(sort_by, direction)).fetch_async()

  def phase2_fetch_complement(future):
    """Initiates the async fetch for the other object.

    If base_type is TaskRequest, it will fetch the corresponding
    TaskResultSummary.
    If base_type is TaskResultSummary, it will fetch the corresponding
    TaskRequest.
    """
    entities = future.get_result()
    if not entities:
      # Skip through if no entity was returned by the Query.
      final.set_result(entities)
      return

    if fetch_request:
      keys = (
        task_result.request_key_to_result_summary_key(i.key) for i in entities
      )
    else:
      keys = (i.request_key for i in entities)

    # Convert a list of Future into a MultiFuture.
    second_future = ndb.MultiFuture()
    map(second_future.add_dependent, ndb.get_multi_async(keys))
    second_future.complete()
    second_future.add_immediate_callback(
        phase3_merge_complementatry_models, entities, second_future)

  def phase3_merge_complementatry_models(entities, future):
    """Merge the results together to yield (TaskRequest, TaskResultSummary)."""
    # See make_runner_view() for the real conversion to simulate the old DB
    # entities.
    if fetch_request:
      out = zip(entities, future.get_result())
    else:
      # Reverse the order.
      out = zip(future.get_result(), entities)
    # Filter out inconsistent items.
    final.set_result([i for i in out if i[0] and i[1]])

  # ndb.Future.add_immediate_callback() adds a callback that is immediately
  # called when self.set_result() is called.
  initial_future.add_immediate_callback(phase2_fetch_complement, initial_future)
  return final


def CountRequestsAsync():
  """Returns a ndb.Future that will return the number of requests in the DB."""
  return task_request.TaskRequest.query().count_async()


def PingRunner(runner_key, machine_id):
  try:
    shard_result_key = task_scheduler.unpack_shard_result_key(runner_key)
    return task_scheduler.bot_update_task(shard_result_key, {}, machine_id)
  except ValueError as e:
    logging.error('Failed to accept value %s: %s', runner_key, e)
    return False


def GetHangingRunners():
  # Not applicable.
  return []


def DeleteRunnerFromKey(_key):
  # TODO(maruel): Remove this API.
  return True


def GetRunnerResults(runner_key_urlsafe):
  try:
    shard_result = GetRunnerFromUrlSafeKey(runner_key_urlsafe)
  except ValueError:
    return None
  if not shard_result:
    # TODO(maruel): Implement in next CL.
    return {
      'exit_codes': '',
      'machine_id': None,
      'machine_tag': None,
      'output': '',
    }
  return {
    'exit_codes': ','.join(map(str, shard_result.exit_codes)),
    'machine_id': shard_result.bot_id,
    # TODO(maruel): Likely unnecessary.
    'machine_tag': machine_stats.GetMachineTag(shard_result.bot_id),
    'config_instance_index': shard_result.shard_number,
    'num_config_instances':
        shard_result.request_key.get().properties.number_shards,
    # TODO(maruel): Return each output independently. Figure out a way to
    # describe what is important in the steps and what should be ditched.
    'output': '\n'.join(i.get().GetResults() for i in shard_result.outputs),
  }


def UpdateTestResult(
    runner, machine_id, success, exit_codes, results, overwrite):
  assert not overwrite
  if runner.bot_id != machine_id:
    raise ValueError('Expected %s, got %s', runner.bot_id, machine_id)

  # Ignore the argument 'success'.
  exit_codes = filter(None, (i.strip() for i in exit_codes.split(',')))
  data = {
    'exit_codes': map(int, exit_codes),
    # TODO(maruel): Store output for each command individually.
    'outputs': [results.key],
  }
  task_scheduler.bot_update_task(runner.key, data, machine_id)
  return True


def GetEncoding(runner):
  return 'utf-8'


### UI code that should be in jinja2 templates


def GenerateButtonWithHiddenForm(button_text, url, form_id):
  """Generate a button that when used will post to the given url.

  Args:
    button_text: The text to display on the button.
    url: The url to post to.
    form_id: The id to give the form.

  Returns:
    The html text to display the button.
  """
  button_html = '<form id="%s" method="post" action=%s>' % (form_id, url)
  button_html += (
      '<button onclick="document.getElementById(%s).submit()">%s</button>' %
      (form_id, button_text))
  button_html += '</form>'

  return button_html


def make_runner_view(runner):
  """Returns a html template friendly dict from a TestRunner."""
  # TODO(maruel): This belongs to a jinja2 template, not here.
  # Simulate the old properties until the templates are updated.
  request, result = runner
  assert isinstance(request, task_request.TaskRequest), request
  assert isinstance(result, task_result.TaskResultSummary), result
  started = [i.started_ts for i in result.shards if i.started_ts]
  out = {
    # TODO(maruel): out['class_string'] = 'failed_test'
    'class_string': '',
    'command_string': 'TODO',
    'created': request.created_ts,
    'ended': result.done_ts,
    'key_string': '%x' % request.key.integer_id(),
    # TODO(maruel):
    # 'machine_id': ','.join(i.bot_id for i in runner.shards if i.bot_id),
    'machine_id': 'TODO',
    'name': request.name,
    'started': min(started) if started else None,
    'status_string': result.to_string(),
    'user': request.user,
  }
  return out

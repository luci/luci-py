# coding: utf-8
# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.
"""Base class for handlers_*_test.py"""

import base64
import json
import os

import mock

import swarming_test_env
swarming_test_env.setup_test_env()

from google.appengine.api import app_identity
from google.appengine.ext import ndb

from google.protobuf import json_format
from protorpc.remote import protojson
import webtest

import handlers_endpoints
import swarming_rpcs
from components import auth
from components import auth_testing
from components import utils
from components.auth import api as auth_api
from components.auth import model as auth_model
from components.auth import realms as auth_realms
from components.auth.proto import replication_pb2
import gae_ts_mon
from test_support import test_case

from proto.api import plugin_pb2
from proto.config import config_pb2
from proto.config import realms_pb2
from server import config
from server import external_scheduler
from server import large
from server import pools_config
from server import realms
from server import service_accounts
from server import task_queues

# Realm permissions used in Swarming.
_ALL_PERMS = [
    realms.get_permission(p) for p in realms_pb2.RealmPermission.values()[1:]
]


class AppTestBase(test_case.TestCase):
  APP_DIR = swarming_test_env.APP_DIR

  def setUp(self):
    super(AppTestBase, self).setUp()
    self.bot_version = None
    self.source_ip = '192.168.2.2'
    self.testbed.init_user_stub()

    gae_ts_mon.reset_for_unittest(disable=True)

    # By default requests in tests are coming from bot with fake IP.
    # WSGI app that implements auth REST API.
    self.auth_app = webtest.TestApp(
        auth.create_wsgi_application(debug=True),
        extra_environ={
            'REMOTE_ADDR': self.source_ip,
            'SERVER_SOFTWARE': os.environ['SERVER_SOFTWARE'],
        })

    admins_group = 'test_admins_group'
    priv_users_group = 'test_priv_users_group'
    users_group = 'test_users_group'

    cfg = config_pb2.SettingsCfg(
        auth=config_pb2.AuthSettings(
            admins_group=admins_group,
            privileged_users_group=priv_users_group,
            users_group=users_group,
        ),
        resultdb=config_pb2.ResultDBSettings(
            server='https://test-resultdb-server.com'),
        cas=config_pb2.CASSettings(
            viewer_server='https://test-cas-viewer-server.com'),
        cipd=config_pb2.CipdSettings(
            default_server='https://test-chrome-infra-packages.appspot.com',
            default_client_package=config_pb2.CipdPackage(
                package_name='chicken/cipd/${platform}', version='latest')))
    self.mock(config, '_get_settings', lambda: ('test_rev', cfg))
    self.mock(
        app_identity,
        'get_default_version_hostname', lambda: 'test-swarming.appspot.com')
    utils.clear_cache(config.settings)

    # Note that auth.ADMIN_GROUP != admins_group.
    auth.bootstrap_group(
        auth.ADMIN_GROUP,
        [auth.Identity(auth.IDENTITY_USER, 'super-admin@example.com')])
    auth.bootstrap_group(
        admins_group, [auth.Identity(auth.IDENTITY_USER, 'admin@example.com')])
    auth.bootstrap_group(
        priv_users_group,
        [auth.Identity(auth.IDENTITY_USER, 'priv@example.com')])
    auth.bootstrap_group(
        users_group, [auth.Identity(auth.IDENTITY_USER, 'user@example.com')])

  def mock_tq_tasks(self):
    # Help to route TQ tasks to their implementations.
    self.mock(utils, 'enqueue_task', self._enqueue_mock)
    self.mock(utils, 'enqueue_task_async', self._enqueue_mock_async)

  def _enqueue_mock(self, _url, queue_name, **kwargs):
    if queue_name == 'es-notify-tasks':
      es_host = kwargs['params']['es_host']
      proto = plugin_pb2.NotifyTasksRequest()
      json_format.Parse(kwargs['params']['request_json'], proto)
      return external_scheduler.notify_request_now(es_host, proto)
    self.assertIn(queue_name,
                  ('buildbucket-notify', 'cancel-children-tasks', 'pubsub',
                   'monitoring-bq-tasks-results-run',
                   'monitoring-bq-tasks-results-summary',
                   'monitoring-bq-bots-events', 'monitoring-bq-tasks-requests'))
    return True

  @ndb.tasklet
  def _enqueue_mock_async(self, url, queue_name, payload, transactional=False):
    if queue_name == 'rescan-matching-task-sets':
      self.assertTrue(transactional)
      self.assertTrue(ndb.in_transaction())

      # Need to execute it after the transaction lands.
      @ndb.non_transactional
      def exec_tq():
        task_queues.rescan_matching_task_sets_async(payload).get_result()

      ndb.get_context().call_on_commit(exec_tq)
      raise ndb.Return(True)

    if queue_name == 'update-bot-matches':
      self.assertTrue(transactional)
      self.assertTrue(ndb.in_transaction())

      # Need to execute it after the transaction lands.
      @ndb.non_transactional
      def exec_tq():
        task_queues.update_bot_matches_async(payload).get_result()

      ndb.get_context().call_on_commit(exec_tq)
      raise ndb.Return(True)

    self.fail(url)

  def set_as_anonymous(self):
    """Removes all IPs from the whitelist."""
    self.testbed.setup_env(USER_EMAIL='', overwrite=True)
    auth.ip_whitelist_key(auth.bots_ip_whitelist()).delete()
    auth_testing.reset_local_state()
    auth_testing.mock_get_current_identity(self, auth.Anonymous)

  def set_as_super_admin(self):
    self.set_as_anonymous()
    self.testbed.setup_env(USER_EMAIL='super-admin@example.com', overwrite=True)
    auth_testing.reset_local_state()
    auth_testing.mock_get_current_identity(
        self, auth.Identity.from_bytes('user:' + os.environ['USER_EMAIL']))

  def set_as_admin(self):
    self.set_as_anonymous()
    self.testbed.setup_env(USER_EMAIL='admin@example.com', overwrite=True)
    auth_testing.reset_local_state()
    auth_testing.mock_get_current_identity(
        self, auth.Identity.from_bytes('user:' + os.environ['USER_EMAIL']))

  def set_as_privileged_user(self):
    self.set_as_anonymous()
    self.testbed.setup_env(USER_EMAIL='priv@example.com', overwrite=True)
    auth_testing.reset_local_state()
    auth_testing.mock_get_current_identity(
        self, auth.Identity.from_bytes('user:' + os.environ['USER_EMAIL']))

  def set_as_user(self):
    self.set_as_anonymous()
    self.testbed.setup_env(USER_EMAIL='user@example.com', overwrite=True)
    auth_testing.reset_local_state()
    auth_testing.mock_get_current_identity(
        self, auth.Identity.from_bytes('user:' + os.environ['USER_EMAIL']))

  def set_as_bot(self):
    self.set_as_anonymous()
    auth.bootstrap_ip_whitelist(auth.bots_ip_whitelist(), [self.source_ip])
    auth_testing.reset_local_state()
    auth_testing.mock_get_current_identity(self, auth.IP_WHITELISTED_BOT_ID)

  # Web or generic

  def get_xsrf_token(self):
    """Gets the generic XSRF token for web clients."""
    resp = self.auth_app.post(
        '/auth/api/v1/accounts/self/xsrf_token',
        headers={
            'X-XSRF-Token-Request': '1'
        }).json
    return resp['xsrf_token'].encode('ascii')

  def post_json(self, url, params, **kwargs):
    """Does an HTTP POST with a JSON API and return JSON response."""
    return self.app.post_json(url, params=params, **kwargs).json

  # pylint: disable=redefined-outer-name
  def mock_default_pool_acl(self,
                            service_accounts,
                            default_task_realm=None,
                            enforced_realm_permissions=None):
    """Mocks ACLs of 'default' pool to allow usage of given service accounts."""
    assert isinstance(service_accounts, (list, tuple)), service_accounts

    def mocked_fetch_pools_config():
      default_cipd = pools_config.CipdServer(
          server='https://pool.config.cipd.example.com',
          package_name='cipd-client-pkg',
          client_version='from_pool_config',
      )
      return pools_config._PoolsCfg(
          {
              "template":
                  pools_config.init_pool_config(
                      name='template',
                      rev='pools_cfg_rev',
                      scheduling_users=frozenset([
                          # See setUp above. We just duplicate the first ACL
                          # layer here
                          auth.Identity(auth.IDENTITY_USER,
                                        'super-admin@example.com'),
                          auth.Identity(auth.IDENTITY_USER,
                                        'admin@example.com'),
                          auth.Identity(auth.IDENTITY_USER, 'priv@example.com'),
                          auth.Identity(auth.IDENTITY_USER, 'user@example.com'),
                      ]),
                      realm='test:pool/default',
                      default_task_realm=default_task_realm,
                      enforced_realm_permissions=enforced_realm_permissions or
                      {},
                      task_template_deployment=pools_config
                      .TaskTemplateDeployment(
                          prod=pools_config.TaskTemplate(
                              cache=(),
                              cipd_package=(pools_config.CipdPackage(
                                  '.', 'some-pkg', 'prod-version'),),
                              env=(pools_config.Env('VAR', 'prod', (), False),),
                              inclusions=()),
                          canary=pools_config.TaskTemplate(
                              cache=(),
                              cipd_package=(pools_config.CipdPackage(
                                  '.', 'some-pkg', 'canary-version'),),
                              env=(pools_config.Env('VAR', 'canary',
                                                    (), False),),
                              inclusions=()),
                          canary_chance=0.5,
                      ),
                      default_cipd=default_cipd,
                  ),
              "default":
                  pools_config.init_pool_config(
                      name='default',
                      rev='pools_cfg_rev',
                      scheduling_users=frozenset([
                          # See setUp above. We just duplicate the first ACL
                          # layer here
                          auth.Identity(auth.IDENTITY_USER,
                                        'super-admin@example.com'),
                          auth.Identity(auth.IDENTITY_USER,
                                        'admin@example.com'),
                          auth.Identity(auth.IDENTITY_USER, 'priv@example.com'),
                          auth.Identity(auth.IDENTITY_USER, 'user@example.com'),
                      ]),
                      realm='test:pool/default',
                      default_task_realm=default_task_realm,
                      enforced_realm_permissions=enforced_realm_permissions or
                      {},
                      default_cipd=default_cipd,
                  ),
          },
          (default_cipd))

    self.mock(pools_config, '_fetch_pools_config', mocked_fetch_pools_config)

  def mock_auth_db(self, permissions):
    cache_mock = mock.Mock(auth_db=self.auth_db(permissions))
    self.mock(auth_api, 'get_request_cache', lambda: cache_mock)

  @staticmethod
  def auth_db(permissions):
    return auth_api.AuthDB.from_proto(
        replication_state=auth_model.AuthReplicationState(),
        auth_db=replication_pb2.AuthDB(
            groups=[{
                'name': 'test_users_group',
                'members': [
                    'user:super-admin@example.com',
                    'user:admin@example.com',
                    'user:priv@example.com',
                    'user:user@example.com',
                ],
                'created_by': 'user:zzz@example.com',
                'modified_by': 'user:zzz@example.com',
            }],
            realms={
                'api_version':
                    auth_realms.API_VERSION,
                'permissions': [{
                    'name': p.name
                } for p in _ALL_PERMS],
                'realms': [
                    {
                        'name': 'test:@root',
                    },
                    {
                        'name':
                            'test:pool/default',
                        'bindings': [{
                            'permissions': [
                                _ALL_PERMS.index(p) for p in permissions
                            ],
                            'principals': [
                                auth.get_current_identity().to_bytes()
                            ],
                        }],
                    },
                    {
                        'name':
                            'test:task_realm',
                        'bindings': [{
                            'permissions': [
                                _ALL_PERMS.index(p) for p in permissions
                            ],
                            'principals': [
                                auth.get_current_identity().to_bytes()
                            ],
                        }, {
                            'permissions': [
                                _ALL_PERMS.index(
                                    realms.get_permission(
                                        realms_pb2.REALM_PERMISSION_TASKS_ACT_AS
                                    )),
                            ],
                            'principals': ['user:service-account@example.com'],
                        }],
                    },
                ],
            },
        ),
        additional_client_ids=[])

  # Bot

  def do_handshake(self, bot='bot1', do_first_poll=False):
    """Performs bot handshake, returns data to be sent to bot handlers.

    Also populates self.bot_version.
    """
    params = {
        'dimensions': {
            'id': [bot],
            'os': ['Amiga'],
            'pool': ['default'],
        },
        'state': {
            'running_time': 1234.0,
            'sleep_streak': 0,
            'started_ts': 1410990411.111,
        },
        'version': '123',
    }
    response = self.app.post_json(
        '/swarming/api/v1/bot/handshake', params=params).json
    self.bot_version = response['bot_version']
    params['version'] = self.bot_version
    params['state']['bot_group_cfg_version'] = response['bot_group_cfg_version']
    # A bit hackish but fine for unit testing purpose.
    if response.get('bot_config'):
      params['bot_config'] = response['bot_config']
    if do_first_poll:
      self.bot_poll(bot=bot, params=params)
    return params

  def bot_poll(self, bot='bot1', params=None):
    """Simulates a bot that polls for task."""
    if not params:
      params = self.do_handshake(bot)
    return self.post_json('/swarming/api/v1/bot/poll', params)

  def bot_complete_task(self, **kwargs):
    # Emulate an isolated task.
    params = {
        'cost_usd': 0.1,
        'duration': 0.1,
        'bot_overhead': 0.1,
        'exit_code': 0,
        'id': 'bot1',
        'cache_trim_stats': {
            'duration': 0.1,
        },
        'cipd_stats': {
            'duration': 3,
        },
        'named_caches_stats': {
            'install': {
                'duration': 0.1,
            },
            'uninstall': {
                'duration': 0.1,
            },
        },
        'isolated_stats': {
            'download': {
                'duration': 1.,
                'initial_number_items': 10,
                'initial_size': 100000,
                'items_cold': [20],
                'items_hot': [30, 40],
            },
            'upload': {
                'duration': 2.,
                'items_cold': [1, 2, 40],
                'items_hot': [1, 2, 3, 50],
            },
        },
        'cleanup_stats': {
            'duration': 0.1,
        },
        'output': base64.b64encode(u'rÉsult string'.encode('utf-8')),
        'output_chunk_start': 0,
        'task_id': None,
    }
    for k in ('download', 'upload'):
      for j in ('items_cold', 'items_hot'):
        params['isolated_stats'][k][j] = base64.b64encode(
            large.pack(params['isolated_stats'][k][j]))
    params.update(kwargs)
    return self.post_json('/swarming/api/v1/bot/task_update', params)

  def bot_run_task(self):
    res = self.bot_poll()
    task_id = res['manifest']['task_id']
    response = self.bot_complete_task(task_id=task_id)
    self.assertEqual({u'must_stop': False, u'ok': True}, response)
    return task_id

  # Client

  def endpoint_call(self, service, name, args):
    srv = test_case.Endpoints(service, source_ip=self.source_ip)
    if not isinstance(args, dict):
      args = json.loads(protojson.encode_message(args))
    return srv.call_api(name, body=args).json

  @staticmethod
  def create_props(**kwargs):
    """Returns a serialized swarming_rpcs.TaskProperties."""
    out = {
        u'cipd_input': {
            u'client_package': {
                u'package_name': u'infra/tools/cipd/${platform}',
                u'version': u'git_revision:deadbeef',
            },
            u'packages': [{
                u'package_name': u'rm',
                u'path': u'bin',
                u'version': u'git_revision:deadbeef',
            }],
            u'server': u'https://pool.config.cipd.example.com',
        },
        u'command': ['python', '-c', 'print(1)'],
        u'containment': {
            u'containment_type': swarming_rpcs.ContainmentType.AUTO,
        },
        u'dimensions': [
            {
                u'key': u'os',
                u'value': u'Amiga'
            },
            {
                u'key': u'pool',
                u'value': u'default'
            },
        ],
        u'env': [],
        u'execution_timeout_secs': 3600,
        u'io_timeout_secs': 1200,
        u'outputs': [u'foo', u'path/to/foobar'],
    }
    out.update((unicode(k), v) for k, v in kwargs.items())
    return out

  def create_new_request(self, **kwargs):
    """Returns an initialized swarming_rpcs.NewTaskRequest.

    Useful to use a swarming_rpcs.TaskSlice.
    """
    out = {
        'expiration_secs': 24 * 60 * 60,
        'name': 'job1',
        'priority': 20,
        'tags': [u'a:tag'],
        'user': 'joe@localhost',
        'bot_ping_tolerance_secs': 600,
    }
    out.update((unicode(k), v) for k, v in kwargs.items())
    # Note that protorpc message constructor accepts dicts for submessages.
    return swarming_rpcs.NewTaskRequest(**out)

  def client_cancel_task(self, task_id, kill_running):
    return self.endpoint_call(handlers_endpoints.SwarmingTaskService, 'cancel',
                              {
                                  'task_id': task_id,
                                  'kill_running': kill_running
                              })

  def client_create_task(self, **kwargs):
    """Creates a minimal task request via the Cloud Endpoints API."""
    request = self.create_new_request(**kwargs)
    response = self.endpoint_call(handlers_endpoints.SwarmingTasksService,
                                  'new', request)
    return response, response['task_id']

  def client_create_task_cas_input_root(self, properties=None, **kwargs):
    """Creates a TaskRequest using a CAS tree via the Cloud Endpoints API.
    """
    properties = (properties or {}).copy()
    properties['cas_input_root'] = {
        'cas_instance': 'projects/test/instances/default',
        'digest': {
            'hash': '12345',
            'size_bytes': 1
        },
    }
    properties['command'] = ['python', 'run_test.py']
    return self.client_create_task(
        properties=self.create_props(**properties), **kwargs)

  def client_create_task_raw(self, properties=None, **kwargs):
    """Creates a raw command TaskRequest via the Cloud Endpoints API."""
    properties = (properties or {}).copy()
    properties['command'] = ['python', 'run_test.py']
    return self.client_create_task(
        properties=self.create_props(**properties), **kwargs)

  def client_get_results(self, task_id, include_performance_stats=False):
    return self.endpoint_call(
        handlers_endpoints.SwarmingTaskService, 'result', {
            'task_id': task_id,
            'include_performance_stats': include_performance_stats
        })

  @staticmethod
  def gen_props(**kwargs):
    """Returns a serialized swarming_rpcs.TaskProperties.

    To be used for expectations.
    """
    out = {
        u'cipd_input': {
            u'client_package': {
                u'package_name': u'infra/tools/cipd/${platform}',
                u'version': u'git_revision:deadbeef',
            },
            u'packages': [{
                u'package_name': u'rm',
                u'path': u'bin',
                u'version': u'git_revision:deadbeef',
            }],
            u'server': u'https://pool.config.cipd.example.com',
        },
        u'command': [u'python', u'-c', u'print(1)'],
        u'containment': {
            u'containment_type': u'AUTO',
        },
        u'dimensions': [
            {
                u'key': u'os',
                u'value': u'Amiga'
            },
            {
                u'key': u'pool',
                u'value': u'default'
            },
        ],
        u'execution_timeout_secs': u'3600',
        u'grace_period_secs': u'30',
        u'idempotent': False,
        u'io_timeout_secs': u'1200',
        u'outputs': [u'foo', u'path/to/foobar']
    }
    out.update((unicode(k), v) for k, v in kwargs.items())
    return out

  @staticmethod
  def gen_request(**kwargs):
    """Returns a serialized swarming_rpcs.TaskRequest.

    To be used for expectations.
    """
    # This assumes:
    # self.mock(random, 'getrandbits', lambda _: 0x88)
    out = {
        u'authenticated': u'user:user@example.com',
        u'expiration_secs': u'86400',
        u'task_id': u'5cee488008810',
        u'name': u'job1',
        u'priority': u'20',
        u'service_account': u'none',
        u'tags': [
            u'a:tag',
            u'authenticated:user:user@example.com',
            u'os:Amiga',
            u'pool:default',
            u'priority:20',
            u'realm:none',
            u'service_account:none',
            u'swarming.pool.template:none',
            u'swarming.pool.version:pools_cfg_rev',
            u'user:joe@localhost',
        ],
        u'user': u'joe@localhost',
        u'bot_ping_tolerance_secs': u'600',
    }
    out.update((unicode(k), v) for k, v in kwargs.items())
    return out

  @staticmethod
  def gen_perf_stats(**kwargs):
    """Returns a serialized swarming_rpcs.PerformanceStats.

    To be used for expectations.
    """
    out = {
        u'bot_overhead': 0.1,
        u'cache_trim': {
            u'duration': 0.1,
        },
        u'package_installation': {
            u'duration': 3.0,
        },
        u'named_caches_install': {
            u'duration': 0.1,
        },
        u'named_caches_uninstall': {
            u'duration': 0.1,
        },
        u'isolated_download': {
            u'duration': 1.0,
            u'initial_number_items': u'10',
            u'initial_size': u'100000',
            u'items_cold': [20],
            u'items_hot': [30, 40],
            u'num_items_cold': u'1',
            u'total_bytes_items_cold': u'20',
            u'num_items_hot': u'2',
            u'total_bytes_items_hot': u'70',
        },
        u'isolated_upload': {
            u'duration': 2.0,
            u'items_cold': [1, 2, 40],
            u'items_hot': [1, 2, 3, 50],
            u'num_items_cold': u'3',
            u'total_bytes_items_cold': u'43',
            u'num_items_hot': u'4',
            u'total_bytes_items_hot': u'56',
        },
        u'cleanup': {
            u'duration': 0.1,
        },
    }
    out.update((unicode(k), v) for k, v in kwargs.items())
    return out

  def gen_result_summary(self, **kwargs):
    """Returns a serialized swarming_rpcs.TaskResult initialized from a
    TaskResultSummary.

    To be used for expectations.
    """
    # This assumes:
    # self.mock(random, 'getrandbits', lambda _: 0x88)
    out = {
        u'bot_dimensions': [
            {
                u'key': u'id',
                u'value': [u'bot1']
            },
            {
                u'key': u'os',
                u'value': [u'Amiga']
            },
            {
                u'key': u'pool',
                u'value': [u'default']
            },
        ],
        u'bot_id': u'bot1',
        u'bot_version': self.bot_version,
        u'current_task_slice': u'0',
        u'failure': False,
        u'internal_failure': False,
        u'name': u'job1',
        u'run_id': u'5cee488008811',
        u'server_versions': [u'v1a'],
        u'state': u'COMPLETED',
        u'tags': [
            u'a:tag',
            u'os:Amiga',
            u'pool:default',
            u'priority:20',
            u'realm:none',
            u'service_account:none',
            u'swarming.pool.template:no_config',
            u'user:joe@localhost',
        ],
        u'task_id': u'5cee488008810',
        u'user': u'joe@localhost',
    }
    out.update((unicode(k), v) for k, v in kwargs.items())
    return out

  def gen_run_result(self, **kwargs):
    """Returns a serialized swarming_rpcs.TaskResult initialized from a
    TaskRunResult.

    To be used for expectations.
    """
    out = {
        u'bot_dimensions': [
            {
                u'key': u'id',
                u'value': [u'bot1']
            },
            {
                u'key': u'os',
                u'value': [u'Amiga']
            },
            {
                u'key': u'pool',
                u'value': [u'default']
            },
        ],
        u'bot_id':
        u'bot1',
        u'bot_version':
        self.bot_version,
        u'costs_usd': [0.0],
        u'current_task_slice':
        u'0',
        u'failure':
        False,
        u'internal_failure':
        False,
        u'name':
        u'job1',
        u'run_id':
        u'5cee488008811',
        u'server_versions': [u'v1a'],
        u'state':
        u'RUNNING',
        u'task_id':
        u'5cee488008811'
    }
    out.update((unicode(k), v) for k, v in kwargs.items())
    return out

  def assertErrorResponseMessage(self, message, response):
    """Asserts the error message in http response.
    """
    self.assertEqual({'error': {'message': message}}, response.json)

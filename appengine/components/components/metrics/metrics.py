# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Defines Buffer class that can be used to batch Cloud Monitoring metrics.

See https://cloud.google.com/monitoring/custom-metrics/.

Usage:

  METRIC_DESCRIPTOR = metrics.Descriptor(
      name='service/metric',
      description='Blah blah',
      labels={'component': 'Blah blah'})

  buf = metrics.Buffer()
  buf.set_gauge(METRIC_DESCRIPTOR, 10, labels={'component': 'some'})
  buf.set_gauge(METRIC_DESCRIPTOR, 20, labels={'component': 'another'})
  buf.flush()

If you want to use asynchronous flush (via task queues) include the components
in app.yaml:

  includes:
    - components/metrics

TODO(vadimsh): Support more metric types when Monitoring API supports them.
TODO(vadimsh): "flush" to the datastore/memcache, send from a cron job.
"""

import collections
import json
import logging

from google.appengine.api import app_identity
from google.appengine.ext import ndb

import webapp2

from components import auth
from components import net
from components import utils
from components.datastore_utils import config


# Public API.
__all__ = [
  'Buffer',
  'Descriptor',
  'MonitoringConfig',
  'METRIC_TYPES',
  'VALUE_TYPES',
]


# Monitoring API supports only gauge custom metrics currently.
METRIC_TYPES = [
  # A sample is a value of some property at an instant in time, e.g. CPU load.
  # Sent to Cloud Monitoring as is.
  'gauge',
]


# Name of the value type -> python type(s). Only int64 and double are supported.
VALUE_TYPES = {
  'double': (float, 'doubleValue'),
  'int64': ((int, long), 'int64Value'),
}


class Descriptor(object):
  """Descriptor defines a schema of a metric.

  See https://cloud.google.com/monitoring/v2beta2/metricDescriptors#resource

  Args:
    name: name of the metric (e.g. "service/name").
    description: textual description of the metric.
    labels: dict {label name: description} or empty for lightweight metric.
    metric_type: type of the metric, see METRIC_TYPES.
    value_type: type of the metric value, see VALUE_TYPES.
  """

  def __init__(
      self,
      name,
      description,
      labels=None,
      metric_type='gauge',
      value_type='int64'):
    if metric_type not in METRIC_TYPES:
      raise ValueError('Metric type not supported: %s' % metric_type)
    if value_type not in VALUE_TYPES:
      raise ValueError('Value type not supported: %s' % value_type)
    self.name = name
    self.description = description
    self.labels = sorted(labels.iteritems()) if labels else []
    self.metric_type = metric_type
    self.value_type = value_type

  def to_dict(self):
    return {
      'name': self.name,
      'description': self.description,
      'labels': dict(self.labels),
      'metric_type': self.metric_type,
      'value_type': self.value_type,
    }

  @staticmethod
  def from_dict(data):
    return Descriptor(
        name=data['name'],
        description=data['description'],
        labels=data['labels'],
        metric_type=data['metric_type'],
        value_type=data['value_type'])

  def make_metric(self):
    """Returns _Metric instance to use for metrics with this description."""
    assert self.metric_type in METRIC_TYPES
    if self.metric_type == 'gauge':
      return _GaugeMetric(self)

  def validate_value(self, value):
    """Checks that metric value match the descriptor."""
    expected = VALUE_TYPES[self.value_type][0]
    if not isinstance(value, expected):
      raise TypeError(
          'Invalid value type %s, expecting %s' % (type(value), expected))

  def validate_labels(self, labels):
    """Checks that labels match the descriptor."""
    labels = labels or {}
    if len(self.labels) != len(labels):
      raise TypeError(
          'Expecting %d labels, got %d' % (len(self.labels), len(labels)))
    for key, _ in self.labels:
      val = labels.get(key)
      if not isinstance(val, basestring):
        raise TypeError('Label %s must be string' % key)
      if not val:
        raise TypeError('Missing value for label %s' % key)


class Buffer(object):
  """Holds collected metrics before they are sent to the Cloud."""

  def __init__(self):
    self._descriptors = {}
    self._metrics = collections.defaultdict(collections.OrderedDict)

  def set_gauge(self, descriptor, value, labels=None):
    """Changes a value of a gauge metric.

    Args:
      descriptor: instance of Descriptor with metric definition.
      value: new value of the gauge.
      labels: dict with metric labels (must match descriptor schema).
    """
    assert descriptor
    if descriptor.metric_type != 'gauge':
      raise TypeError('Expecting gauge metric descriptor')
    self._metric(descriptor, labels).set_gauge(value)

  def flush(self, task_queue_name=None):
    """Dumps all buffered metrics to Cloud Monitoring.

    Args:
      task_queue_name: task queue to use for asynchronous execution or None to
          execute right now.
    """
    # Collect all data into single uberdict to pass through the task queue.
    descriptors = {k: v.to_dict() for k, v in self._descriptors.iteritems()}
    points = []
    for desc_name, metrics in sorted(self._metrics.iteritems()):
      for labels, metric in metrics.iteritems():
        points.append({
          'desc': desc_name,
          'labels': labels,
          'point': metric.to_dict(),
        })
    self._descriptors.clear()
    self._metrics.clear()
    if not points:
      return
    flush_task = {
      'descriptors': descriptors,
      'points': points,
    }
    if task_queue_name:
      _enqueue_flush(flush_task, task_queue_name)
    else:
      try:
        _execute_flush(flush_task)
      except Exception:
        logging.exception('Failed to send monitoring metrics')

  ## Private part.

  def _metric(self, descriptor, labels=None):
    # Ensure metric labels match the descriptor.
    labels = labels or {}
    descriptor.validate_labels(labels)
    # Register the descriptor.
    if descriptor.name not in self._descriptors:
      self._descriptors[descriptor.name] = descriptor
    else:
      cur = self._descriptors[descriptor.name]
      if cur is not descriptor and cur.to_dict() != descriptor.to_dict():
        raise ValueError(
            'Metric name %r is used by two different descriptors' %
            descriptor.name)
    # Get or create a metric.
    key = tuple(labels[k] for k, _ in descriptor.labels)
    metric = self._metrics[descriptor.name].get(key)
    if metric is None:
      metric = descriptor.make_metric()
      self._metrics[descriptor.name][key] = metric
    return metric


class MonitoringConfig(config.GlobalConfig):
  """Application-wide monitoring-related settings."""
  # Cloud Project ID to send metrics to (current GAE project by default).
  project_id = ndb.StringProperty(indexed=False)
  # Service account to use to authenticate (GAE service account by default).
  client_email = ndb.StringProperty(indexed=False)
  # Private key of the corresponding service account.
  private_key = ndb.StringProperty(indexed=False)
  # Private key ID of the corresponding service account.
  private_key_id = ndb.StringProperty(indexed=False)

  @property
  def service_account_key(self):
    if not self.client_email:
      return None
    return auth.ServiceAccountKey(
        client_email=self.client_email,
        private_key=self.private_key,
        private_key_id=self.private_key_id)

  @service_account_key.setter
  def service_account_key(self, value):
    if not value:
      self.client_email = None
      self.private_key = None
      self.private_key_id = None
    else:
      assert isinstance(value, auth.ServiceAccountKey)
      self.client_email = value.client_email
      self.private_key = value.private_key
      self.private_key_id = value.private_key_id


## Internal guts, do not use directly.


# OAuth2 scope.
_MONITORING_SCOPE = 'https://www.googleapis.com/auth/monitoring'


# URL to use to register metrics.
_CREATE_URL = (
    'https://www.googleapis.com/cloudmonitoring/v2beta2'
    '/projects/{project_id}/metricDescriptors')

# URL to push metrics to.
_WRITE_URL = (
    'https://www.googleapis.com/cloudmonitoring/v2beta2'
    '/projects/{project_id}/timeseries:write')

# Format string for UTC datetime as expected by Monitoring API.
_TS_FORMAT = '%Y-%m-%dT%H:%M:%SZ'


# Number of metrics to push at once (API limit is 200).
_MAX_BATCH_SIZE = 100

# Alert threshold for task queue task length (GAE limit is 100Kb).
_MAX_TASK_SIZE = 90 * 1024


class _Metric(object):
  """Carries single sample of a metric.

  Used as a base class for concrete metric types.
  """

  def __init__(self, descriptor):
    self.descriptor = descriptor
    self.value = None
    self.min_ts = None
    self.max_ts = None

  def to_dict(self):
    assert self.min_ts is not None
    assert self.max_ts is not None
    return {
      'start': self.min_ts,
      'end': self.max_ts,
      'value': self.value,
    }


class _GaugeMetric(_Metric):
  def set_gauge(self, value):
    self.descriptor.validate_value(value)
    self.value = value
    self.min_ts = utils.datetime_to_timestamp(utils.utcnow())
    self.max_ts = self.min_ts


def _split_in_batches(enumerable, batch_size):
  """Given enumerable, yields lists (each list <= batch_size in length)."""
  cur = []
  for x in enumerable:
    cur.append(x)
    if len(cur) == batch_size:
      yield cur
      cur = []
  if cur:
    yield cur


def _enqueue_flush(flush_task, task_queue_name):
  """Enqueues a task that sends metrics."""
  # Task queue size limit is 100Kb, beware the size approaching the limit.
  payload = json.dumps(flush_task, sort_keys=True, separators=(',', ':'))
  if len(payload) >= _MAX_TASK_SIZE:
    logging.error(
        'Metrics push task payload is too big.\n%.1f Kb', len(payload) / 1024.0)
  ok = utils.enqueue_task(
      url='/internal/task/metrics/flush',
      queue_name=task_queue_name,
      payload=payload)
  if not ok:
    logging.error('Failed to enqueue a task to send metrics')


def _execute_flush(flush_task):
  """Pushes metrics to Cloud Monitoring API.

  See Buffer.flush() for the format of flush_task dict.
  """
  descriptors = {
    k: Descriptor.from_dict(v)
    for k, v in flush_task['descriptors'].iteritems()
  }
  _send_metrics(descriptors, flush_task['points'])


def _send_metrics(descriptors, points):
  """Calls 'timeseries:write' API to push metrics to Cloud Monitoring.

  Args:
    descriptors: dict {name -> Descriptor object}.
    points: list of dicts {'desc': <name>, 'labels': <list>, 'point': <dict>}
        where point dict is in a format of _Metric.to_dict().
  """
  if not points:
    return

  conf = MonitoringConfig.cached()
  project_id = conf.project_id or app_identity.get_application_id()
  service_account_key = conf.service_account_key

  # Register all metrics (in parallel).
  futures = []
  for name in sorted(descriptors):
    futures.append(
        _register_metric(descriptors[name], service_account_key, project_id))
  ndb.Future.wait_all(futures)
  for f in futures:
    f.check_success()

  def make_point_dict(p):
    # See https://cloud.google.com/monitoring/v2beta2/timeseries/write.
    desc = descriptors[p['desc']]
    labels = {l[0]: v for l, v in zip(desc.labels, p['labels'])}
    point = p['point']
    desc.validate_value(point['value'])
    value_key = '%sValue' % desc.value_type
    return {
      'timeseriesDesc': {
        'metric': 'custom.cloudmonitoring.googleapis.com/%s' % desc.name,
        'labels': {
          'custom.cloudmonitoring.googleapis.com/%s' % k: v
          for k, v in labels.iteritems()
        },
      },
      'point': {
        'start': utils.timestamp_to_datetime(
            point['start']).strftime(_TS_FORMAT),
        'end': utils.timestamp_to_datetime(
            point['end']).strftime(_TS_FORMAT),
        value_key: point['value'],
      },
    }

  # Split points in batches and send each batch in parallel.
  futures = []
  for batch in _split_in_batches(points, _MAX_BATCH_SIZE):
    futures.append(net.json_request_async(
        url=_WRITE_URL.format(project_id=project_id),
        method='POST',
        payload={'timeseries': [make_point_dict(p) for p in batch]},
        scopes=[_MONITORING_SCOPE],
        service_account_key=service_account_key))
  ndb.Future.wait_all(futures)
  for f in futures:
    f.check_success()


@ndb.tasklet
def _register_metric(descriptor, service_account_key, project_id):
  """Registers a metric if it is not registered yet."""
  # Use datastore (and memcache via NDB) to keep "already registered" flag. Do
  # not bother with transactions since Monitoring API call below is idempotent.
  key = ndb.Key(_MonitoringMetric, '%s:%s' % (project_id, descriptor.name))
  existing = yield key.get_async()
  if existing and existing.descriptor == descriptor.to_dict():
    return
  # See https://cloud.google.com/monitoring/v2beta2/metricDescriptors/create.
  # Monitoring API doesn't mind when the metric is updated with modified labels.
  resp = yield net.json_request_async(
      url=_CREATE_URL.format(project_id=project_id),
      method='POST',
      payload={
        'name': 'custom.cloudmonitoring.googleapis.com/%s' % descriptor.name,
        'description': descriptor.description,
        'labels': [
          {
            'key': 'custom.cloudmonitoring.googleapis.com/%s' % k,
            'description': d,
          } for k, d in descriptor.labels
        ],
        'typeDescriptor': {
          'metricType': descriptor.metric_type,
          'valueType': descriptor.value_type,
        },
      },
      scopes=[_MONITORING_SCOPE],
      service_account_key=service_account_key)
  yield _MonitoringMetric(key=key, descriptor=descriptor.to_dict()).put_async()
  logging.info('Metric %s is updated: %s', descriptor.name, resp)


class _MonitoringMetric(ndb.Model):
  descriptor = ndb.JsonProperty()


## Routing for task queue tasks.


class FlushTaskHandler(webapp2.RequestHandler):
  def post(self):
    # Check that called from a task queue, doesn't matter which one.
    if not self.request.headers.get('X-AppEngine-QueueName'):
      self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
      msg = 'Only internal task can do this'
      logging.error(msg)
      self.abort(403, msg)
      return
    # Do not retry too much. Better to drop the metrics rather than fill the
    # queue with failing tasks.
    execution_count = self.request.headers.get('X-AppEngine-TaskExecutionCount')
    if int(execution_count or '0') >= 10:
      logging.error('Giving up on metric sending retries, see logs')
      return
    # Let exceptions fall through to cause task retry (and scary log entry).
    _execute_flush(json.loads(self.request.body))


def get_backend_routes():
  return [webapp2.Route('/internal/task/metrics/flush', FlushTaskHandler)]

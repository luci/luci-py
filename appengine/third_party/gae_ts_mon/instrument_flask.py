# Copyright 2019 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.

import flask
import logging
import time

from infra_libs.ts_mon import exporter
from infra_libs.ts_mon import handlers
from infra_libs.ts_mon import shared
from infra_libs.ts_mon.common import http_metrics
from infra_libs.ts_mon.common import interface


def _is_instrumented(app):
  """Checks if FlaskInstrumentor() has been installed into the middleware chain.
  """
  max_depth = 32
  cur = app
  while cur is not None and max_depth:
    if isinstance(cur, FlaskInstrumentor):
      return True
    cur = getattr(cur, 'wsgi_app', None)
    max_depth -= 1

  if cur is not None and max_depth == 0:
    raise Exception('Failed to check if the Flask is instrumented by tsmon; '
                    'max-depth for the middleware chain has been reached')
  return False


def instrument(app, time_fn=time.time):
  if not _is_instrumented(app):
    app.wsgi_app = FlaskInstrumentor(app.wsgi_app, time_fn)
    app.add_url_rule(
        shared.CRON_REQUEST_PATH_TASKNUM_ASSIGNER, view_func=assign_task_num)

  return app


class FlaskHTTPStat(object):

  def __init__(self):
    self.name = ''
    self.request_content_length = 0
    self.request_user_agent = ''
    self.response_status = 0
    self.response_content_length = 0

  def _get_status_code(self, status):
    """Returns the status code from a given status string.

    If the status contains an invalid value, 500 is returned instead.
    """
    try:
      s = status.split(None, 1)
      return int(s[0])

    # This would never happen. flask/werkzeug adds status-code 0 into the
    # status line, if a page handler returns a status line with an invalid
    # status code. Or, if a handler returns an empty status line, an exception
    # is raised before this point reached.
    except (IndexError, ValueError):  # pragma: no cover
      logging.exception('Invalid status code')
      return 0

  def _get_response_content_length(self, headers):
    """Returns the content length of the response.

    If 'Content-Length' is missing from the response headers or the value is
    not a valid number, then 0 is returned instead.
    """
    try:
      for h in headers:
        if h[0] == 'Content-Length':
          return int(h[1])

      # werkzeug always generates Content-Length header, even if
      # automatically_set_content_length is set with False, if a generated
      # Response() doesn't have one. Therefore, this won't be reached.
      return 0  # pragma: no cover
    except ValueError:
      logging.exception('Invalid Content-Length')
      return 0

  def parse(self, status, request, response_headers):
    """Parses the request context and response headers to get the stat info."""
    try:
      # If the requested path doesn't match with any of the existing rules,
      # then url_rule is None. e.g., 404
      if flask.request.url_rule is not None:
        # Use the url rule, such as '/user/<int:id>', to prevent an explosion
        # in possible field values.
        self.name = request.url_rule.rule
      self.request_user_agent = flask.request.user_agent.string
      self.response_status = self._get_status_code(status)
      self.response_content_length = self._get_response_content_length(
          response_headers)

    except Exception:
      # tsmon bug? log the exception and report it with status-code 0.
      # The actual HTTP response will be sent to the client, as original.
      logging.exception("Failed to parse HTTP response")
      self.response_status = 0
      self.response_content_length = 0


class FlaskInstrumentor(object):

  def __init__(self, app, time_fn):
    self.app = app
    self.time_fn = time_fn

  def __call__(self, environ, start_response):
    start_time = self.time_fn()
    stat = FlaskHTTPStat()

    def _start_response(status, headers, exc_info=None):
      # This wraps the start_response to catch the request context and response.
      stat.parse(status, flask.request, headers)
      return start_response(status, headers, exc_info)

    try:
      # start a flush thread, if necessary.
      with exporter.parallel_flush(start_time):
        return self.app(environ, _start_response)
    except Exception:
      # If a uncaught exception occurs, then the default or custom
      # handle_exception() is invoked to handle response, and _start_response()
      # is not invoked. At this moment, the request context has been popped out,
      # and stat contains empty string or 0 values.
      stat.name = environ.get('PATH_INFO', '')
      stat.response_status = 500
      raise
    finally:
      elapsed_ms = int((self.time_fn() - start_time) * 1000)
      http_metrics.update_http_server_metrics(
          stat.name,
          stat.response_status,
          elapsed_ms,
          request_size=stat.request_content_length,
          response_size=stat.response_content_length,
          user_agent=stat.request_user_agent)


@handlers.report_memory
def assign_task_num():
  """Assigns a unique task num into each of the active Appengine instances."""
  if flask.request.headers.get('X-Appengine-Cron') != 'true':
    flask.abort(403)

  with shared.instance_namespace_context():
    handlers._assign_task_num()
  interface.invoke_global_callbacks()

  return "OK"

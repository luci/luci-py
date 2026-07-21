#!/usr/bin/env vpython
# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import datetime
import logging
import os
import platform
import re
import sys
import unittest

from test_support import test_env

test_env.setup_test_env()

import webapp2
import webtest

from components import auth
from components.ereporter2 import formatter
from components.ereporter2 import models
from components.ereporter2 import on_error
from test_support import test_case


ON_ERROR_PATH = os.path.abspath(on_error.__file__)


class Ereporter2OnErrorTest(test_case.TestCase):
  def setUp(self):
    super(Ereporter2OnErrorTest, self).setUp()
    self.mock(logging, "error", lambda *_, **_kwargs: None)
    self._now = datetime.datetime(2014, 6, 24, 20, 19, 42, 653775)
    self.mock_now(self._now, 0)

  def test_log(self):
    kwargs = dict((k, k) for k in on_error.VALID_ERROR_KEYS)
    kwargs["args"] = ["args"]
    kwargs["category"] = "exception"
    kwargs["duration"] = 2.3
    kwargs["env"] = {"foo": "bar"}
    kwargs["params"] = {"foo": "bar"}
    kwargs["source"] = "bot"
    kwargs["source_ip"] = "0.0.0.0"
    on_error.log(**kwargs)
    self.assertEqual(1, models.Error.query().count())
    expected = {
      "args": ["args"],
      "category": "exception",
      "created_ts": self._now,
      "cwd": "cwd",
      "duration": 2.3,
      "endpoint": "endpoint",
      "env": {"foo": "bar"},
      "exception_type": "exception_type",
      "hostname": "hostname",
      "identity": None,
      "message": "message",
      "method": "method",
      "os": "os",
      "params": {"foo": "bar"},
      "python_version": "python_version",
      "request_id": "request_id",
      "source": "bot",
      "source_ip": "0.0.0.0",
      "stack": "stack",
      "user": "user",
      "version": "version",
    }
    self.assertEqual(expected, models.Error.query().get().to_dict())

  def test_log_server(self):
    # version is automatiaclly added.
    on_error.log(source="server")
    self.assertEqual(1, models.Error.query().count())
    expected = dict((k, None) for k in on_error.VALID_ERROR_KEYS)
    expected["args"] = []
    expected["created_ts"] = self._now
    expected["identity"] = None
    expected["python_version"] = unicode(platform.python_version())
    expected["source"] = "server"
    expected["source_ip"] = None
    expected["version"] = "v1a"
    self.assertEqual(expected, models.Error.query().get().to_dict())

  def test_ignored_flag(self):
    on_error.log(foo="bar")
    self.assertEqual(1, models.Error.query().count())
    expected = {
      "args": [],
      "category": None,
      "created_ts": self._now,
      "cwd": None,
      "duration": None,
      "endpoint": None,
      "env": None,
      "exception_type": None,
      "hostname": None,
      "identity": None,
      "message": None,
      "method": None,
      "os": None,
      "params": None,
      "python_version": None,
      "request_id": None,
      "source": "unknown",
      "source_ip": None,
      "stack": None,
      "user": None,
      "version": None,
    }
    self.assertEqual(expected, models.Error.query().get().to_dict())

  def test_exception(self):
    on_error.log(env="str")
    self.assertEqual(1, models.Error.query().count())
    relpath_on_error = formatter._relative_path(ON_ERROR_PATH)
    expected = {
      "args": [],
      "category": "exception",
      "created_ts": self._now,
      "cwd": None,
      "duration": None,
      "endpoint": None,
      "env": None,
      "exception_type": "<type 'exceptions.TypeError'>",
      "hostname": None,
      "identity": None,
      "message": "log({'env': 'str'}) caused: JSON property must be a "
      "<type 'dict'>",
      "method": None,
      "os": None,
      "params": None,
      "python_version": None,
      "request_id": None,
      "source": "server",
      "source_ip": None,
      "stack": "Traceback (most recent call last):\n"
      '  File "%s", line 0, in log\n'
      "    error = models.Error(identity=identity, **kwargs)\n"
      '  File "appengine/ext/ndb/model.py", line 0, in __init__\n'
      % relpath_on_error.replace(".pyc", ".py"),
      "user": None,
      "version": None,
    }
    actual = models.Error.query().get().to_dict()
    # Zap out line numbers to 0, it's annoying otherwise to update the unit test
    # just for line move. Only keep the first 4 lines because json_dict
    # verification is a tad deep insode ndb/model.py.
    actual["stack"] = "".join(
      re.sub(r" \d+", " 0", actual["stack"]).splitlines(True)[:4]
    )
    # Also make no distinction between *.pyc and *.py files.
    actual["stack"] = actual["stack"].replace(".pyc", ".py")
    self.assertEqual(expected, actual)

  def test_log_request(self):
    # Create a small adhoc webapp2 instance and ensures logging works.
    def handle(request):
      on_error.log_request(request)

    app = webtest.TestApp(webapp2.WSGIApplication([("/", handle)], debug=True))
    app.get("/?foo=bar")
    app.post("/?foo=bar", {"foo": "baz"})
    # Strip None values for clarity.
    actual = [
      {k: v for k, v in e.to_dict().items() if v is not None}
      for e in models.Error.query()
    ]
    # It happens this value is hardcoded on time.
    request_id = "7357B3D7091D"
    expected = [
      {
        "args": [],
        "created_ts": self._now,
        "endpoint": "/",
        "method": "GET",
        "params": {"foo": "bar"},
        "request_id": request_id,
        "source": "unknown",
      },
      {
        "args": [],
        "created_ts": self._now,
        "endpoint": "/",
        "method": "POST",
        "params": {"foo": ["bar", "baz"]},
        "request_id": request_id,
        "source": "unknown",
      },
    ]
    self.assertEqual(expected, actual)


class Ereporter2OnErrorTestNoAuth(test_case.TestCase):
  def setUp(self):
    super(Ereporter2OnErrorTestNoAuth, self).setUp()
    self._now = datetime.datetime(2014, 6, 24, 20, 19, 42, 653775)
    self.mock_now(self._now, 0)

  def test_log(self):
    # It must work even if auth is not initialized.
    self.mock(logging, "error", lambda *_, **_kwargs: None)
    error_id = on_error.log(
      source="bot", category="task_failure", message="Dang"
    )
    self.assertEqual(1, models.Error.query().count())
    self.assertEqual(error_id, models.Error.query().get().key.integer_id())
    expected = {
      "args": [],
      "category": "task_failure",
      "created_ts": self._now,
      "cwd": None,
      "duration": None,
      "endpoint": None,
      "env": None,
      "exception_type": None,
      "hostname": None,
      "identity": None,
      "message": "Dang",
      "method": None,
      "os": None,
      "params": None,
      "python_version": None,
      "request_id": None,
      "source": "bot",
      "source_ip": None,
      "stack": None,
      "user": None,
      "version": None,
    }
    self.assertEqual(expected, models.Error.query().get().to_dict())


if __name__ == "__main__":
  if "-v" in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
    level=logging.DEBUG if "-v" in sys.argv else logging.ERROR
  )
  unittest.main()

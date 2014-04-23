# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

from google.appengine.ext import testbed

from depot_tools import auto_stub


# W0212: Access to a protected member XXX of a client class
# pylint: disable=W0212


class TestCase(auto_stub.TestCase):
  """Support class to enable google.appengine.api.mail.send_mail_to_admins()."""
  def setUp(self):
    super(TestCase, self).setUp()
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    # Using init_all_stubs() costs ~10ms more to run all the tests.
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_logservice_stub()
    self.testbed.init_memcache_stub()
    self.testbed.init_mail_stub()
    self.mail_stub = self.testbed.get_stub(testbed.MAIL_SERVICE_NAME)
    self.old_send_to_admins = self.mock(
        self.mail_stub, '_Dynamic_SendToAdmins', self._SendToAdmins)

  def tearDown(self):
    self.testbed.deactivate()
    super(TestCase, self).tearDown()

  def _SendToAdmins(self, request, *args, **kwargs):
    """Make sure the request is logged.

    See google_appengine/google/appengine/api/mail_stub.py around line 299,
    MailServiceStub._SendToAdmins().
    """
    self.mail_stub._CacheMessage(request)
    return self.old_send_to_admins(request, *args, **kwargs)

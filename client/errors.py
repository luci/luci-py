# Copyright 2022 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.
"""Exceptions for handling CIPD and CAS errors"""


class NonRecoverableException(Exception):
  """For handling errors where we cannot recover from and should not retry."""

  def __init__(self, status, msg):
    super(Exception, self).__init__(msg)
    self.status = status

  def to_dict(self):
    """Returns a dictionary with the attributes serialised."""
    raise NotImplementedError()


class NonRecoverableCasException(NonRecoverableException):
  """For handling a bad CAS input where we should not attempt to retry."""

  def __init__(self, status, digest, instance):
    super(NonRecoverableCasException, self).__init__(
        status, "CAS error: {} with digest {} on instance {}".format(
            status, digest, instance))
    self.digest = digest
    self.instance = instance

  def to_dict(self):
    return {
        'status': self.status,
        'digest': self.digest,
        'instance': self.instance,
    }


class NonRecoverableCipdException(NonRecoverableException):
  """For handling a bad CIPD package where we should not attempt to retry."""

  def __init__(self, status, package_name, path, version):
    super(NonRecoverableCipdException, self).__init__(
        status, "CIPD error: {} with package {}, version {} on path {}".format(
            status, package_name, version, path))
    self.package_name = package_name
    self.path = path
    self.version = version

  def to_dict(self):
    return {
        'status': self.status,
        'package_name': self.package_name,
        'path': self.path,
        'version': self.version
    }

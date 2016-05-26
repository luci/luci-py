# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""CIPD-specific code is concentrated here."""

import re

# Regular expressions below are copied from
# https://chromium.googlesource.com/infra/infra/+/468bb43/appengine/chrome_infra_packages/cipd/impl.py
# https://chromium.googlesource.com/infra/infra/+/468bb43/appengine/chrome_infra_packages/cas/impl.py

PACKAGE_NAME_RE = re.compile(r'^([a-z0-9_\-]+/)*[a-z0-9_\-]+$')
INSTANCE_ID_RE = re.compile(r'^[0-9a-f]{40}$')
TAG_KEY_RE = re.compile(r'^[a-z0-9_\-]+$')
REF_RE = re.compile(r'^[a-z0-9_\-]{1,100}$')
TAG_MAX_LEN = 400


# CIPD package name template parameters allow a user to reference different
# packages for different enviroments. Inspired by
# https://chromium.googlesource.com/infra/infra/+/f1072a132c68532b548458392c5444f04386d684/build/README.md
# The values of the parameters are computed on the bot.
#
# Platform parameter value is "<os>-<arch>" string, where
# os can be "linux", "mac" or "windows" and arch can be "386", "amd64" or "arm".
PARAM_PLATFORM = '${platform}'
# OS version parameter defines major and minor version of the OS distribution.
# It is useful if package depends on .dll/.so libraries provided by the OS.
# Example values: "ubuntu14_04", "mac10_9", "win6_1".
PARAM_OS_VER = '${os_ver}'
ALL_PARAMS = (PARAM_PLATFORM, PARAM_OS_VER)


def is_valid_package_name(package_name):
  """Returns True if |package_name| is a valid CIPD package name."""
  return bool(PACKAGE_NAME_RE.match(package_name))


def is_valid_package_name_template(template):
  """Returns True if |package_name| is a valid CIPD package name template."""
  # Render known parameters first.
  for p in ALL_PARAMS:
    template = template.replace(p, 'x')
  return is_valid_package_name(template)


def is_valid_version(version):
  """Returns True if |version| is a valid CIPD package version."""
  return bool(
      INSTANCE_ID_RE.match(version) or
      is_valid_tag(version) or
      REF_RE.match(version)
  )


def is_valid_tag(tag):
  """True if string looks like a valid package instance tag."""
  if not tag or ':' not in tag or len(tag) > TAG_MAX_LEN:
    return False
  # Care only about the key. Value can be anything (including empty string).
  return bool(TAG_KEY_RE.match(tag.split(':', 1)[0]))


def is_pinned_version(version):
  """Returns True if |version| is pinned."""
  return bool(INSTANCE_ID_RE.match(version)) or is_valid_tag(version)

# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""CIPD-specific code is concentrated here."""

import contextlib
import logging
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
# os can be "linux", "mac" or "windows" and arch can be "386", "amd64" or
# "armv6l".
PARAM_PLATFORM = '${platform}'
PARAM_PLATFORM_ESC = re.escape(PARAM_PLATFORM)
# OS version parameter defines major and minor version of the OS distribution.
# It is useful if package depends on .dll/.so libraries provided by the OS.
# Example values: "ubuntu14_04", "mac10_9", "win6_1".
PARAM_OS_VER = '${os_ver}'
PARAM_OS_VER_ESC = re.escape(PARAM_OS_VER)
ALL_PARAMS = (PARAM_PLATFORM, PARAM_OS_VER)


@contextlib.contextmanager
def pin_check_fn(platform, os_ver):
  """Yields a function that verifies that an input CipdPackage could have been
  plausibly expanded, via pinning, to another CipdPackage. Repeated invocations
  of the function will retain knowledge of any resolved name template paramters
  like ${platform} and ${os_ver}.

  Args:
    platform - a pre-defined expansion of ${platform}, or None to learn from the
        first valid checked CipdPackage containing ${platform}.
    os_ver - a pre-defined expansion of ${os_ver}, or None to learn from the
        first valid checked CipdPackage containing ${os_ver}.

  Args of yielded function:
    original - a CipdPackage which may contain template params like ${platform}
    expanded - a CipdPackage which is nominally an expansion of original.

  CipdPackage is a duck-typed object which has three string properties:
  'package_name', 'path' and 'version'.

  Yielded function raises:
    ValueError if expanded is not a valid derivation of original.

  Example:
    with pin_check_fn(None, None) as check:
      check(CipdPackage('', '${platform}', 'ref'),
          CipdPackage('', 'windows-amd64', 'deadbeef'*5))
      check(CipdPackage('', '${platform}', 'ref'),
          CipdPackage('', 'linux-amd64', 'deadbeef'*5)) ## will raise ValueError
  """
  plat_ref = [platform]
  os_ver_ref = [os_ver]
  def _check_fn(original, expanded):
    if original.path != expanded.path:
      logging.warn('Mismatched path: %r v %r', original.path, expanded.path)
      raise ValueError('Mismatched path')

    def sub_param(regex, param_esc, param_re, param_const):
      # This is validated at task creation time as well, but just to make sure.
      if regex.count(param_esc) > 1:
        logging.warn('Duplicate template param %r: %r', param_esc, regex)
        raise ValueError('%s occurs more than once in name.' % param_esc)

      ret = False
      if param_const is None:
        ret = param_esc in regex
        if ret:
          regex = regex.replace(param_esc, param_re, 1)
      else:
        regex = regex.replace(param_esc, param_const, 1)
      return regex, ret

    name_regex = re.escape(original.package_name)
    name_regex, scan_plat = sub_param(
        name_regex, PARAM_PLATFORM_ESC, r'(?P<platform>\w+-[a-z0-9]+)',
        plat_ref[0])
    name_regex, scan_os_ver = sub_param(
        name_regex, PARAM_OS_VER_ESC, r'(?P<os_ver>[_a-z0-9]+)',
        os_ver_ref[0])

    match = re.match(name_regex, expanded.package_name)
    if not match:
      logging.warn('Mismatched package_name: %r | %r v %r',
          original.package_name, name_regex, expanded.package_name)
      raise ValueError('Mismatched package_name')

    if is_valid_instance_id(original.version):
      if original.version != expanded.version:
        logging.warn('Mismatched pins: %r v %r', original.version,
            expanded.version)
        raise ValueError('Mismatched pins')
    else:
      if not is_valid_instance_id(expanded.version):
        logging.warn('Pin not a pin: %r', expanded.version)
        raise ValueError('Pin value is not a pin')

    if scan_plat:
      plat_ref[0] = re.escape(match.group('platform'))
    if scan_os_ver:
      os_ver_ref[0] = re.escape(match.group('os_ver'))

  yield _check_fn


def is_valid_package_name(package_name):
  """Returns True if |package_name| is a valid CIPD package name."""
  return bool(PACKAGE_NAME_RE.match(package_name))


def is_valid_package_name_template(template):
  """Returns True if |package_name| is a valid CIPD package name template."""
  # Render known parameters first.
  for p in ALL_PARAMS:
    template = template.replace(p, 'x')
    if template.count(p) > 1:
      return False
  return is_valid_package_name(template)


def is_valid_version(version):
  """Returns True if |version| is a valid CIPD package version."""
  return bool(
      is_valid_instance_id(version) or
      is_valid_tag(version) or
      REF_RE.match(version)
  )


def is_valid_tag(tag):
  """True if string looks like a valid package instance tag."""
  if not tag or ':' not in tag or len(tag) > TAG_MAX_LEN:
    return False
  # Care only about the key. Value can be anything (including empty string).
  return bool(TAG_KEY_RE.match(tag.split(':', 1)[0]))


def is_valid_instance_id(version):
  """Returns True if |version| is an insance_id."""
  return bool(INSTANCE_ID_RE.match(version))


def is_pinned_version(version):
  """Returns True if |version| is pinned."""
  return is_valid_instance_id(version) or is_valid_tag(version)

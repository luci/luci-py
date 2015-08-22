# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""OSX specific utility functions."""

import cgi
import os
import platform
import re
import subprocess

import plistlib

from utils import tools


@tools.cached
def _get_SPDisplaysDataType():
  """Returns an XML about the system display properties."""
  sp = subprocess.check_output(
      ['system_profiler', 'SPDisplaysDataType', '-xml'])
  return plistlib.readPlistFromString(sp)[0]['_items']


### Public API.


@tools.cached
def get_os_version_number():
  """Returns the normalized OS version number as a string.

  Returns:
    10.7, 10.8, etc.
  """
  version_parts = platform.mac_ver()[0].split('.')
  assert len(version_parts) >= 2, 'Unable to determine Mac version'
  return u'.'.join(version_parts[:2])


def generate_launchd_plist(command, cwd, plistname):
  """Generates a plist content with the corresponding command for launchd."""
  # The documentation is available at:
  # https://developer.apple.com/library/mac/documentation/Darwin/Reference/ \
  #    ManPages/man5/launchd.plist.5.html
  escaped_plist = cgi.escape(plistname)
  entries = [
    '<key>Label</key><string>%s</string>' % escaped_plist,
    '<key>StandardOutPath</key><string>%s.log</string>' % escaped_plist,
    '<key>StandardErrorPath</key><string>%s-err.log</string>' % escaped_plist,
    '<key>LimitLoadToSessionType</key><array><string>Aqua</string></array>',
    '<key>RunAtLoad</key><true/>',
    '<key>Umask</key><integer>18</integer>',

    '<key>EnvironmentVariables</key>',
    '<dict>',
    '  <key>PATH</key>',
    # TODO(maruel): Makes it configurable if necessary.
    '  <string>/opt/local/bin:/opt/local/sbin:/usr/local/sbin:/usr/local/bin'
      ':/usr/sbin:/usr/bin:/sbin:/bin</string>',
    '</dict>',

    '<key>SoftResourceLimits</key>',
    '<dict>',
    '  <key>NumberOfFiles</key>',
    '  <integer>8000</integer>',
    '</dict>',
  ]
  entries.append(
      '<key>Program</key><string>%s</string>' % cgi.escape(command[0]))
  entries.append('<key>ProgramArguments</key>')
  entries.append('<array>')
  # Command[0] must be passed as an argument.
  entries.extend('  <string>%s</string>' % cgi.escape(i) for i in command)
  entries.append('</array>')
  entries.append(
      '<key>WorkingDirectory</key><string>%s</string>' % cgi.escape(cwd))
  header = (
    '<?xml version="1.0" encoding="UTF-8"?>\n'
    '<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" '
    '"http://www.apple.com/DTDs/PropertyList-1.0.dtd">\n'
    '<plist version="1.0">\n'
    '  <dict>\n'
    + ''.join('    %s\n' % l for l in entries) +
    '  </dict>\n'
    '</plist>\n')
  return header


def get_gpu():
  """Returns video device as listed by 'system_profiler'. See get_gpu()."""
  dimensions = set()
  state = set()
  for card in _get_SPDisplaysDataType():
    # Warning: the value provided depends on the driver manufacturer.
    # Other interesting values: spdisplays_vram, spdisplays_revision-id
    ven_id = u'UNKNOWN'
    if 'spdisplays_vendor-id' in card:
      # NVidia
      ven_id = card['spdisplays_vendor-id'][2:]
    elif 'spdisplays_vendor' in card:
      # Intel and ATI
      match = re.search(r'\(0x([0-9a-f]{4})\)', card['spdisplays_vendor'])
      if match:
        ven_id = match.group(1)
    dev_id = card['spdisplays_device-id'][2:]
    dimensions.add(unicode(ven_id))
    dimensions.add(u'%s:%s' % (ven_id, dev_id))

    # VMWare doesn't set it.
    if 'sppci_model' in card:
      state.add(unicode(card['sppci_model']))
  return sorted(dimensions), sorted(state)


def get_monitor_hidpi():
  """Returns True if the monitor is hidpi."""
  hidpi = any(
    any(m.get('spdisplays_retina') == 'spdisplays_yes'
        for m in card['spdisplays_ndrvs'])
    for card in _get_SPDisplaysDataType()
    if 'spdisplays_ndrvs' in card)
  return str(int(hidpi))


@tools.cached
def _get_xcode_version(xcode_app):
  """Returns the version of Xcode installed at the given path.

  Args:
    xcode_app: Absolute path to the Xcode app directory, e.g
               /Applications/Xcode.app.

  Returns:
    A tuple of (Xcode version, Xcode build version), or None if
    this is not an Xcode installation.
  """
  xcodebuild = os.path.join(
      xcode_app, 'Contents', 'Developer', 'usr', 'bin', 'xcodebuild')
  if os.path.exists(xcodebuild):
    try:
      out = subprocess.check_output([xcodebuild, '-version']).splitlines()
    except subprocess.CalledProcessError:
      return None
    return out[0].split()[-1], out[1].split()[-1]


def get_xcode_state():
  """Returns the state of Xcode installations on this machine."""
  state = {}
  applications_dir = os.path.join('/Applications')
  for app in os.listdir(applications_dir):
    name, ext = os.path.splitext(app)
    if name.startswith('Xcode') and ext == '.app':
      xcode_app = os.path.join(applications_dir, app)
      version = _get_xcode_version(xcode_app)
      if version:
        state[xcode_app] = {
          'version': version[0],
          'build version': version[1],
        }
  return state


def get_xcode_versions():
  """Returns all Xcode versions installed."""
  return sorted(xcode['version'] for xcode in get_xcode_state().itervalues())

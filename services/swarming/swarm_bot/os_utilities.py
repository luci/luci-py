# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""OS specific utility functions.

Includes code:
- to declare the current system this code is running under.
- to run a command on user login.
- to restart the host.
"""

import cgi
import logging
import os
import platform
import socket
import subprocess
import sys
import time


### Private stuff.


def _write(filepath, content):
  """Writes out a file and returns True on success."""
  logging.info('Writing in %s:\n%s', filepath, content)
  try:
    with open(filepath, mode='wb') as f:
      f.write(content)
    return True
  except IOError as e:
    logging.error('Failed to write %s: %s', filepath, e)
    return False


def _from_cygwin_path(path):
  """Converts an absolute cygwin path to a standard Windows path."""
  if not path.startswith('/cygdrive/'):
    logging.error('%s is not a cygwin path', path)
    return None

  # Remove the cygwin path identifier.
  path = path[len('/cygdrive/'):]

  # Add : after the drive letter.
  path = path[:1] + ':' + path[1:]
  return path.replace('/', '\\')


def _to_cygwin_path(path):
  """Converts an absolute standard Windows path to a cygwin path."""
  if len(path) < 2 or path[1] != ':':
    # TODO(maruel): Accept \\?\ and \??\ if necessary.
    logging.error('%s is not a win32 path', path)
    return None
  return '/cygdrive/%s/%s' % (path[0].lower(), path[3:].replace('\\', '/'))


def _get_startup_dir_win():
  # Do not use environment variables since it wouldn't work reliably on cygwin.
  # TODO(maruel): Stop hardcoding the values and use the proper function
  # described below. Postponed to a later CL since I'll have to spend quality
  # time on Windows to ensure it works well.
  # https://msdn.microsoft.com/library/windows/desktop/bb762494.aspx
  # CSIDL_STARTUP = 7
  # https://msdn.microsoft.com/library/windows/desktop/bb762180.aspx
  # shell.SHGetFolderLocation(NULL, CSIDL_STARTUP, NULL, NULL, string)
  if get_os_version() == '5.1':
    startup = 'Start Menu\\Programs\\Startup'
  else:
    # Vista+
    startup = (
        'AppData\\Roaming\\Microsoft\\Windows\\Start Menu\\Programs\\Startup')

  # On cygwin 1.5, which is still used on some slaves, '~' points inside
  # c:\\cygwin\\home so use USERPROFILE.
  return '%s\\%s\\' % (
    os.environ.get('USERPROFILE', 'DUMMY, ONLY USED IN TESTS'), startup)


def _generate_launchd_plist(command, cwd, plistname):
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


### Public API.


def get_os_version():
  """Returns the normalized OS version as a string.

  Returns:
    The format depends on the OS:
    - Windows: 5.1, 6.1, etc.
    - OSX: 10.7, 10.8, etc.
    - Ubuntu: 12.04, 10.04, etc.
    Others will return None.
  """
  if sys.platform in ('cygwin', 'win32'):
    version = (
        platform.system() if sys.platform == 'cygwin' else platform.version())
    if '-' in version:
      version = version.split('-')[1]
    version_parts = version.split('.')
    assert len(version_parts) >= 2,  'Unable to determine Windows version'
    if version_parts[0] < 5 or (version_parts[0] == 5 and version_parts[1] < 1):
      assert False, 'Version before XP are unsupported: %s' % version_parts
    return '.'.join(version_parts[:2])

  if sys.platform == 'darwin':
    version_parts = platform.mac_ver()[0].split('.')
    assert len(version_parts) >= 2, 'Unable to determine Mac version'
    return '.'.join(version_parts[:2])

  if sys.platform == 'linux2':
    # Assumes Ubuntu here. Improve if needed. No need to convert the Ubuntu
    # value since it already returns what we want like '12.04' or '10.04'.
    distro_details = platform.linux_distribution()
    assert distro_details[0] == 'Ubuntu', distro_details
    return distro_details[1]

  logging.error('Unable to determine platform version')
  return None


def get_os_name():
  """Returns standardized OS name.

  Defaults to sys.platform for OS not normalized.

  TODO(maruel): Differentiate between linux distros like Ubuntu or debian.
  """
  return {
    'cygwin': 'Windows',
    'darwin': 'Mac',
    'linux2': 'Linux',
    'win32': 'Windows',
  }.get(sys.platform, sys.platform)


def get_cpu_type():
  """Returns the type of processor: arm or x86."""
  machine = platform.machine().lower()
  if machine in ('amd64', 'x86_64', 'i386'):
    return 'x86'
  return machine


def get_cpu_bitness():
  """Returns the number of bits in the CPU architecture as a str: 32 or 64.

  Unless someone ported python to PDP-10 or 286.

  Note: this function may return 32 bits on 64 bits OS in case of a 32 bits
  python process.
  """
  if platform.machine().endswith('64'):
    return '64'
  # TODO(maruel): Work harder to figure out if OS is 64 bits.
  return '64' if sys.maxsize > 2**32 else '32'


def get_attributes(tag):
  """Returns the default Swarming dictionary of attributes for this bot.

  'tag' is used to uniquely identify the bot.
  'dimensions' is used for task selection.
  """
  # Windows enjoys putting random case in there. Enforces lower case for sanity.
  hostname = socket.getfqdn().lower()
  os_name = get_os_name()
  cpu_type = get_cpu_type()
  return {
    'dimensions': {
      'hostname': hostname,
      'cpu': [
        cpu_type,
        cpu_type + '-' + get_cpu_bitness(),
      ],
      'os': [
        os_name,
        os_name + '-' + get_os_version(),
      ],
    },
    'tag': tag,
  }


def setup_auto_startup_win(command, cwd, batch_name):
  """Uses Startup folder in the Start Menu.

  This assumes the user is automatically logged in on OS startup.

  Works both inside cygwin's python or native python which makes this function a
  bit more tricky than necessary.

  Use the start up menu instead of registry for two reasons:
  - It's easy to remove in case of failure, for example in case of reboot loop.
  - It works well even with cygwin.

  TODO(maruel): This function assumes |command| is python script to be run.
  """
  logging.info('setup_auto_startup_win(%s, %s, %s)', command, cwd, batch_name)
  assert batch_name.endswith('.bat'), batch_name
  batch_path = _get_startup_dir_win() + batch_name

  # If we are running through cygwin, the path to write to must be changed to be
  # in the cywgin format, but we also need to change the commands to be in
  # non-cygwin format (since they will execute in a batch file).
  if sys.platform == 'cygwin':
    batch_path = _to_cygwin_path(batch_path)
    assert batch_path
    cwd = _from_cygwin_path(cwd)
    assert cwd

    # Convert all the cygwin paths in the command.
    for i in range(len(command)):
      if '/cygdrive/' in command[i]:
        command[i] = _from_cygwin_path(command[i])

  # TODO(maruel): Shell escape! Sadly shlex.quote() is only available starting
  # python 3.3 and it's tricky on Windows with '^'.
  # Don't forget the CRLF, otherwise cmd.exe won't process it.
  content = (
      '@echo off\r\n'
      ':: This file was generated automatically by os_utilities.py.\r\n'
      'cd /d %s\r\n'
      '%s\r\n') % (cwd, ' '.join(command))
  return _write(batch_path, content)


def setup_auto_startup_osx(command, cwd, plistname):
  """Uses launchd to start the command when the user logs in.

  This assumes the user is automatically logged in on OS startup.

  In case of failure like reboot loop, simply remove the file in
  ~/Library/LaunchAgents/.
  """
  logging.info('setup_auto_startup_osx(%s, %s, %s)', command, cwd, plistname)
  assert plistname.endswith('.plist'), plistname
  launchd_dir = os.path.expanduser('~/Library/LaunchAgents')
  if not os.path.isdir(launchd_dir):
    # This directory doesn't exist by default.
    os.mkdir(launchd_dir)
  filepath = os.path.join(launchd_dir, plistname)
  return _write(filepath, _generate_launchd_plist(command, cwd, plistname))


def restart():
  """Restarts this machine.

  If it fails to reboot the host, it loops. This function never return.
  """
  while True:
    restart_and_return()
    # Sleep for 300 seconds to ensure we don't try to do anymore work while the
    # OS is preparing to shutdown.
    time.sleep(300)


def restart_and_return():
  """Tries to restart this host and immediately return to the caller.

  This is mostly useful when done via remote shell, like via ssh, where it is
  not worth waiting for the TCP connection to tear down.

  Returns:
    True if at least one command succeeded.
  """
  if sys.platform == 'win32':
    cmds = []
  elif sys.platform == 'cygwin':
    # The one that will succeed depends if it is executed via a prompt or via
    # a ssh command. #itscomplicated.
    cmds = [
      ['shutdown', '-r', '-f', '-t', '1'],
      ['shutdown', '-r', '-f', '1'],
    ]
  elif sys.platform == 'linux2' or sys.platform == 'darwin':
    cmds = [['sudo', '/sbin/shutdown', '-r', 'now']]
  else:
    cmds = [['sudo', 'shutdown', '-r', 'now']]

  success = False
  for cmd in cmds:
    logging.info('Restarting machine with command %s', ' '.join(cmd))
    try:
      subprocess.check_call(cmd)
    except (OSError, subprocess.CalledProcessError) as e:
      logging.error('Failed to run %s: %s', ' '.join(cmd), e)
    else:
      success = True
  return success

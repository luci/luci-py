# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""GNU/Linux specific utility functions."""

import os
import pipes
import platform
import re
import shlex
import subprocess

from utils import tools


@tools.cached
def get_os_version_number():
  """Returns the normalized OS version number as a string.

  Returns:
    - 12.04, 10.04, etc.
  """
  # On Ubuntu it will return a string like '12.04'. On Raspbian, it will look
  # like '7.6'.
  return unicode(platform.linux_distribution()[1])


@tools.cached
def _lspci():
  """Returns list of PCI devices found.

  list(Bus, Type, Vendor [ID], Device [ID], extra...)
  """
  try:
    lines = subprocess.check_output(
        ['lspci', '-mm', '-nn'], stderr=subprocess.PIPE).splitlines()
  except (OSError, subprocess.CalledProcessError):
    # It normally happens on Google Compute Engine as lspci is not installed by
    # default and on ARM since they do not have a PCI bus.
    return None
  return [shlex.split(l) for l in lines]


def get_temperatures():
  """Returns the temperatures measured via thermal_zones

  Returns:
    - dict of temperatures found on device in degrees C
      (e.g. {'thermal_zone0': 43.0, 'thermal_zone1': 46.117})
  """
  temps = {}
  for filename in os.listdir('/sys/class/thermal/'):
    if re.match(r'^thermal_zone\d+', filename):
      try:
        with open(os.path.join('/sys/class/thermal', filename, 'temp'),
                  'rb') as f:
          # Convert from milli-C to Celsius
          temps[filename] = int(f.read()) / 1000.0
      except (IOError, OSError):
        pass
  return temps


@tools.cached
def get_audio():
  """Returns information about the audio."""
  pci_devices = _lspci()
  if not pci_devices:
    return None
  # Join columns 'Vendor' and 'Device'. 'man lspci' for more details.
  return [
    u': '.join(l[2:4]) for l in pci_devices if l[1] == 'Audio device [0403]'
  ]


@tools.cached
def get_gpu():
  """Returns video device as listed by 'lspci'. See get_gpu().
  """
  pci_devices = _lspci()
  if not pci_devices:
    # It normally happens on Google Compute Engine as lspci is not installed by
    # default and on ARM since they do not have a PCI bus. In either case, we
    # don't care about the GPU.
    return None, None

  dimensions = set()
  state = set()
  re_id = re.compile(r'^(.+?) \[([0-9a-f]{4})\]$')
  for line in pci_devices:
    # Look for display class as noted at http://wiki.osdev.org/PCI
    dev_type = re_id.match(line[1]).group(2)
    if dev_type.startswith('03'):
      vendor = re_id.match(line[2])
      device = re_id.match(line[3])
      ven_id = vendor.group(2)
      dimensions.add(unicode(ven_id))
      dimensions.add(u'%s:%s' % (ven_id, device.group(2)))
      state.add(u'%s %s' % (vendor.group(1), device.group(1)))
  return sorted(dimensions), sorted(state)


def generate_initd(command, cwd, user):
  """Returns a valid init.d script for use for Swarming.

  Author is lazy so he copy-pasted.
  Source: https://github.com/fhd/init-script-template

  Copyright (C) 2012-2014 Felix H. Dahlke
  This is open source software, licensed under the MIT License. See the file
  LICENSE for details.

  LICENSE is at https://github.com/fhd/init-script-template/blob/master/LICENSE
  """
  return """#!/bin/sh
### BEGIN INIT INFO
# Provides:
# Required-Start:    $remote_fs $syslog
# Required-Stop:     $remote_fs $syslog
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Start daemon at boot time
# Description:       Enable service provided by daemon.
### END INIT INFO

dir='%(cwd)s'
user='%(user)s'
cmd='%(cmd)s'

name=`basename $0`
pid_file="/var/run/$name.pid"
stdout_log="/var/log/$name.log"
stderr_log="/var/log/$name.err"

get_pid() {
  cat "$pid_file"
}

is_running() {
  [ -f "$pid_file" ] && ps `get_pid` > /dev/null 2>&1
}

case "$1" in
  start)
  if is_running; then
    echo "Already started"
  else
    echo "Starting $name"
    cd "$dir"
    sudo -u "$user" $cmd >> "$stdout_log" 2>> "$stderr_log" &
    echo $! > "$pid_file"
    if ! is_running; then
      echo "Unable to start, see $stdout_log and $stderr_log"
      exit 1
    fi
  fi
  ;;
  stop)
  if is_running; then
    echo -n "Stopping $name.."
    kill `get_pid`
    for i in {1..10}
    do
      if ! is_running; then
        break
      fi

      echo -n "."
      sleep 1
    done
    echo

    if is_running; then
      echo "Not stopped; may still be shutting down or shutdown may have failed"
      exit 1
    else
      echo "Stopped"
      if [ -f "$pid_file" ]; then
        rm "$pid_file"
      fi
    fi
  else
    echo "Not running"
  fi
  ;;
  restart)
  $0 stop
  if is_running; then
    echo "Unable to stop, will not attempt to start"
    exit 1
  fi
  $0 start
  ;;
  status)
  if is_running; then
    echo "Running"
  else
    echo "Stopped"
    exit 1
  fi
  ;;
  *)
  echo "Usage: $0 {start|stop|restart|status}"
  exit 1
  ;;
esac

exit 0
""" % {
      'cmd': ' '.join(pipes.quote(c) for c in command),
      'cwd': pipes.quote(cwd),
      'user': pipes.quote(user),
    }


def generate_autostart_destkop(command, name):
  """Returns a valid .desktop for use with Swarming bot.

  http://standards.freedesktop.org/desktop-entry-spec/desktop-entry-spec-latest.html
  """
  return (
    '[Desktop Entry]\n'
    'Type=Application\n'
    'Name=%(name)s\n'
    'Exec=%(cmd)s\n'
    'Hidden=false\n'
    'NoDisplay=false\n'
    'Comment=Created by os_utilities.py in swarming_bot.zip\n'
    'X-GNOME-Autostart-enabled=true\n') % {
      'cmd': ' '.join(pipes.quote(c) for c in command),
      'name': name,
    }

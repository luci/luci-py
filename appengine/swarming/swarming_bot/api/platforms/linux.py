# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""GNU/Linux specific utility functions."""

from __future__ import absolute_import

import ctypes
import ctypes.util
import logging
import multiprocessing
import os
import pipes
import re
import shlex
import subprocess

import distro

from utils import tools

from api.platforms import common
from api.platforms import gpu


## Private stuff.

_RESOLUTION_REGEX = re.compile(
    r' connected primary (?P<horizontal>\d+)x(?P<vertical>\d+)\+')


libc = ctypes.cdll.LoadLibrary(ctypes.util.find_library('c'))

cpu_mask_t = ctypes.c_ulong

CPU_SETSIZE = 1024
NCPUBITS = 8 * ctypes.sizeof(cpu_mask_t)

pid_t = ctypes.c_uint64

class cpu_set_t(ctypes.Structure):
  _fields_ = [('__bits', cpu_mask_t * (CPU_SETSIZE // NCPUBITS))]

  def get_cpus(self):
    """Convert bits in list len == CPU_SETSIZE
    Use 1 / 0 per cpu
    """
    cpus = []
    for bitmask in getattr(self, '__bits'):
      for i in range(NCPUBITS):
        if bitmask & 1:
          cpus.append(i)
        bitmask >>= 1
        if not bitmask:
          break
    return cpus


@tools.cached
def get_num_processors():
  # Multiprocessing cpu_count() returns number of processors on the system,
  # not the number of processors process can actually use. These numbers
  # may differ for example in Docker containers. Use sched_getaffinity to
  # find out the number of usable processors instead.
  cpu_set = cpu_set_t()
  err = libc.sched_getaffinity(
      pid_t(os.getpid()),
      ctypes.sizeof(cpu_set_t),
      ctypes.pointer(cpu_set))
  if err != 0:
    # This is not a big deal, fallback onto multiprocessing. This happens on
    # MIPS.
    return multiprocessing.cpu_count()
  return len(cpu_set.get_cpus())


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
  return [shlex.split(l.decode()) for l in lines]


@tools.cached
def _get_nvidia_version():
  try:
    with open('/sys/module/nvidia/version') as f:
      # Looks like '367.27'.
      return f.read().strip()
  except (IOError, OSError):
    return None


@tools.cached
def _get_mesa_version():
  """Retrieves the Mesa DRI driver version, which is usable as the Intel and AMD
  GPU driver versions.
  """
  try:
    out = subprocess.check_output(['dpkg', '-s', 'libgl1-mesa-dri']).decode()
    # Looks like 'Version: 17.2.8-0ubuntu0~17.10.1', and we need '17.2.8'.
    for line in out.splitlines():
      match = re.match(r'^Version: (\d+.\d+.\d+)', line)
      if match:
        return match.group(1)
    return None
  except (OSError, subprocess.CalledProcessError):
    return None


def _read_cpuinfo():
  try:
    with open('/proc/cpuinfo', 'r') as f:
      return f.read()
  except (IOError, OSError):
    return ''


def _read_cgroup():
  with open('/proc/self/cgroup', 'r') as f:
    return f.read()


def _read_dmi_file(filename):
  try:
    with open('/sys/devices/virtual/dmi/id/' + filename, 'r') as f:
      return f.read().strip()
  except (IOError, OSError):
    return None


## Public API.


@tools.cached
def get_os_version_number():
  """Returns the normalized OS version number as a string.

  Returns:
    - 12.04, 10.04, etc.
  """
  if distro.id() == 'debian':
    # distro doesn't show minor version for debian.
    with open('/etc/debian_version') as f:
      return f.read().strip()
  # On Ubuntu it will return a string like '12.04'. On Raspbian, it will look
  # like '7.6'.
  return distro.version(best=True)


def get_temperatures():
  """Returns the temperatures measured via thermal_zones

  Returns:
    - dict of temperatures found on device in degrees C
      (e.g. {'thermal_zone0': 43.0, 'thermal_zone1': 46.117})
  """
  temps = {}
  path = '/sys/class/thermal'
  if os.path.isdir(path):
    for filename in os.listdir(path):
      if re.match(r'^thermal_zone\d+', filename):
        try:
          with open(os.path.join(path, filename, 'temp'), 'rb') as f:
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
      ': '.join(l[2:4]) for l in pci_devices if l[1] == 'Audio device [0403]'
  ]


@tools.cached
def get_cpuinfo():
  values = common._safe_parse(_read_cpuinfo())
  cpu_info = {}
  if 'vendor_id' in values and 'flags' in values:
    # Intel.
    cpu_info['flags'] = values['flags']
    cpu_info['model'] = [
        int(values['cpu family']),
        int(values['model']),
        int(values['stepping']),
        int(values['microcode'], 0),
    ]
    cpu_info['name'] = values['model name']
    cpu_info['vendor'] = values['vendor_id']
  elif 'mips' in values.get('isa', ''):
    # MIPS.
    cpu_info['flags'] = values['isa']
    cpu_info['name'] = values['cpu model']
  elif "POWER" in values.get('cpu', ''):
    # ppc64/ppc64le
    cpu_info['name'] = values.get('cpu', '').split(' ')[0]
  elif "S390" in values.get('vendor_id', ''):
    # s390x
    cpu_info['name'] = values.get('vendor_id', '').split('/')[1]
  elif values:
    # CPU implementer == 0x41 means ARM.
    if 'Features' in values:
      cpu_info['flags'] = values['Features']
    if 'CPU variant' in values:
      cpu_info['model'] = (
          int(values['CPU variant'], 0),
          int(values.get('CPU part', 0), 0),
          int(values.get('CPU revision', 0)),
      )
    # ARM CPUs have a serial number embedded. Intel did try on the Pentium III
    # but gave up after backlash;
    # http://www.wired.com/1999/01/intel-on-privacy-whoops/
    # http://www.theregister.co.uk/2000/05/04/intel_processor_serial_number_q/
    # It is very ironic that ARM based manufacturers are getting away with.
    if 'Serial' in values:
      cpu_info['serial'] = values['Serial'].lstrip('0')
    if 'Revision' in values:
      cpu_info['revision'] = values['Revision']

    # 'Hardware' field has better content so use it instead of 'model name' /
    # 'Processor' field.
    if 'Hardware' in values:
      cpu_info['name'] = values['Hardware']
      # Samsung felt this was useful information. Strip that.
      suffix = ' (Flattened Device Tree)'
      if cpu_info['name'].endswith(suffix):
        cpu_info['name'] = cpu_info['name'][:-len(suffix)]
    # SAMSUNG EXYNOS5 uses 'Processor' instead of 'model name' as the key for
    # its name <insert exasperation meme here>.
    cpu_info['vendor'] = (values.get('model name') or values.get('Processor')
                          or 'N/A')

  # http://unix.stackexchange.com/questions/43539/what-do-the-flags-in-proc-cpuinfo-mean
  if 'flags' in cpu_info:
    cpu_info['flags'] = sorted(i for i in cpu_info['flags'].split())
  return cpu_info


def get_gpu():
  """Returns video device as listed by 'lspci'. See get_gpu().

  Not cached as the GPU driver may change underneat.
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
    m = re_id.match(line[1])
    if not m:
      continue
    dev_type = m.group(2)
    if not dev_type or not dev_type.startswith('03'):
      continue
    vendor = re_id.match(line[2])
    device = re_id.match(line[3])
    if not vendor or not device:
      continue
    ven_name = vendor.group(1)
    ven_id = vendor.group(2)
    dev_name = device.group(1)
    dev_id = device.group(2)

    version = ''
    if ven_id == gpu.NVIDIA:
      version = _get_nvidia_version()
    elif ven_id in (gpu.INTEL, gpu.AMD):
      version = _get_mesa_version()
    ven_name, dev_name = gpu.ids_to_names(ven_id, ven_name, dev_id, dev_name)

    dimensions.add(ven_id)
    dimensions.add('%s:%s' % (ven_id, dev_id))
    if version:
      dimensions.add('%s:%s-%s' % (ven_id, dev_id, version))
      state.add('%s %s %s' % (ven_name, dev_name, version))
    else:
      state.add('%s %s' % (ven_name, dev_name))
  return sorted(dimensions), sorted(state)


def get_uptime():
  """Returns uptime in seconds since system startup.

  Includes sleep time.
  """
  try:
    with open('/proc/uptime', 'rb') as f:
      return float(f.read().split()[0])
  except (IOError, OSError, ValueError):
    return 0.


def get_reboot_required():
  """Returns True if the system should be rebooted to apply updates.

  This only checks /var/run/reboot-required, which not all distros support and
  which is not set for all conditions requiring reboot.
  """
  return os.path.exists('/var/run/reboot-required')


def is_display_attached():
  """Returns whether a display is attached to the machine or not.

  Returns:
    None, True, or False. It is None when the presence of a display cannot be
    determined, and a bool otherwise returning whether a display is attached.
  """
  try:
    # Since this uses xrandr, an alternative method will need to be added
    # whenever Wayland is planned to be used for testing.
    out = subprocess.check_output(['xrandr', '--display', ':0.0', '--query'],
                                  universal_newlines=True)
  except (OSError, subprocess.CalledProcessError) as e:
    # Could happen when the host is shutting down or lshw is not available
    # for some reason.
    logging.error('is_display_attached(): %s', e)
    return None

  # When a display is properly attached, there should be a monitor listed as
  # connected and set as primary.
  for line in out.splitlines():
    if ' connected primary ' in line:
      return True
  return False


def get_display_resolution():
  """Gets the resolution of the attached display.

  Returns:
    None or a tuple (horizontal, vertical). It is None when the resolution
    cannot be determined, e.g. if a display is not attched. Otherwise,
    |horizontal| and |vertical| are ints specifying the horizontal and vertical
    resolution of the display.
  """
  try:
    # Since this uses xrandr, an alternative method will need to be added
    # whenever Wayland is planned to be used for testing.
    out = subprocess.check_output(['xrandr', '--display', ':0.0', '--query'],
                                  universal_newlines=True)
  except (OSError, subprocess.CalledProcessError) as e:
    # Could happen when the host is shutting down or xrandr is not available
    # for some reason.
    logging.error('get_display_resolution(): %s', e)
    return None

  # When a display is properly attached, there should be a monitor listed as
  # connected and set as primary with the current resolution, e.g.
  #   DisplayPort-2 connected primary 2560x1440+0+518 (normal...
  for line in out.splitlines():
    match = _RESOLUTION_REGEX.search(line)
    if not match:
      continue
    return int(match.group('horizontal')), int(match.group('vertical'))

  return None


@tools.cached
def get_ssd():
  """Returns a list of SSD disks."""
  try:
    out = subprocess.check_output(['lsblk', '-d', '-o', 'name,rota'],
                                  universal_newlines=True).splitlines()
    ssd = []
    for line in out:
      match = re.match(r'(\w+)\s+(0|1)', line)
      if match and match.group(2) == '0':
        ssd.append(match.group(1))
    return tuple(sorted(ssd))
  except (OSError, subprocess.CalledProcessError) as e:
    logging.error('Failed to read disk info: %s', e)
    return ()


@tools.cached
def get_kernel():
  """Returns the currently running kernel version string."""
  return os.uname().release


@tools.cached
def get_kvm():
  """Check whether KVM is available."""
  # We only check the file existence, not whether we can access it. This avoids
  # the race condition between swarming_bot and udev which is responsible for
  # setting the correct ACL of /dev/kvm.
  return os.path.exists('/dev/kvm')


@tools.cached
def get_inside_docker():
  """Returns a strings representing if running inside docker, and which type.

  Returns:
    - None if not run in docker.
    - 'stock' if running in standard docker.
    - 'nvidia' if running in nvidia-docker.
  """
  if ':/k8s.io' in _read_cgroup():
    return 'stock'
  if not os.path.isfile('/.docker_env') and not os.path.isfile('/.dockerenv'):
    return None
  # TODO(maruel): Detect nvidia-docker.
  return 'stock'


@tools.cached
def get_device_tree_compatible():
  """Returns the devicetree/compatible data, if available.

  This is generally used on ARM based hardware.
  """
  try:
    with open('/sys/firmware/devicetree/base/compatible', 'rb') as f:
      items = f.read().strip().split(b',')
  except IOError:
    return None
  # The data could contain nul byte or other invalid data, we've observed this
  # on ODROID-C2.
  for i, item in enumerate(items):
    if b'\x00' in item:
      items[i] = None
    else:
      try:
        items[i] = item.decode('utf-8', 'replace')
      except UnicodeDecodeError:
        items[i] = None
  return sorted(i for i in items if i)


def get_cpu_scaling_governor(cpu_num):
  """Returns the current CPU scaling governor, if available."""
  files = [
      '/sys/devices/system/cpu/cpu%d/cpufreq/scaling_governor' % cpu_num,
      '/sys/devices/system/cpu/cpufreq/policy%d/scaling_governor' % cpu_num,
  ]
  for p in files:
    try:
      with open(p) as f:
        return [f.read().strip()]
    except IOError:
      continue
  return None


## Mutating code.


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
# Provides:          swarming
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


def generate_autostart_desktop(command, name):
  """Returns a valid .desktop for use with Swarming bot.

  http://standards.freedesktop.org/desktop-entry-spec/desktop-entry-spec-latest.html
  """
  return ('[Desktop Entry]\n'
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


@tools.cached
def get_computer_system_info():
  """Return a named tuple, which lists the following params:

  name, vendor, version, uuid
  """
  return common.ComputerSystemInfo(
      name=_read_dmi_file('product_name'),
      vendor=_read_dmi_file('sys_vendor'),
      version=_read_dmi_file('product_version'),
      serial=_read_dmi_file('product_serial'))

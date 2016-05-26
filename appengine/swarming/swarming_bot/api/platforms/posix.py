# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""POSIX specific utility functions."""

import subprocess
import sys


def _run_df():
  """Runs df and returns the output."""
  proc = subprocess.Popen(
      ['/bin/df', '-k', '-P'], env={'LANG': 'C'},
      stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  for l in proc.communicate()[0].splitlines():
    l = l.decode('utf-8')
    if l.startswith(u'/dev/'):
      items = l.split()
      if (sys.platform == 'darwin' and
          items[5].startswith(u'/Volumes/firmwaresyncd.')):
        # There's an issue on OSX where sometimes a small volume is mounted
        # during boot time and may be caught here by accident. Just ignore it as
        # it could trigger the low free disk space check and cause an unexpected
        # bot self-quarantine.
        continue
      yield items


def get_disks_info():
  """Returns disks info on all mount point in Mb."""
  return {
    items[5]: {
      u'free_mb': round(float(items[3]) / 1024., 1),
      u'size_mb': round(float(items[1]) / 1024., 1),
    }
    for items in _run_df()
  }

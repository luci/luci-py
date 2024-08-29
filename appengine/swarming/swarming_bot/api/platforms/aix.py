# Copyright 2024 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import ctypes

from utils import tools

# AIX getsystemcfg enum value for CPU identification
_SC_IMPL = 2

# Known AIX cpu mappings
_impl_map = {
    0x10000: "POWER8",
    0x20000: "POWER9",
    0x40000: "POWER10",
}


def _getsystemcfg(num):
  libc = ctypes.CDLL('/lib/libc.a(shr_64.o)')
  val = libc.getsystemcfg(ctypes.c_int(num))
  return val


@tools.cached
def get_cpuinfo():
  sc_impl = _getsystemcfg(_SC_IMPL)
  cpu_info = {}
  if sc_impl in _impl_map:
    cpu_info['name'] = _impl_map[sc_impl]
  return cpu_info

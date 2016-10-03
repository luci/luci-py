# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""GPU specific utility functions."""


AMD = u'1002'
MAXTROX = u'102b'
NVIDIA = u'10de'
INTEL = u'8086'


_VENDOR_MAPPING = {
  AMD: (
    u'AMD',
    {
      # http://developer.amd.com/resources/ati-catalyst-pc-vendor-id-1002-li/
      u'6613': u'Radeon R7 240',   # The table is incorrect
      u'6821': u'Radeon R8 M370X', # 'HD 8800M' or 'R7 M380' based on rev_id
      u'683d': u'Radeon HD 7700',
    }),
  INTEL: (
    u'Intel',
    {
      # http://downloadmirror.intel.com/23188/eng/config.xml
      u'0166': u'HD Graphics 4000',
      u'0412': u'HD Graphics 4600',
      u'0a2e': u'Iris Graphics 5100',
      u'0d26': u'Iris Pro Graphics 5200',
      u'1912': u'HD Graphics 530',
    }),
  MAXTROX: (
    u'Matrox',
    {
      u'0534': u'G200eR2',
    }),
  NVIDIA: (
    u'Nvidia',
    {
      # ftp://download.nvidia.com/XFree86/Linux-x86_64/352.21/README/README.txt
      u'06fa': u'Quadro NVS 450',
      u'08a4': u'GeForce 320M',
      u'0df8': u'Quadro 600',
      u'0fd5': u'GeForce GT 650M',
      u'0fe9': u'GeForce GT 750M',
      u'104a': u'GeForce GT 610',
      u'11c0': u'GeForce GTX 660',
      u'1244': u'GeForce GTX 550 Ti',
      u'1401': u'GeForce GTX 960',
    }),
}


def ids_to_names(ven_id, ven_name, dev_id, dev_name):
  """Uses an internal lookup table to return canonical names when known.

  Returns:
    tuple(vendor name, device name).
  """
  m = _VENDOR_MAPPING.get(ven_id)
  if not m:
    return ven_name, dev_name
  return m[0], m[1].get(dev_id, dev_name)

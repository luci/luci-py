# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""GPU specific utility functions."""



AMD = '1002'
ASPEED = '1a03'
INTEL = '8086'
MAXTROX = '102b'
NVIDIA = '10de'


_VENDOR_MAPPING = {
    AMD: (
        'AMD',
        {
            # http://developer.amd.com/resources/ati-catalyst-pc-vendor-id-1002-li/
            '6613': 'Radeon R7 240',  # The table is incorrect
            '6646': 'Radeon R9 M280X',
            '6779': 'Radeon HD 6450/7450/8450',
            '679e': 'Radeon HD 7800',
            '67ef': 'Radeon RX 560',
            '6821':
            'Radeon R8 M370X',  # 'HD 8800M' or 'R7 M380' based on rev_id
            '683d': 'Radeon HD 7700',
            '9830': 'Radeon HD 8400',
            '9874': 'Carrizo',
        }),
    ASPEED: (
        'ASPEED',
        {
            # https://pci-ids.ucw.cz/read/PC/1a03/2000
            # It seems all ASPEED graphics cards use the same device id
            # (for driver reasons?)
            '2000': 'ASPEED Graphics Family',
        }),
    INTEL: (
        'Intel',
        {
            # http://downloadmirror.intel.com/23188/eng/config.xml
            '0046': 'Ironlake HD Graphics',
            '0102': 'Sandy Bridge HD Graphics 2000',
            '0116': 'Sandy Bridge HD Graphics 3000',
            '0166': 'Ivy Bridge HD Graphics 4000',
            '0412': 'Haswell HD Graphics 4600',
            '041a': 'Haswell HD Graphics',
            '0a16': 'Intel Haswell HD Graphics 4400',
            '0a26': 'Haswell HD Graphics 5000',
            '0a2e': 'Haswell Iris Graphics 5100',
            '0d26': 'Haswell Iris Pro Graphics 5200',
            '0f31': 'Bay Trail HD Graphics',
            '1616': 'Broadwell HD Graphics 5500',
            '161e': 'Broadwell HD Graphics 5300',
            '1626': 'Broadwell HD Graphics 6000',
            '162b': 'Broadwell Iris Graphics 6100',
            '1912': 'Skylake HD Graphics 530',
            '191e': 'Skylake HD Graphics 515',
            '1926': 'Skylake Iris 540/550',
            '193b': 'Skylake Iris Pro 580',
            '22b1': 'Braswell HD Graphics',
            '3e92': 'Coffee Lake UHD Graphics 630',
            '5912': 'Kaby Lake HD Graphics 630',
            '591e': 'Kaby Lake HD Graphics 615',
            '5926': 'Kaby Lake Iris Plus Graphics 640',
        }),
    MAXTROX: ('Matrox', {
        '0522': 'MGA G200e',
        '0532': 'MGA G200eW',
        '0534': 'G200eR2',
    }),
    NVIDIA: (
        'Nvidia',
        {
            # pylint: disable=line-too-long
            # ftp://download.nvidia.com/XFree86/Linux-x86_64/352.21/README/README.txt
            '06fa': 'Quadro NVS 450',
            '08a4': 'GeForce 320M',
            '08aa': 'GeForce 320M',
            '0a65': 'GeForce 210',
            '0df8': 'Quadro 600',
            '0fd5': 'GeForce GT 650M',
            '0fe9': 'GeForce GT 750M Mac Edition',
            '0ffa': 'Quadro K600',
            '104a': 'GeForce GT 610',
            '11c0': 'GeForce GTX 660',
            '1244': 'GeForce GTX 550 Ti',
            '1401': 'GeForce GTX 960',
            '1ba1': 'GeForce GTX 1070',
            '1cb3': 'Quadro P400',
            '2184': 'GeForce GTX 1660',
        }),
}


def vendor_name_to_id(ven_name):
  """Uses an internal lookup table to return the vendor ID for known
  vendor names.

  Returns:
    a string, or None if unknown.
  """
  # macOS 10.13 doesn't provide the vendor ID any more, so support reverse
  # lookups on vendor name.
  lower_name = ven_name.lower()
  for k, v in _VENDOR_MAPPING.items():
    if lower_name == v[0].lower():
      return k
  return None


def ids_to_names(ven_id, ven_name, dev_id, dev_name):
  """Uses an internal lookup table to return canonical names when known.

  Returns:
    tuple(vendor name, device name).
  """
  m = _VENDOR_MAPPING.get(ven_id)
  if not m:
    return ven_name, dev_name
  return m[0], m[1].get(dev_id, dev_name)

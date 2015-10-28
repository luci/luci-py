#!/usr/bin/env python
# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import logging
import sys
import unittest

import test_env_platforms
test_env_platforms.setup_test_env()

import android
from adb import sign_pythonrsa


class MockDevice(object):
  def __init__(self, cmds):
    super(MockDevice, self).__init__()
    self._cmds = cmds[:]

  def shell(self, cmd):
    data = self._cmds.pop(0)
    assert data[0] == cmd, (data, cmd)
    return data[1], 0


RAW_IMEI = """Result: Parcel(
  0x00000000: 00000000 0000000f 00350033 00320035 '........3.5.5.2.'
  0x00000010: 00360033 00350030 00360038 00350038 '3.6.0.5.8.6.8.5.'
  0x00000020: 00390038 00000034                   '8.9.4...        ')
"""

class TestAndroid(unittest.TestCase):
  def test_get_imei(self):
    device = MockDevice(
        [
          ('dumpsys iphonesubinfo', ''),
          ('service call iphonesubinfo 1', RAW_IMEI),
        ])
    self.assertEqual(
        u'355236058685894', android.HighDevice(device, None).get_imei())


class PythonRSASignerTest(unittest.TestCase):
  # openssl genrsa 2048 | openssl pkcs8 -topk8 -inform pem -outform pem -nocrypt
  TEST_KEY = r"""
-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDkX+itb6GgmKSI
9+1vnYCZvm8edqB9DSVuQUnKjougISl6m74U5YR0dsonpgaHttAEtlwCpY8gS5tA
Hzbr3kLXPoM+XxmxRwklfY0hlQm/Jfksag5W074TDzEe+0tWnY4xKJepi47tWTap
HPYAgqHCUmLnHIE5oMbaew4LjjRZYD3hcs0JtiSEN2rf+gLuhfowTPOkIsBZMvM1
KicRXX3dUna+BIJIP7QRqW1RDPYD0O7LVI4qh7lQ0AQb82REXILwJ9DprNBKojFt
RdIRLzTYPJOrNCY89k/xY/baXvdoUUvOB9uY2QLS1yZGVZy/UIlRF5dMkvqEu35e
vCFrEYXRAgMBAAECggEAHaP9FRWaCrgFEunq3UO5/zpiPbfz2IcuRoMeyrV5lcz4
DzvLjfbEHEGWt1KrIk4t7Y6lnopSa0Sk38utWyu7zSgF7MB3GzU3ELCc8rDfVPne
v078kXRmBR9Bpt3Cx3knjWXRWWYNpOyhVY22uBY8XHZI8+oVM5+Ub6LtDb7lOHww
eBYVM6K4NuSx51cETkielg/olRGJi5Bw0XmvKt6Mvy7CWjcim6R/SLj2/mdzg+0c
f33KgT8xaXdGP4laIwiuHiZz5JIW+D0onDvIx6Ahx+GXaomJHW0XIuRWMpsmZ8fc
GiYhDcsXKCNz8yDTPuQxMSvt0ER/6UMKh7E17U/NyQKBgQD8yoDPli0uEpXUouli
poxe6J2pPZY/edI4FhbgYsgcYwBCcF7z0ranpO1nu+87QgwFKpH3HLRdDVpEHLws
JHok6msY6Fspge7RUrwUEgBZaqD9zVuJj/bC9zfNS4jNrbXbsWQHCBXvPhsToRRt
nHTikJ4e9KLuYNHZEqsePrztQwKBgQDnRg9D4qc+gZ14Qu+mrsGpSNBBw3uhhiwr
zgpojcl1h7LjUI0nU1AEn4eaN676KJ6tasIrN6sxJUhfiy0OdaJoaSiVPjwOXayM
TbCoC1CF1WYnxSNxMAgkLp3GB+doa/zF8lFFEXMsCt/x8X7e2PVKftSh7ko1+0D9
4P3DHL+lWwKBgHgkGgIOopxR8umjH35mHzKAJWYowf6a/CDxxp+P8wEiwB3TYB1L
WMnmJXp2pCUqp6HQ8JxcBRBwZyUV7wyfrXlb+9hTnfflK8ZKJt73Czlz5t2Yutdv
F1zjt56XNoZh95tY/GwnGJ2ii6XRmW7au+Ztyuh1ZfYbB8C+EZuHP60XAoGATdKo
blqFlqX8/CviAr/JkRJcadTC9F523KKvo/EaOn9YNd+0L+h8A6I++ikEq08h3g8P
mYVZCOeXy/bESZpR9Tp1morfoKHq3yeBa3qrNO3TO0y+GWdlRe+dzbZ5Kw0zeNSK
fmhZzhc2tm5iF9D/8XpuSLMrq7CJdSTRn2c8IgMCgYBf0Mu0o/CVmHEm3C7ipl80
V1RyfpJh+YlmGqPq7mbO0m4I1puyOhhoy0jnaxsROiTxeCEzn12y2Gs63Lo4GPKF
oShjkmSqylt4CG34EcCl+JkRTZVFsvp4f8gxC1yQasPo9xjB3DC7maanjp70uMLw
+wAUeW7AMn5lsETmXc+Reg==
-----END PRIVATE KEY-----
""".strip()

  def test_sign(self):
    # Does not crash => works.
    signer = sign_pythonrsa.PythonRSASigner('pub', self.TEST_KEY)
    self.assertTrue(signer.Sign('data to sign'))


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()

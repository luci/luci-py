#!/usr/bin/env python
# Copyright 2014 Google Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""ADB debugging binary.

Call it similar to how you call android's adb. Takes either --serial or
--port_path to connect to a device.
"""
from __future__ import absolute_import
from __future__ import print_function

import os
import sys

import gflags

from . import adb_commands
from . import common_cli

try:
  from . import sign_m2crypto
  rsa_signer = sign_m2crypto.M2CryptoSigner
except ImportError:
  try:
    from . import sign_pythonrsa
    rsa_signer = sign_pythonrsa.PythonRSASigner.FromRSAKeyPath
  except ImportError:
    rsa_signer = None


gflags.ADOPT_module_key_flags(common_cli)

gflags.DEFINE_multistring('rsa_key_path', '~/.android/adbkey',
                         'RSA key(s) to use')
gflags.DEFINE_integer('auth_timeout_s', 60,
                     'Seconds to wait for the dialog to be accepted when using '
                     'authenticated ADB.')
FLAGS = gflags.FLAGS


def GetRSAKwargs():
  if FLAGS.rsa_key_path:
    if rsa_signer is None:
      print('Please install either M2Crypto or python-rsa', file=sys.stderr)
      sys.exit(1)
    return {
        'rsa_keys': [rsa_signer(os.path.expanduser(path))
                     for path in FLAGS.rsa_key_path],
        'auth_timeout_ms': int(FLAGS.auth_timeout_s * 1000.0),
    }
  return {}


def main(argv):
  common_cli.StartCli(
      argv, adb_commands.AdbCommands.ConnectDevice,
      list_callback=adb_commands.AdbCommands.Devices, **GetRSAKwargs())


if __name__ == '__main__':
  main(FLAGS(sys.argv))

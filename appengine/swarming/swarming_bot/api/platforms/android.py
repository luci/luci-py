# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Android specific utility functions.

This file serves as an API to bot_config.py. bot_config.py can be replaced on
the server to allow additional server-specific functionality.
"""

import collections
import logging
import os


from adb import adb_protocol
from adb import common
from adb.contrib import adb_commands_safe
from adb.contrib import high
from api.platforms import gce


# Master switch that can easily be temporarily increased to INFO or even DEBUG
# when needed by simply pushing a new tainted swarming server version. This
# helps quickly debugging issues. On the other hand, even INFO level is quite
# verbose so keep it at WARNING by default.
LEVEL = logging.WARNING
adb_commands_safe._LOG.setLevel(LEVEL)
adb_protocol._LOG.setLevel(LEVEL)
common._LOG.setLevel(LEVEL)
high._LOG.setLevel(LEVEL)


# This list of third party apps embedded in the base OS image varies from
# version to version.
KNOWN_APPS = frozenset([
    'android',
    'android.autoinstalls.config.google.nexus',
    'com.frogmind.badland',
    'com.hp.android.printservice',
    'com.huawei.callstatisticsutils',
    'com.huawei.entitlement',
    'com.huawei.mmitest',
    'com.huawei.sarcontrolservice',
    'com.lge.HiddenMenu',
    'com.lge.SprintHiddenMenu',
    'com.lge.entitlement',
    'com.lge.lifetimer',
    'com.lge.update',
    'com.mediatek.fmradio',
    'com.mediatek.lbs.em2.ui',
    'com.motorola.android.buacontactadapter',
    'com.motorola.appdirectedsmsproxy',
    'com.motorola.entitlement',
    'com.motorola.motocit',
    'com.motorola.motosignature.app',
    'com.motorola.triggerenroll',
    'com.motorola.triggertrainingservice',
    'com.nuance.xt9.input',
    'com.nvidia.NvCPLSvc',
    'com.nvidia.NvCPLUpdater',
    'com.nvidia.benchmarkblocker',
    'com.nvidia.blakepairing',
    'com.nvidia.feedback',
    'com.nvidia.nvcecservice',
    'com.nvidia.nvgamecast',
    'com.nvidia.osc',
    'com.nvidia.ota',
    'com.nvidia.shield.nvcustomize',
    'com.nvidia.shield.welcome',
    'com.nvidia.shieldservice',
    'com.nvidia.stats',
    'com.nvidia.tegraprofiler.security',
    'com.nvidia.tegrazone3',
    'com.plexapp.android',
    'com.qti.qualcomm.datastatusnotification',
    'com.qualcomm.atfwd',
    'com.qualcomm.cabl',
    'com.qualcomm.embms',
    'com.qualcomm.qcrilmsgtunnel',
    'com.qualcomm.qti.rcsbootstraputil',
    'com.qualcomm.qti.rcsimsbootstraputil',
    'com.qualcomm.shutdownlistner',
    'com.qualcomm.timeservice',
    'com.quicinc.cne.CNEService',
    'com.quickoffice.android',
    'com.redbend.vdmc',
    'com.verizon.omadm',
    'com.vzw.apnservice',
    'com.yodo1.crossyroad',
    'jp.co.omronsoft.iwnnime.ml',
    'jp.co.omronsoft.iwnnime.ml.kbd.white',
    'org.codeaurora.ims',
])


def get_unknown_apps(device):
  return [
      p for p in device.GetPackages() or []
      if (not p.startswith(('com.android.', 'com.google.')) and
          p not in KNOWN_APPS)
  ]


def initialize(pub_key, priv_key):
  return high.Initialize(pub_key, priv_key)


def get_devices(bot, endpoints=None, enable_resets=False):
  devices = []
  if not gce.is_gce():
    devices += high.GetLocalDevices(
      'swarming', 10000, 10000, on_error=bot.post_error if bot else None,
      as_root=False, enable_resets=enable_resets)

  if endpoints:
    devices += high.GetRemoteDevices(
        'swarming', endpoints, 10000, 10000,
        on_error=bot.post_error if bot else None, as_root=False)

  return devices


def close_devices(devices):
  return high.CloseDevices(devices)


def kill_adb():
  return adb_commands_safe.KillADB()

// Copyright 2019 The LUCI Authors. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

export function botData(url, opts) {
  let botId = url.match('/bot/(.+)/get');
  botId = botId[1] || 'running';
  return botDataMap[botId];
}

export const botDataMap = {
  'running': {
    "authenticated_as": "bot:running.chromium.org",
    "dimensions": [
      {
        "value": [
          "vpython"
        ],
        "key": "caches"
      },
      {
        "value": [
          "8"
        ],
        "key": "cores"
      },
      {
        "value": [
          "x86",
          "x86-64",
          "x86-64-E3-1230_v5"
        ],
        "key": "cpu"
      },
      {
        "value": [
          "0"
        ],
        "key": "gce"
      },
      {
        "value": [
          "10de",
          "10de:1cb3",
          "10de:1cb3-25.21.14.1678"
        ],
        "key": "gpu"
      },
      {
        "value": [
          "build16-a9"
        ],
        "key": "id"
      },
      {
        "value": [
          "high"
        ],
        "key": "integrity"
      },
      {
        "value": [
          "en_US.cp1252"
        ],
        "key": "locale"
      },
      {
        "value": [
          "n1-standard-8"
        ],
        "key": "machine_type"
      },
      {
        "value": [
          "Windows",
          "Windows-10",
          "Windows-10-16299.309"
        ],
        "key": "os"
      },
      {
        "value": [
          "Skia"
        ],
        "key": "pool"
      },
      {
        "value": [
          "2.7.13"
        ],
        "key": "python"
      },
      {
        "value": [
          "1"
        ],
        "key": "rack"
      },
      {
        "value": [
          "4085-c81638b"
        ],
        "key": "server_version"
      },
      {
        "value": [
          "us",
          "us-mtv",
          "us-mtv-chops",
          "us-mtv-chops-a",
          "us-mtv-chops-a-9"
        ],
        "key": "zone"
      }
    ],
    "task_id": "42fb00e06d95be11",
    "external_ip": "70.32.137.220",
    "is_dead": false,
    "quarantined": false,
    "deleted": false,
    "state": "{\"audio\":[\"NVIDIA High Definition Audio\"],\"bot_group_cfg_version\":\"hash:d50e0a198b5ee4\",\"cost_usd_hour\":0.7575191297743056,\"cpu_name\":\"Intel(R) Xeon(R) CPU E3-1230 v5 @ 3.40GHz\",\"cwd\":\"C:\\\\b\\\\s\",\"cygwin\":[false],\"disks\":{\"c:\\\\\":{\"free_mb\":690166.5,\"size_mb\":763095.0}},\"env\":{\"PATH\":\"C:\\\\Windows\\\\system32;C:\\\\Windows;C:\\\\Windows\\\\System32\\\\Wbem;C:\\\\Windows\\\\System32\\\\WindowsPowerShell\\\\v1.0\\\\;c:\\\\Tools;C:\\\\CMake\\\\bin;C:\\\\Program Files\\\\Puppet Labs\\\\Puppet\\\\bin;C:\\\\Users\\\\chrome-bot\\\\AppData\\\\Local\\\\Microsoft\\\\WindowsApps\"},\"files\":{\"c:\\\\Users\\\\chrome-bot\\\\ntuser.dat\":1310720},\"gpu\":[\"Nvidia Quadro P400 25.21.14.1678\"],\"hostname\":\"build16-a9.labs.chromium.org\",\"ip\":\"192.168.216.26\",\"named_caches\":{\"vpython\":[[\"qp\",50935560],1549982906.0]},\"nb_files_in_temp\":2,\"pid\":7940,\"python\":{\"executable\":\"c:\\\\infra-system\\\\bin\\\\python.exe\",\"packages\":null,\"version\":\"2.7.13 (v2.7.13:a06454b1afa1, Dec 17 2016, 20:53:40) [MSC v.1500 64 bit (AMD64)]\"},\"ram\":32726,\"running_time\":21321,\"sleep_streak\":8,\"ssd\":[],\"started_ts\":1549961665,\"top_windows\":[],\"uptime\":21340,\"user\":\"chrome-bot\"}",
    "version": "8ea94136c96de7396fda8587d8e40cbc2d0c20ec01ce6b45c68d42a526d02316",
    "first_seen_ts": "2017-08-02T23:12:16.365500",
    "task_name": "Test-Win10-Clang-Golo-GPU-QuadroP400-x86_64-Debug-All-ANGLE",
    "last_seen_ts": "2019-02-12T14:54:12.335408",
    "bot_id": "running"
  },
}
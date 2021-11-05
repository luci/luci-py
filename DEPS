# Copyright 2021 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

use_relative_paths = True

deps = {
  'appengine/swarming/ui2/nodejs/': {
    'packages': [
      {
        'package': 'infra/3pp/tools/nodejs/${{platform}}',
        'version': 'version:13.3.0',
      }
    ],
    'dep_type': 'cipd',
  },

  # luci-go clients are used for client/run_isolated.py and integration tests.
  'luci-go': {
    'packages': [
      {
        'package': 'infra/tools/luci/cas/${{platform}}',
        'version': 'git_revision:de048fb36dbb1cb5b78774cc4a758e783865062f',
      },
      {
        'package': 'infra/tools/luci/fakecas/${{platform}}',
        'version': 'git_revision:2b6c41a617ae191ad5341683570f0a199b798ec5',
      },
      {
        'package': 'infra/tools/luci/isolate/${{platform}}',
        'version': 'git_revision:0547bbaba38b74361043c373c2f3979a7632b3ae',
      },
      {
        'package': 'infra/tools/luci/swarming/${{platform}}',
        'version': 'git_revision:93e1f15f1e75e0d93f5254eaff58716b73a06ab9',
      }
    ],
    'dep_type': 'cipd',
  },

  # Nsjail is used for our unit tests.
  'nsjail': {
    'packages': [
      {
        'package': 'infra/3pp/tools/nsjail/${{platform}}',
        'version': 'version:2@3.0.chromium.1',
      }
    ],
    "condition": "checkout_linux",
    'dep_type': 'cipd',
  },
}

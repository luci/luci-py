# Copyright 2021 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

use_relative_paths = True

deps = {
  'appengine/swarming/ui2/nodejs/': {
    'packages': [
      {
        'package': 'infra/3pp/tools/nodejs/${{platform}}',
        'version': 'version:2@20.10.0',
      }
    ],
    'dep_type': 'cipd',
    'condition': 'checkout_x64',
  },

  # luci-go clients are used for client/run_isolated.py and integration tests.
  'luci-go': {
    'packages': [
      {
        'package': 'infra/tools/luci/cas/${{platform}}',
        'version': 'git_revision:320a8df0f7934159fda7721f3b507e707531d284',
      },
      {
        'package': 'infra/tools/luci/fakecas/${{platform}}',
        'version': 'git_revision:74149ff5898591651443b0d2d17c46e275c0f1f1',
      },
      {
        'package': 'infra/tools/luci/isolate/${{platform}}',
        'version': 'git_revision:86e1e222575e40a48da50295d4712c9fbab58dd9',
      },
      {
        'package': 'infra/tools/luci/swarming/${{platform}}',
        'version': 'git_revision:320a8df0f7934159fda7721f3b507e707531d284',
      }
    ],
    'dep_type': 'cipd',
    'condition': 'checkout_x64',
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

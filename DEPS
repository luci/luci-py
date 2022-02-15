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
    'condition': 'checkout_x64',
  },

  # luci-go clients are used for client/run_isolated.py and integration tests.
  'luci-go': {
    'packages': [
      {
        'package': 'infra/tools/luci/cas/${{platform}}',
        'version': 'git_revision:fbbb5b9748a05dd16fe621f7ea48a4ece1913874',
      },
      {
        'package': 'infra/tools/luci/fakecas/${{platform}}',
        'version': 'git_revision:c4791d15e395b201e6a85336f7d8a21cce973dfa',
      },
      {
        'package': 'infra/tools/luci/isolate/${{platform}}',
        'version': 'git_revision:fbbb5b9748a05dd16fe621f7ea48a4ece1913874',
      },
      {
        'package': 'infra/tools/luci/swarming/${{platform}}',
        'version': 'git_revision:fbbb5b9748a05dd16fe621f7ea48a4ece1913874',
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

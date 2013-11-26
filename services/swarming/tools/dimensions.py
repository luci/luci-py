# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""A reusable dictionary to map configuration names to dimensions dicts."""

DIMENSIONS = {
    # Keep all config names lower case to make it easier to find them!
    'xp-ie7-enus': {
        'os': 'win-xp',
        'cpu': 'intel',
        'browser': 'ie-7',
        'lang': 'en-US',
    },
    'xp-ie8-enus': {
        'os': 'win-xp',
        'cpu': 'intel',
        'browser': 'ie-8',
        'lang': 'en-US',
    },
    'vista32-ie7-enus': {
        'os': 'win-vista',
        'cpu': 'intel-32',
        'browser': 'ie-7',
        'lang': 'en-US',
    },
    'vista32-ie8-enus': {
        'os': 'win-vista',
        'cpu': 'intel-32',
        'browser': 'ie-8',
        'lang': 'en-US',
    },
    'vista64-ie7-enus': {
        'os': 'win-vista',
        'cpu': 'intel-64',
        'browser': 'ie-7',
        'lang': 'en-US',
    },
    'vista64-ie8-enus': {
        'os': 'win-vista',
        'cpu': 'intel-64',
        'browser': 'ie-8',
        'lang': 'en-US',
    },
    'win7-32-ie9-enus': {
        'os': 'win-7',
        'cpu': 'intel-32',
        'browser': 'ie-9',
        'lang': 'en-US',
    },
    'win7-64-ie8-enus': {
        'os': 'win-7',
        'cpu': 'intel-64',
        'browser': 'ie-8',
        'lang': 'en-US',
    },
}

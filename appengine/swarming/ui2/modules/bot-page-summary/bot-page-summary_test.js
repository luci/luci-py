// Copyright 2019 The LUCI Authors. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

import 'modules/bot-page-summary';

describe('bot-page-summary', function() {
  // Instead of using import, we use require. Otherwise,
  // the concatenation trick we do doesn't play well with webpack, which would
  // leak dependencies (e.g. bot-list's 'column' function to task-list) and
  // try to import things multiple times.
  const {customMatchers} = require('modules/test_util');
  const {prettifyName} = require('modules/bot-page-summary/bot-page-summary');
  // A reusable HTML element in which we create our element under test.
  const container = document.createElement('div');
  document.body.appendChild(container);

  beforeEach(function() {
    jasmine.addMatchers(customMatchers);
  });

  afterEach(function() {
    container.innerHTML = '';
  });

  // ===============TESTS START====================================

  it('make task names less unique', function() {
    const testCases = [
      {
        input: 'Perf-Win10-Clang-Golo-GPU-QuadroP400-x86_64-Debug-All-ANGLE (debug)',
        output: 'Perf-Win10-Clang-Golo-GPU-QuadroP400-x86_64-Debug-All-ANGLE',
      },
    ];
    for (const test of testCases) {
      expect(prettifyName(test.input)).toEqual(test.output);
    }
  });
});

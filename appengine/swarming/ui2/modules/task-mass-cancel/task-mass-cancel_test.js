// Copyright 2019 The LUCI Authors. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

import 'modules/task-mass-cancel'

describe('task-mass-cancel', function() {
  // A reusable HTML element in which we create our element under test.
  let container = document.createElement('div');
  document.body.appendChild(container);

  afterEach(function() {
    container.innerHTML = '';
  });

//===============TESTS START====================================

  // calls the test callback with one element 'ele', a created <task-mass-cancel>.
  function createElement(test) {
    return window.customElements.whenDefined('task-mass-cancel').then(() => {
      container.innerHTML = `<task-mass-cancel></task-mass-cancel>`;
      expect(container.firstElementChild).toBeTruthy();
      test(container.firstElementChild);
    });
  }

  xit('is implemented', function(done) {
    createElement((ele) => {
      expect('implemented').toBeFalsy();
      done();
    });
  });

});
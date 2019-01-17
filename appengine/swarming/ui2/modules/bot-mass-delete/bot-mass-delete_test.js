// Copyright 2019 The LUCI Authors. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

import 'modules/bot-mass-delete'

describe('bot-mass-delete', function() {
  const { $, $$ } = require('common-sk/modules/dom');
  const { customMatchers, mockAppGETs } = require('modules/test_util');
  const { fetchMock, MATCHED, UNMATCHED } = require('fetch-mock');

  // A reusable HTML element in which we create our element under test.
  let container = document.createElement('div');
  document.body.appendChild(container);

  beforeEach(function() {
    jasmine.addMatchers(customMatchers);

    mockAppGETs(fetchMock, {delete_bot: true});

    fetchMock.get('/_ah/api/swarming/v1/bots/count?dimensions=os%3AAndroid&dimensions=pool%3AChrome', {'dead': 532});

    // Everything else
    fetchMock.catch(404);
  });

  afterEach(function() {
    container.innerHTML = '';
    // Completely remove the mocking which allows each test
    // to be able to mess with the mocked routes w/o impacting other tests.
    fetchMock.reset();
  });

//===============TESTS START====================================

  // calls the test callback with one element 'ele', a created <bot-mass-delete>.
  function createElement(test) {
    return window.customElements.whenDefined('bot-mass-delete').then(() => {
      container.innerHTML = `<bot-mass-delete dimensions="pool:Chrome,os:Android" auth_header="fake"></bot-mass-delete>`;
      expect(container.firstElementChild).toBeTruthy();
      test(container.firstElementChild);
    });
  }

  it('can read in attributes', function(done) {
    createElement((ele) => {
      expect(ele.dimensions.length).toBe(2);
      expect(ele.dimensions).toContain('pool:Chrome');
      expect(ele.dimensions).toContain('os:Android');
      expect(ele.auth_header).toBe('fake');
      done();
    });
  });


  it('has a list of the passed in dimensions', function(done) {
    createElement((ele) => {
      ele.render();

      let listedDims = $('ul li', ele);
      expect(listedDims.length).toBe(2);
      expect(listedDims[0]).toMatchTextContent('os:Android');
      done();
    });
  });

  function expectNoUnmatchedCalls() {
    let calls = fetchMock.calls(UNMATCHED, 'GET');
    expect(calls.length).toBe(0, 'no unmatched (unexpected) GETs');
    calls = fetchMock.calls(UNMATCHED, 'POST');
    expect(calls.length).toBe(0, 'no unmatched (unexpected) POSTs');
  }

  it('makes an API call to count when loading', function(done) {
    createElement((ele) => {
      ele.show();
      // The true on flush waits for res.json() to resolve too, which
      // is when we know the element has updated the _tasks.
      fetchMock.flush(true).then(() => {
        expectNoUnmatchedCalls()
        let calls = fetchMock.calls(MATCHED, 'GET');
        expect(calls.length).toBe(1);
        done();
      });
    });
  });

  it('makes an API call to list after clicking, then deletes', function(done) {
    createElement((ele) => {

      // create a shortened version of the returned data
      fetchMock.get('/_ah/api/swarming/v1/bots/list?dimensions=os%3AAndroid&dimensions=pool%3AChrome' +
                    '&fields=items%2Fbot_id&limit=200&is_dead=TRUE',
        {
          items: [{bot_id: 'bot-1'}, {bot_id: 'bot-2'}, {bot_id: 'bot-3'}],
        }
      );

      fetchMock.post('/_ah/api/swarming/v1/bot/bot-1/delete', 200);
      fetchMock.post('/_ah/api/swarming/v1/bot/bot-2/delete', 200);
      fetchMock.post('/_ah/api/swarming/v1/bot/bot-3/delete', 200);

      let sawStartEvent = false;
      ele.addEventListener('bots-deleting-started', () => {
        sawStartEvent = true;
      });

      ele.addEventListener('bots-deleting-finished', () => {
        expect(sawStartEvent).toBeTruthy();
        expectNoUnmatchedCalls();
        let calls = fetchMock.calls(MATCHED, 'GET');
        expect(calls.length).toBe(1, '1 from list (ele.show() was not called)');

        calls = fetchMock.calls(MATCHED, 'POST');
        expect(calls.length).toBe(3, '3 to delete');
        done();
      });

      ele._readyToDelete = true;
      ele.render();
      let button = $$('button.delete', ele);
      button.click();
    });
  });
});
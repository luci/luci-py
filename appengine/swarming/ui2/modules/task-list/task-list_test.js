// Copyright 2019 The LUCI Authors. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

import 'modules/task-list'

import { processTasks } from 'modules/task-list/task-list-helpers'
import { tasks_20 } from 'modules/task-list/test_data'

describe('task-list', function() {
  // Things that get imported multiple times go here, using require. Otherwise,
  // the concatenation trick we do doesn't play well with webpack, which tries
  // to include it multiple times.
  const { mockAppGETs, customMatchers}  = require('modules/test_util');
  const { fetchMock, MATCHED, UNMATCHED } = require('fetch-mock');

  beforeEach(function() {
    jasmine.addMatchers(customMatchers);
    // Clear out any query params we might have to not mess with our current state.
    history.pushState(null, '', window.location.origin + window.location.pathname + '?');
  });

  beforeEach(function() {
    // These are the default responses to the expected API calls (aka 'matched').
    // They can be overridden for specific tests, if needed.
    mockAppGETs(fetchMock, {
      cancel_task: false,
    });

    fetchMock.get('glob:/_ah/api/swarming/v1/tasks/list?*', tasks_20);

    // Everything else
    fetchMock.catch(404);
  });

  afterEach(function() {
    // Completely remove the mocking which allows each test
    // to be able to mess with the mocked routes w/o impacting other tests.
    fetchMock.reset();
  });

  // A reusable HTML element in which we create our element under test.
  let container = document.createElement('div');
  document.body.appendChild(container);

  afterEach(function() {
    container.innerHTML = '';
  });

  beforeEach(function() {
    // Fix the time so all of our relative dates work.
    // Note, this turns off the default behavior of setTimeout and related.
    jasmine.clock().install();
    jasmine.clock().mockDate(new Date(Date.UTC(2018, 12, 19, 14, 46, 22, 1234)));
  });

  afterEach(function() {
    jasmine.clock().uninstall();
  });

  // calls the test callback with one element 'ele', a created <swarming-index>.
  // We can't put the describes inside the whenDefined callback because
  // that doesn't work on Firefox (and possibly other places).
  function createElement(test) {
    return window.customElements.whenDefined('task-list').then(() => {
      container.innerHTML = `<task-list client_id=for_test testing_offline=true></task-list>`;
      expect(container.firstElementChild).toBeTruthy();
      test(container.firstElementChild);
    });
  }

  function userLogsIn(ele, callback) {
    // The swarming-app emits the 'busy-end' event when all pending
    // fetches (and renders) have resolved.
    let ran = false;
    ele.addEventListener('busy-end', (e) => {
      if (!ran) {
        ran = true; // prevent multiple runs if the test makes the
                    // app go busy (e.g. if it calls fetch).
        callback();
      }
    });
    let login = $$('oauth-login', ele);
    login._logIn();
    fetchMock.flush();
  }

  // convenience function to save indentation and boilerplate.
  // expects a function test that should be called with the created
  // <task-list> after the user has logged in.
  function loggedInTasklist(test) {
    createElement((ele) => {
      userLogsIn(ele, () => {
        test(ele);
      });
    });
  }

  describe('html structure', function() {
    it('contains swarming-app as its only child', function(done) {
      createElement((ele) => {
        expect(ele.children.length).toBe(1);
        expect(ele.children[0].tagName).toBe('swarming-app'.toUpperCase());
        done();
      });
    });

    describe('when not logged in', function() {
      it('tells the user they should log in', function(done) {
        createElement((ele) => {
          let loginMessage = $$('swarming-app>main .message', ele);
          expect(loginMessage).toBeTruthy();
          expect(loginMessage.hidden).toBeFalsy('Message should not be hidden');
          expect(loginMessage.textContent).toContain('must sign in');
          done();
        })
      })
      it('does not display filters or tasks', function(done) {
        createElement((ele) => {
          let taskTable = $$('.task-table', ele);
          expect(taskTable).toBeTruthy();
          expect(taskTable.hidden).toBeTruthy('.task-table should be hidden');
          expect($$('main button:not([hidden])', ele)).toBeFalsy('no buttons seen');
          expect($$('.header', ele)).toBeFalsy('no filters seen');
          done();
        })
      });
    }); //end describe('when not logged in')

    describe('when logged in as unauthorized user', function() {

      function notAuthorized() {
        // overwrite the default fetchMock behaviors to have everything return 403.
        fetchMock.get('/_ah/api/swarming/v1/server/details', 403,
                      { overwriteRoutes: true });
        fetchMock.get('/_ah/api/swarming/v1/server/permissions', {},
                      { overwriteRoutes: true });
        fetchMock.get('glob:/_ah/api/swarming/v1/tasks/list?*', 403,
                      { overwriteRoutes: true });
      }

      beforeEach(notAuthorized);

      it('tells the user they should change accounts', function(done) {
        loggedInTasklist((ele) => {
          let loginMessage = $$('swarming-app>main .message', ele);
          expect(loginMessage).toBeTruthy();
          expect(loginMessage.hidden).toBeFalsy('Message should not be hidden');
          expect(loginMessage.textContent).toContain('different account');
          done();
        });
      });
      it('does not display filters or tasks', function(done) {
        loggedInTasklist((ele) => {
          let taskTable = $$('.task-table', ele);
          expect(taskTable).toBeTruthy();
          expect(taskTable.hidden).toBeTruthy('.task-table should be hidden');

          expect($$('main button:not([hidden])', ele)).toBeFalsy('no buttons seen');
          expect($$('.header', ele)).toBeFalsy('no filters seen');
          done();
        });
      });
    }); // end describe('when logged in as unauthorized user')

    describe('when logged in as user (not admin)', function() {

      describe('default landing page', function() {
        it('displays whatever tasks show up', function(done) {
          loggedInTasklist((ele) => {
            let rows = $('.task-table .task-row', ele);
            expect(rows).toBeTruthy();
            expect(rows.length).toBe(20, '(num taskRows)');
            done();
          });
        });
      }); // end describe('default landing page')

    });// end describe('when logged in as user')

  }); // end describe('html structure')

  describe('dynamic behavior', function() {
    // TODO
  }); // end describe('dynamic behavior')

  describe('api calls', function() {
    function expectNoUnmatchedCalls() {
      let calls = fetchMock.calls(UNMATCHED, 'GET');
      expect(calls.length).toBe(0, 'no unmatched (unexpected) GETs');
      calls = fetchMock.calls(UNMATCHED, 'POST');
      expect(calls.length).toBe(0, 'no unmatched (unexpected) POSTs');
    }

    it('makes no API calls when not logged in', function(done) {
      createElement((ele) => {
        fetchMock.flush().then(() => {
          // MATCHED calls are calls that we expect and specified in the
          // beforeEach at the top of this file.
          let calls = fetchMock.calls(MATCHED, 'GET');
          expect(calls.length).toBe(0);
          calls = fetchMock.calls(MATCHED, 'POST');
          expect(calls.length).toBe(0);

          expectNoUnmatchedCalls();
          done();
        });
      });
    });

    function checkAuthorizationAndNoPosts(calls) {
      // check authorization headers are set
      calls.forEach((c) => {
        expect(c[1].headers).toBeDefined();
        expect(c[1].headers.authorization).toContain('Bearer ');
      })

      calls = fetchMock.calls(MATCHED, 'POST');
      expect(calls.length).toBe(0, 'no POSTs on task-list');

      expectNoUnmatchedCalls();
    }

    it('makes auth\'d API calls when a logged in user views landing page', function(done) {
      loggedInTasklist((ele) => {
        let calls = fetchMock.calls(MATCHED, 'GET');
        expect(calls.length).toBe(2+1, '2 GETs from swarming-app, 1 from task-list');
        // calls is an array of 2-length arrays with the first element
        // being the string of the url and the second element being
        // the options that were passed in
        let gets = calls.map((c) => c[0]);

        // limit=100 comes from the default limit value.
        expect(gets).toContainRegex(/\/_ah\/api\/swarming\/v1\/tasks\/list.+limit=100.*/);

        checkAuthorizationAndNoPosts(calls)
        done();
      });
    });
  }); // end describe('api calls')

  describe('data parsing', function() {
    const ANDROID_TASK = tasks_20.items[0];

    it('turns the dates into DateObjects', function() {
      // Make a copy of the object because processTasks will modify it in place.
      let tasks = processTasks([deepCopy(ANDROID_TASK)]);
      let task = tasks[0]
      expect(task.created_ts).toBeTruthy();
      expect(task.created_ts instanceof Date).toBeTruthy('Should be a date object');
      expect(task.human_created_ts).toBeTruthy();
      expect(task.pending_time).toBeTruthy();
      expect(task.human_pending_time).toBeTruthy();
    });

    it('gracefully handles null data', function() {
      let tasks = processTasks(null);

      expect(tasks).toBeTruthy();
      expect(tasks.length).toBe(0);
    });

  }); //end describe('data parsing')
});
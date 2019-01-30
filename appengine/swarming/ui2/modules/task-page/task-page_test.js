// Copyright 2019 The LUCI Authors. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

import 'modules/task-page'

describe('task-page', function() {
  // Instead of using import, we use require. Otherwise,
  // the concatenation trick we do doesn't play well with webpack, which would
  // leak dependencies (e.g. bot-list's 'column' function to task-list) and
  // try to import things multiple times.
  const { $, $$ } = require('common-sk/modules/dom');
  const { customMatchers, expectNoUnmatchedCalls, mockAppGETs } = require('modules/test_util');
  const { fetchMock, MATCHED, UNMATCHED } = require('fetch-mock');
  const { taskResults, taskRequests } = require('modules/task-page/test_data');

  const TEST_TASK_ID = 'test0b3c0fac7810';

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

    // By default, don't have any handlers mocked out - this requires
    // tests to opt-in to wanting certain request data.

    // Everything else
    fetchMock.catch(404);
  });

  afterEach(function() {
    // Completely remove the mocking which allows each test
    // to be able to mess with the mocked routes w/o impacting other tests.
    fetchMock.reset();
  });

  // A reusable HTML element in which we create our element under test.
  const container = document.createElement('div');
  document.body.appendChild(container);

  afterEach(function() {
    container.innerHTML = '';
  });

  beforeEach(function() {
    // Fix the time so all of our relative dates work.
    // Note, this turns off the default behavior of setTimeout and related.
    jasmine.clock().install();
    jasmine.clock().mockDate(new Date(Date.UTC(2019, 1, 4, 16, 46, 22, 1234)));
  });

  afterEach(function() {
    jasmine.clock().uninstall();
  });

  // calls the test callback with one element 'ele', a created <task-page>.
  function createElement(test) {
    return window.customElements.whenDefined('task-page').then(() => {
      container.innerHTML = `<task-page client_id=for_test testing_offline=true></task-page>`;
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
    const login = $$('oauth-login', ele);
    login._logIn();
    fetchMock.flush();
  }

  // convenience function to save indentation and boilerplate.
  // expects a function test that should be called with the created
  // <task-page> after the user has logged in.
  function loggedInTaskPage(test) {
    createElement((ele) => {
      ele._taskId = TEST_TASK_ID;
      userLogsIn(ele, () => {
        test(ele);
      });
    });
  }

  function serveTask(idx, msg) {
    // msg is the name field in the task request, used to 1) give a human
    // readable description of the task data inline of the test and 2)
    // lessen the risk of copy-pasta mistakes.
    const request = taskRequests[idx];
    expect(request.name).toEqual(msg);
    const result = taskResults[idx];
    expect(result.name).toEqual(msg);

    fetchMock.get(`/_ah/api/swarming/v1/task/${TEST_TASK_ID}/request`, request);
    fetchMock.get(`/_ah/api/swarming/v1/task/${TEST_TASK_ID}/result?include_performance_stats=true`, result);
  }

//===============TESTS START====================================

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
          const loginMessage = $$('swarming-app>main .message', ele);
          expect(loginMessage).toBeTruthy();
          expect(loginMessage.hidden).toBeFalsy('Message should not be hidden');
          expect(loginMessage.textContent).toContain('must sign in');
          done();
        });
      });

      it('does not display filters or tasks', function(done) {
        createElement((ele) => {
          const topDivs = $('main > div', ele);
          expect(topDivs).toBeTruthy();
          expect(topDivs.length).toBe(2);
          expect(topDivs[0].hidden).toBeTruthy('left side hidden');
          expect(topDivs[1].hidden).toBeTruthy('right side hidden');
          done();
        });
      });
    }); //end describe('when not logged in')

    describe('when logged in as unauthorized user', function() {

      function notAuthorized() {
        // overwrite the default fetchMock behaviors to have everything return 403.
        fetchMock.get('/_ah/api/swarming/v1/server/details', 403,
                      { overwriteRoutes: true });
        fetchMock.get('/_ah/api/swarming/v1/server/permissions', {},
                      { overwriteRoutes: true });
        fetchMock.get('glob:/_ah/api/swarming/v1/task/*', 403,
                      { overwriteRoutes: true });
      }

      beforeEach(notAuthorized);

      it('tells the user they should change accounts', function(done) {
        loggedInTaskPage((ele) => {
          const loginMessage = $$('swarming-app>main .message', ele);
          expect(loginMessage).toBeTruthy();
          expect(loginMessage.hidden).toBeFalsy('Message should not be hidden');
          expect(loginMessage.textContent).toContain('different account');
          done();
        });
      });

      it('does not display filters or tasks', function(done) {
        createElement((ele) => {
          const topDivs = $('main > div', ele);
          expect(topDivs).toBeTruthy();
          expect(topDivs.length).toBe(2);
          expect(topDivs[0].hidden).toBeTruthy('left side hidden');
          expect(topDivs[1].hidden).toBeTruthy('right side hidden');
          done();
        });
      });
    }); // end describe('when logged in as unauthorized user')

    describe('Completed task with 2 slices', function() {
      beforeEach(() => serveTask(0, 'Completed task with 2 slices'));

      it('shows relevent task request data', function(done) {
        loggedInTaskPage((ele) => {
          const taskInfo = $$('table.request-info', ele);
          expect(taskInfo).toBeTruthy();
          const rows = $('tr', taskInfo);
          expect(rows.length).toBeTruthy('Has some rows');

          // little helper for readability
          const cell = (r, c) => rows[r].children[c];
          // Spot check some of the content
          expect(cell(0, 0)).toMatchTextContent('Name');
          expect(cell(0, 1)).toMatchTextContent('Completed task with 2 slices');
          expect(cell(1, 0)).toMatchTextContent('State');
          expect(cell(1, 1)).toMatchTextContent('COMPLETED (SUCCESS)');
          expect(rows[5].hidden).toBeTruthy();
          expect(cell(7, 0)).toMatchTextContent('Wait for Capacity');
          expect(cell(7, 1)).toMatchTextContent('false');
          expect(cell(14, 0).rowSpan).toEqual(7); // 6 dimensions shown

          const subsections = $('tbody', taskInfo);
          expect(subsections.length).toEqual(2);
          expect(subsections[0].hidden).toBeFalsy();
          expect(subsections[1].hidden).toBeTruthy();

          done();
        });
      });

      it('shows relevent task timing data', function(done) {
        loggedInTaskPage((ele) => {
          const taskInfo = $$('table.task-timing', ele);
          expect(taskInfo).toBeTruthy();
          const rows = $('tr', taskInfo);
          expect(rows.length).toEqual(9);

          // little helper for readability
          const cell = (r, c) => rows[r].children[c];
          // Spot check some of the content
          expect(rows[1].hidden).toBeFalsy();
          expect(cell(6, 0)).toMatchTextContent('Pending Time');
          expect(cell(6, 1)).toMatchTextContent('3m 22s');
          expect(cell(7, 0)).toMatchTextContent('Total Overhead');
          expect(cell(7, 1)).toMatchTextContent('12.63s');
          expect(cell(8, 0)).toMatchTextContent('Running Time');
          expect(cell(8, 1)).toMatchTextContent('14m 41s');

          done();
        });
      });

      it('shows relevent task execution data', function(done) {
        loggedInTaskPage((ele) => {
          const taskExecution = $$('table.task-execution', ele);
          expect(taskExecution).toBeTruthy();
          const rows = $('tr', taskExecution);
          expect(rows.length).toBeTruthy();

          // little helper for readability
          const cell = (r, c) => rows[r].children[c];
          // Spot check some of the content
          expect(cell(0, 0)).toMatchTextContent('Bot assigned to task');
          expect(cell(0, 1).innerHTML).toContain('<a ', 'has a link');
          expect(cell(0, 1).innerHTML).toContain('href="/bot?id=swarm1931-c4"', 'link is correct');
          expect(cell(1, 0).rowSpan).toEqual(15); // 14 dimensions shown

          done();
        });
      });

      it('shows relevent performance stats', function(done) {
        loggedInTaskPage((ele) => {
          const taskPerformance = $$('table.performance-stats', ele);
          expect(taskPerformance).toBeTruthy();
          const rows = $('tr', taskPerformance);
          expect(rows.length).toBeTruthy();

          // little helper for readability
          const cell = (r, c) => rows[r].children[c];
          // Spot check some of the content
          expect(cell(0, 0)).toMatchTextContent('Total Overhead');
          expect(cell(0, 1)).toMatchTextContent('12.63s');
          expect(cell(6, 0)).toMatchTextContent('Uploaded Cold Items');
          expect(cell(6, 1)).toMatchTextContent('2 items; 12 KB');
          expect(cell(7, 0)).toMatchTextContent('Uploaded Hot Items');
          expect(cell(7, 1)).toMatchTextContent('0 items; 0 B');
          done();
        });
      });
    });

  }); // end describe('html structure')

  describe('dynamic behavior', function() {
    it('shows and hides the extra details', function(done) {
      loggedInTaskPage((ele) => {
        ele._showDetails = false;
        ele.render();

        const lowerHalf = $('.task-info > tbody', ele)[1];
        expect(lowerHalf.hidden).toBeTruthy();

        const btn = $$('.details button', ele);
        btn.click();
        expect(lowerHalf.hidden).toBeFalsy();
        btn.click();
        expect(lowerHalf.hidden).toBeTruthy();

        done();
      });
    });
  });

  describe('api calls', function() {
    it('makes no API calls when not logged in', function(done) {
      createElement((ele) => {
        fetchMock.flush().then(() => {
          // MATCHED calls are calls that we expect and specified in the
          // beforeEach at the top of this file.
          let calls = fetchMock.calls(MATCHED, 'GET');
          expect(calls.length).toBe(0);
          calls = fetchMock.calls(MATCHED, 'POST');
          expect(calls.length).toBe(0);

          expectNoUnmatchedCalls(fetchMock);
          done();
        });
      });
    });

    function checkAuthorizationAndNoPosts(calls) {
      // check authorization headers are set
      calls.forEach((c) => {
        expect(c[1].headers).toBeDefined();
        expect(c[1].headers.authorization).toContain('Bearer ');
      });

      calls = fetchMock.calls(MATCHED, 'POST');
      expect(calls.length).toBe(0, 'no POSTs on task-list');

      expectNoUnmatchedCalls(fetchMock);
    }

    it('makes auth\'d API calls when a logged in user views landing page', function(done) {
      serveTask(0, 'Completed task with 2 slices');
      loggedInTaskPage((ele) => {
        let calls = fetchMock.calls(MATCHED, 'GET');
        expect(calls.length).toBe(2+2, '2 GETs from swarming-app, 2 from task-page');
        // calls is an array of 2-length arrays with the first element
        // being the string of the url and the second element being
        // the options that were passed in
        const gets = calls.map((c) => c[0]);

        expect(gets).toContain(`/_ah/api/swarming/v1/task/${TEST_TASK_ID}/request`);
        expect(gets).toContain(`/_ah/api/swarming/v1/task/${TEST_TASK_ID}/result?include_performance_stats=true`);

        checkAuthorizationAndNoPosts(calls);
        done();
      });
    });

  }); // end describe('api calls')
});
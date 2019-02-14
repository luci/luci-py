// Copyright 2019 The LUCI Authors. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

import 'modules/bot-page'

describe('bot-page', function() {
  // Instead of using import, we use require. Otherwise,
  // the concatenation trick we do doesn't play well with webpack, which would
  // leak dependencies (e.g. bot-list's 'column' function to task-list) and
  // try to import things multiple times.
  const { $, $$ } = require('common-sk/modules/dom');
  const { customMatchers, expectNoUnmatchedCalls, mockAppGETs } = require('modules/test_util');
  const { fetchMock, MATCHED, UNMATCHED } = require('fetch-mock');
  const { botDataMap, eventsMap, tasksMap } = require('modules/bot-page/test_data');


  const TEST_BOT_ID = 'example-gce-001';

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
    jasmine.clock().mockDate(new Date(Date.UTC(2019, 1, 12, 18, 46, 22, 1234)));
  });

  afterEach(function() {
    jasmine.clock().uninstall();
  });

  // calls the test callback with one element 'ele', a created <bot-page>.
  function createElement(test) {
    return window.customElements.whenDefined('bot-page').then(() => {
      container.innerHTML = `<bot-page client_id=for_test testing_offline=true></bot-page>`;
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
  // <bot-page> after the user has logged in.
  function loggedInBotPage(test, emptyBotId) {
    createElement((ele) => {
      if (!emptyBotId) {
        ele._botId = TEST_BOT_ID;
      }
      userLogsIn(ele, () => {
        test(ele);
      });
    });
  }

  function serveBot(botName) {
    const data = botDataMap[botName];
    const tasks = {items: tasksMap['SkiaGPU']};
    const events = {items: eventsMap['SkiaGPU']};

    fetchMock.get(`/_ah/api/swarming/v1/bot/${TEST_BOT_ID}/get`, data);
    fetchMock.get(`glob:/_ah/api/swarming/v1/bot/${TEST_BOT_ID}/tasks*`, tasks);
    fetchMock.get(`glob:/_ah/api/swarming/v1/bot/${TEST_BOT_ID}/events*`, events);
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
          expect(loginMessage).not.toHaveAttribute('hidden', 'Message should not be hidden');
          expect(loginMessage.textContent).toContain('must sign in');
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
        fetchMock.get('glob:/_ah/api/swarming/v1/bot/*', 403,
                      { overwriteRoutes: true });
      }

      beforeEach(notAuthorized);

      it('tells the user they should change accounts', function(done) {
        loggedInBotPage((ele) => {
          const loginMessage = $$('swarming-app>main .message', ele);
          expect(loginMessage).toBeTruthy();
          expect(loginMessage).not.toHaveAttribute('hidden', 'Message should not be hidden');
          expect(loginMessage.textContent).toContain('different account');
          done();
        });
      });

      it('does not display logs or task details', function(done) {
        loggedInBotPage((ele) => {
          const content = $$('main .content', ele);
          expect(content).toBeTruthy();
          expect(content).toHaveAttribute('hidden');
          done();
        });
      });
    }); // end describe('when logged in as unauthorized user')

    describe('authorized user, but no bot id', function() {

      it('tells the user they should enter a bot id', function(done) {
        loggedInBotPage((ele) => {
          const loginMessage = $$('.id_buttons .message', ele);
          expect(loginMessage).toBeTruthy();
          expect(loginMessage.textContent).toContain('Enter a Bot ID');
          done();
        }, true);
      });

      it('does not display filters or tasks', function(done) {
        loggedInBotPage((ele) => {
          const content = $$('main .content', ele);
          expect(content).toBeTruthy();
          expect(content).toHaveAttribute('hidden');
          done();
        }, true);
      });
    }); // end describe('authorized user, but no taskid')

    describe('gpu bot with a running task', function() {
      beforeEach(() => serveBot('running'));

      it('renders some of the bot data', function(done) {
        loggedInBotPage((ele) => {
          const dataTable = $$('table.data_table', ele);
          expect(dataTable).toBeTruthy();

          const rows = $('tr', dataTable);
          expect(rows).toBeTruthy();
          expect(rows.length).toBeTruthy();

          // little helper for readability
          const cell = (r, c) => rows[r].children[c];

          expect(cell(1, 0)).toMatchTextContent('Current Task');
          expect(cell(1, 1)).toMatchTextContent('42fb00e06d95be11');
          expect(cell(1, 1).innerHTML).toContain('<a ', 'has a link');
          expect(cell(1, 1).innerHTML).toContain('href="/task?id=42fb00e06d95be10"');
          expect(cell(2, 0)).toMatchTextContent('Dimensions');
          expect(cell(7, 0)).toMatchTextContent('gpu');
          expect(cell(7, 1)).toMatchTextContent('NVIDIA (10de) | ' +
            'NVIDIA Quadro P400 (10de:1cb3) | NVIDIA Quadro P400 (10de:1cb3-25.21.14.1678)');
          expect(cell(19, 0)).toMatchTextContent('Bot Version');
          expect(rows[19]).toHaveClass('old_version');
          done();
        });
      });

      it('renders the tasks in a table', function(done) {
        loggedInBotPage((ele) => {
          ele._showEvents = false;
          ele.render();
          const tasksTable = $$('table.tasks_table', ele);
          expect(tasksTable).toBeTruthy();

          const rows = $('tr', tasksTable);
          expect(rows).toBeTruthy();
          expect(rows.length).toEqual(1 + 30, '1 for header, 30 tasks');

          // little helper for readability
          const cell = (r, c) => rows[r].children[c];

          // row 0 is the header
          expect(cell(1, 0)).toMatchTextContent('Perf-Win10-Clang-Golo-GPU-QuadroP400-x86_64-Debug-All-ANGLE');
          expect(cell(1, 0).innerHTML).toContain('<a ', 'has a link');
          expect(cell(1, 0).innerHTML).toContain('href="/task?id=43004cb4fca98110"');
          expect(cell(1, 2)).toMatchTextContent('7m 20s');
          expect(cell(1, 3)).toMatchTextContent('RUNNING');
          expect(rows[1]).toHaveClass('pending_task');
          expect(cell(2, 2)).toMatchTextContent('3m 51s');
          expect(cell(2, 3)).toMatchTextContent('SUCCESS');
          expect(rows[2]).not.toHaveClass('pending_task');
          expect(rows[2]).not.toHaveClass('failed_task)');
          expect(rows[2]).not.toHaveClass('exception');
          expect(rows[2]).not.toHaveClass('bot_died');

          done();
        });
      });

      it('renders all events in a table', function(done) {
        loggedInBotPage((ele) => {
          ele._showEvents = true;
          ele._showAll = true;
          ele.render();
          const eventsTable = $$('table.events_table', ele);
          expect(eventsTable).toBeTruthy();

          const rows = $('tr', eventsTable);
          expect(rows).toBeTruthy();
          expect(rows.length).toEqual(1 + 50, '1 for header, 50 events');

          // little helper for readability
          const cell = (r, c) => rows[r].children[c];

          // row 0 is the header
          expect(cell(1, 0)).toMatchTextContent('');
          expect(cell(1, 1)).toMatchTextContent('request_task');
          expect(cell(1, 3).innerHTML).toContain('<a ', 'has a link');
          expect(cell(1, 3).innerHTML).toContain('href="/task?id=4300ceb85b93e010"');
          expect(cell(1, 4)).toMatchTextContent('abcdoeraym');
          expect(cell(1, 4)).not.toHaveClass('old_version');

          expect(cell(15, 0)).toMatchTextContent('About to restart: ' +
                'Updating to abcdoeraymeyouandme');
          expect(cell(15, 1)).toMatchTextContent('bot_shutdown');
          expect(cell(15, 3)).toMatchTextContent('');
          expect(cell(15, 4)).toMatchTextContent('6fda8587d8');
          expect(cell(15, 4)).toHaveClass('old_version');
          done();
        });
      });

      it('renders some events in a table', function(done) {
        loggedInBotPage((ele) => {
          ele._showEvents = true;
          ele._showAll = false;
          ele.render();
          const eventsTable = $$('table.events_table', ele);
          expect(eventsTable).toBeTruthy();

          const rows = $('tr', eventsTable);
          expect(rows).toBeTruthy();
          expect(rows.length).toEqual(1 + 1, '1 for header, 1 shown event');

          // little helper for readability
          const cell = (r, c) => rows[r].children[c];

          // row 0 is the header
          expect(cell(1, 0)).toMatchTextContent('About to restart: ' +
                'Updating to abcdoeraymeyouandme');
          expect(cell(1, 1)).toMatchTextContent('bot_shutdown');
          expect(cell(1, 3)).toMatchTextContent('');
          expect(cell(1, 4)).toMatchTextContent('6fda8587d8');
          expect(cell(1, 4)).toHaveClass('old_version');
          done();
        });
      });

      it('disables buttons for unprivileged users', function(done) {
        loggedInBotPage((ele) => {
          ele.permissions.cancel_task = false;
          ele.permissions.terminate_bot = false;
          ele.render();
          const killBtn = $$('main button.kill', ele);
          expect(killBtn).toBeTruthy();
          expect(killBtn).toHaveAttribute('disabled');

          const tBtn = $$('main button.shut_down', ele);
          expect(tBtn).toBeTruthy();
          expect(tBtn).toHaveAttribute('disabled');

          done();
        });
      });

      it('enables buttons for privileged users', function(done) {
        loggedInBotPage((ele) => {
          ele.permissions.cancel_task = true;
          ele.permissions.terminate_bot = true;
          ele.render();
          const killBtn = $$('main button.kill', ele);
          expect(killBtn).toBeTruthy();
          expect(killBtn).not.toHaveAttribute('disabled');

          const tBtn = $$('main button.shut_down', ele);
          expect(tBtn).toBeTruthy();
          expect(tBtn).not.toHaveAttribute('disabled');

          done();
        });
      });
    }); // end describe('gpu bot with a running task')

  }); // end describe('html structure')

  describe('dynamic behavior', function() {
    //TODO
  }); // end describe('dynamic behavior')

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
      expect(calls.length).toBe(0, 'no POSTs on bot-page');

      expectNoUnmatchedCalls(fetchMock);
    }

    it('makes auth\'d API calls when a logged in user views landing page', function(done) {
      serveBot('running');
      loggedInBotPage((ele) => {
        let calls = fetchMock.calls(MATCHED, 'GET');
        expect(calls.length).toBe(2+3, '2 GETs from swarming-app, 3 from bot-page');
        // calls is an array of 2-length arrays with the first element
        // being the string of the url and the second element being
        // the options that were passed in
        const gets = calls.map((c) => c[0]);

        expect(gets).toContain(`/_ah/api/swarming/v1/bot/${TEST_BOT_ID}/get`);
        checkAuthorizationAndNoPosts(calls);
        done();
      });
    });

    it('can kill a running task', function(done) {
      serveBot('running');
      loggedInBotPage((ele) => {
        ele.permissions.cancel_task = true;
        ele.render();
        fetchMock.resetHistory();
        // This is the task_id on the 'running' bot.
        fetchMock.post('/_ah/api/swarming/v1/task/42fb00e06d95be11/cancel', {success: true});

        const killBtn = $$('main button.kill', ele);
        expect(killBtn).toBeTruthy();

        killBtn.click();

        const dialog = $$('.prompt-dialog', ele);
        expect(dialog).toBeTruthy();
        expect(dialog).toHaveClass('opened');

        const okBtn = $$('button.ok', dialog);
        expect(okBtn).toBeTruthy();

        okBtn.click();

        fetchMock.flush().then(() => {
          // MATCHED calls are calls that we expect and specified in the
          // beforeEach at the top of this file.
          let calls = fetchMock.calls(MATCHED, 'GET');
          expect(calls.length).toBe(0);
          calls = fetchMock.calls(MATCHED, 'POST');
          expect(calls.length).toBe(1);
          const call = calls[0];
          const options = call[1];
          expect(options.body).toEqual('{"kill_running":true}');

          expectNoUnmatchedCalls(fetchMock);
          done();
        });
      });
    });

    it('can terminate a non-dead bot', function(done) {
      serveBot('running');
      loggedInBotPage((ele) => {
        ele.permissions.terminate_bot = true;
        ele.render();
        fetchMock.resetHistory();
        // This is the task_id on the 'running' bot.
        fetchMock.post(`/_ah/api/swarming/v1/bot/${TEST_BOT_ID}/terminate`, {success: true});

        const tBtn = $$('main button.shut_down', ele);
        expect(tBtn).toBeTruthy();

        tBtn.click();

        const dialog = $$('.prompt-dialog', ele);
        expect(dialog).toBeTruthy();
        expect(dialog).toHaveClass('opened');

        const okBtn = $$('button.ok', dialog);
        expect(okBtn).toBeTruthy();

        okBtn.click();

        fetchMock.flush().then(() => {
          // MATCHED calls are calls that we expect and specified in the
          // beforeEach at the top of this file.
          let calls = fetchMock.calls(MATCHED, 'GET');
          expect(calls.length).toBe(0);
          calls = fetchMock.calls(MATCHED, 'POST');
          expect(calls.length).toBe(1);

          expectNoUnmatchedCalls(fetchMock);
          done();
        });
      });
    });
  }); // end describe('api calls')
});

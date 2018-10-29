// Copyright 2018 The LUCI Authors. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

import 'modules/bot-list'
import { deepCopy } from 'common-sk/modules/object'
import { $, $$ } from 'common-sk/modules/dom'

import { data_s10, fleetCount, fleetDimensions, queryCount } from 'modules/bot-list/test_data'
import { colHeaderMap, column, listQueryParams, processBots, processDimensions,
         processPrimaryMap } from 'modules/bot-list/bot-list-helpers'

describe('bot-list', function() {
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
      delete_bot: true
    });

    fetchMock.get('glob:/_ah/api/swarming/v1/bots/list?*', data_s10);
    fetchMock.get('/_ah/api/swarming/v1/bots/dimensions', fleetDimensions);
    fetchMock.get('/_ah/api/swarming/v1/bots/count', fleetCount);
    fetchMock.get('glob:/_ah/api/swarming/v1/bots/count?*', queryCount);

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
    // Note, this turns the default behavior of setTimeout and related.
    jasmine.clock().install();
    jasmine.clock().mockDate(new Date(Date.UTC(2018, 5, 14, 12, 46, 22, 1234)));
  });

  afterEach(function() {
    jasmine.clock().uninstall();
  });

  // calls the test callback with one element 'ele', a created <swarming-index>.
  // We can't put the describes inside the whenDefined callback because
  // that doesn't work on Firefox (and possibly other places).
  function createElement(test) {
    return window.customElements.whenDefined('bot-list').then(() => {
      container.innerHTML = `<bot-list client_id=for_test testing_offline=true></bot-list>`;
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
  // <bot-list> after the user has logged in.
  function loggedInBotlist(test) {
    createElement((ele) => {
      userLogsIn(ele, () => {
        test(ele);
      });
    });
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
          let loginMessage = $$('swarming-app>main .message', ele);
          expect(loginMessage).toBeTruthy();
          expect(loginMessage.hidden).toBeFalsy('Message should not be hidden');
          expect(loginMessage.textContent).toContain('must sign in');
          done();
        })
      })
      it('does not display filters or bots', function(done) {
        createElement((ele) => {
          let botTable = $$('.bot-table', ele);
          expect(botTable).toBeTruthy();
          expect(botTable.hidden).toBeTruthy('.bot-table should be hidden');
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
        fetchMock.get('glob:/_ah/api/swarming/v1/bots/list?*', 403,
                      { overwriteRoutes: true });
        fetchMock.get('/_ah/api/swarming/v1/bots/dimensions', 403,
                      { overwriteRoutes: true });
      }

      beforeEach(notAuthorized);

      it('tells the user they should change accounts', function(done) {
        loggedInBotlist((ele) => {
          let loginMessage = $$('swarming-app>main .message', ele);
          expect(loginMessage).toBeTruthy();
          expect(loginMessage.hidden).toBeFalsy('Message should not be hidden');
          expect(loginMessage.textContent).toContain('different account');
          done();
        });
      });
      it('does not display filters or bots', function(done) {
        loggedInBotlist((ele) => {
          let botTable = $$('.bot-table', ele);
          expect(botTable).toBeTruthy();
          expect(botTable.hidden).toBeTruthy('.bot-table should be hidden');

          let filters = $$('.filters', ele);
          expect(filters).toBeFalsy('.filters should not be shown');
          done();
        });
      });
    }); // end describe('when logged in as unauthorized user')

    describe('when logged in as user (not admin)', function() {

      describe('default landing page', function() {
        it('displays whatever bots show up', function(done) {
          loggedInBotlist((ele) => {
            let botRows = $('.bot-table .bot-row', ele);
            expect(botRows).toBeTruthy();
            expect(botRows.length).toBe(10, '(num botRows)');
            done();
          });
        });

        it('shows the default set of columns', function(done) {
          loggedInBotlist((ele) => {
            // ensure sorting is deterministic.
            ele._sort = 'id';
            ele._dir = 'asc';
            ele._verbose = false;
            ele.render();

            let colHeaders = $('.bot-table thead th', ele);
            expect(colHeaders).toBeTruthy();
            expect(colHeaders.length).toBe(4, '(num colHeaders)');
            expect(colHeaders[0].innerHTML).toContain('<more-vert-icon-sk');
            expect(colHeaders[0]).toMatchTextContent('Bot Id');
            expect(colHeaders[1]).toMatchTextContent('Current Task');
            expect(colHeaders[2]).toMatchTextContent('OS');
            expect(colHeaders[3]).toMatchTextContent('Status');

            let rows = $('.bot-table .bot-row', ele);
            expect(rows).toBeTruthy();
            expect(rows.length).toBe(10, '10 rows');

            let cols = $('.bot-table .bot-row td', ele);
            expect(cols).toBeTruthy();
            expect(cols.length).toBe(4 * 10, '4 columns * 10 rows');
            // little helper for readability
            let cell = (r, c) => cols[4*r+c];

            // Check the content of the first few rows (after sorting)
            expect(rows[0]).not.toHaveClass('dead');
            expect(rows[0]).not.toHaveClass('quarantined');
            expect(rows[0]).not.toHaveClass('old_version');
            expect(cell(0, 0)).toMatchTextContent('somebot10-a9');
            expect(cell(0, 0).innerHTML).toContain('<a ', 'has a link');
            expect(cell(0, 0).innerHTML).toContain('href="/bot?id=somebot10-a9"', 'link is correct');
            expect(cell(0, 1)).toMatchTextContent('idle');
            expect(cell(0, 1).innerHTML).not.toContain('<a ', 'no link');
            expect(cell(0, 2)).toMatchTextContent('Ubuntu-17.04');
            expect(cell(0, 3).textContent).toContain('Alive');

            expect(rows[1]).toHaveClass('quarantined');
            expect(cell(1, 0)).toMatchTextContent('somebot11-a9');
            expect(cell(1, 1)).toMatchTextContent('idle');
            expect(cell(1, 2)).toMatchTextContent('Android');
            expect(cell(1, 3).textContent).toContain('Quarantined');
            expect(cell(1, 3).textContent).toContain('[available, too_hot, low_battery]');

            expect(rows[2]).toHaveClass('dead');
            expect(rows[2]).toHaveClass('old_version');
            expect(cell(2, 0)).toMatchTextContent('somebot12-a9');
            expect(cell(2, 1)).toMatchTextContent('3e17182091d7ae11');
            expect(cell(2, 1).innerHTML).toContain('<a ', 'has a link');
            expect(cell(2, 1).innerHTML).toContain('href="/task?id=3e17182091d7ae10"',
                                      'link is pointing to the cannonical (0 ending) page');
            expect(cell(2, 1).innerHTML).toContain('title="Perf-Win10-Clang-Golo',
                                      'Mouseover with task name');
            expect(cell(2, 2)).toMatchTextContent('Windows-10-16299.431');
            expect(cell(2, 3).textContent).toContain('Dead');
            expect(cell(2, 3).textContent).toContain('Last seen 1w ago');

            expect(rows[3]).toHaveClass('maintenance');
            expect(cell(3, 3).textContent).toContain('Maintenance');
            expect(cell(3, 3).textContent).toContain('Need to re-format the hard drive.');

            expect(rows[4]).toHaveClass('old_version');
            done();
          });
        });

        it('updates the sort-toggles based on the current sort direction', function(done) {
          loggedInBotlist((ele) => {
            ele._sort = 'id';
            ele._dir = 'asc';
            ele.render();

            let sortToggles = $('.bot-table thead sort-toggle', ele);
            expect(sortToggles).toBeTruthy();
            expect(sortToggles.length).toBe(4, '(num sort-toggles)');

            expect(sortToggles[0].key).toBe('id');
            expect(sortToggles[0].currentKey).toBe('id');
            expect(sortToggles[0].direction).toBe('asc');
            // spot check one of the other ones
            expect(sortToggles[2].key).toBe('os');
            expect(sortToggles[2].currentKey).toBe('id');
            expect(sortToggles[2].direction).toBe('asc');

            ele._sort = 'task';
            ele._dir = 'desc';
            ele.render();

            expect(sortToggles[0].key).toBe('id');
            expect(sortToggles[0].currentKey).toBe('task');
            expect(sortToggles[0].direction).toBe('desc');

            expect(sortToggles[1].key).toBe('task');
            expect(sortToggles[1].currentKey).toBe('task');
            expect(sortToggles[1].direction).toBe('desc');
            done();
          });
        });

        it('displays counts', function(done) {
          loggedInBotlist((ele) => {
            let summaryTables = $('.summary table', ele);
            expect(summaryTables).toBeTruthy();
            expect(summaryTables.length).toBe(2, '(num summaryTables');
            let [fleetTable, queryTable] = summaryTables;
            // spot check some values
            let tds = $('tr:first-child td', fleetTable);
            expect(tds).toBeTruthy();
            expect(tds.length).toBe(2);
            expect(tds[0]).toMatchTextContent('All:');
            expect(tds[0].innerHTML).not.toContain('href');
            expect(tds[1]).toMatchTextContent('11434');

            tds = $('tr:nth-child(4) td', fleetTable);
            expect(tds).toBeTruthy();
            expect(tds.length).toBe(2);
            expect(tds[0]).toMatchTextContent('Idle:');
            expect(tds[0].innerHTML).toContain(encodeURIComponent('task:idle'));
            expect(tds[1]).toMatchTextContent('211');

            tds = $('tr:first-child td', queryTable);
            expect(tds).toBeTruthy();
            expect(tds.length).toBe(2);
            expect(tds[0]).toMatchTextContent('Displayed:');
            expect(tds[0].innerHTML).not.toContain('href');
            expect(tds[1]).toMatchTextContent('10');

            tds = $('tr:nth-child(3) td', queryTable);
            expect(tds).toBeTruthy();
            expect(tds.length).toBe(2);
            expect(tds[0]).toMatchTextContent('Alive:');
            expect(tds[0].innerHTML).toContain(encodeURIComponent('status:alive'));
            expect(tds[1]).toMatchTextContent('429');

            tds = $('tr:nth-child(8) td', queryTable);
            expect(tds).toBeTruthy();
            expect(tds.length).toBe(2);
            expect(tds[0]).toMatchTextContent('Maintenance:');
            expect(tds[0].innerHTML).toContain(encodeURIComponent('status:maintenance'));
            expect(tds[1]).toMatchTextContent('0');

            // by default fleet table should be hidden
            expect(fleetTable.parentElement).toHaveAttribute('hidden', 'fleet table of counts');
            expect(queryTable).not.toHaveAttribute('hidden', 'query table of counts');

            done();
          });
        });

        // disabled because it is causing flakes on Chrome
        // pushState doesn't appear to be synchronous on Chrome and thus
        // we have strange behavior.
        // https://bugs.chromium.org/p/chromium/issues/detail?id=510026
        xit('fills out the query params with the defaults', function(done) {
          let before = window.location.search;
          loggedInBotlist((ele) => {
              jasmine.clock().uninstall();
              // Run on a slight delay because the location doesn't seem to
              // be updated synchronously on some platforms (Chrome).
              setTimeout(() => {
                let queryParams = window.location.search
                console.log('before', before, 'after', queryParams);
                // The single letters are defined in bot-list constructor when
                // the stateReflector is set up.
                expect(queryParams).toContain('c=id');
                expect(queryParams).toContain('c=task');
                expect(queryParams).toContain('l=100');
                expect(queryParams).toContain('s=id');
                done();
              }, 50);
          });
        });

      }); // end describe('default landing page')

    });// end describe('when logged in as user')

  }); // end describe('html structure')

  describe('dynamic behavior', function() {
    // This is done w/o interacting with the sort-toggles because that is more
    // complicated with adding the event listener and so on.
    it('can stable sort in ascending order', function(done){
      loggedInBotlist((ele) => {
        ele._verbose = false;
        // First sort in descending id order
        ele._sort = 'id';
        ele._dir = 'desc';
        ele.render();
        // next sort in ascending os order
        ele._sort = 'os';
        ele._dir = 'asc';
        ele.render();

        let actualOSOrder = ele._bots.map((b) => column('os', b, ele));
        let actualIDOrder = ele._bots.map((b) => b.bot_id);

        expect(actualOSOrder).toEqual(['Android', 'Ubuntu-17.04', 'Ubuntu-17.04', 'Ubuntu-17.04',
          'Ubuntu-17.04', 'Ubuntu-17.04', 'Windows-10-16299.309', 'Windows-10-16299.309',
          'Windows-10-16299.431', 'Windows-10-16299.431']);
        expect(actualIDOrder).toEqual([
          'somebot11-a9', // Android
          'somebot77-a3', 'somebot15-a9', 'somebot13-a9', 'somebot13-a2', 'somebot10-a9', // Ubuntu in descending id
          'somebot17-a9', 'somebot16-a9',   // Win10.309
          'somebot18-a9', 'somebot12-a9']); // Win10.431
        done();
      });
    });

    it('can stable sort in descending order', function(done){
      loggedInBotlist((ele) => {
        ele._verbose = false;
        // First sort in asc id order
        ele._sort = 'id';
        ele._dir = 'asc';
        ele.render();
        // next sort in descending os order
        ele._sort = 'os';
        ele._dir = 'desc';
        ele.render();

        let actualOSOrder = ele._bots.map((b) => column('os', b, ele));
        let actualIDOrder = ele._bots.map((b) => b.bot_id);

        expect(actualOSOrder).toEqual(['Windows-10-16299.431', 'Windows-10-16299.431',
          'Windows-10-16299.309', 'Windows-10-16299.309', 'Ubuntu-17.04', 'Ubuntu-17.04',
          'Ubuntu-17.04', 'Ubuntu-17.04', 'Ubuntu-17.04', 'Android']);
        expect(actualIDOrder).toEqual([
          'somebot12-a9', 'somebot18-a9', // Win10.431
          'somebot16-a9', 'somebot17-a9', // Win10.309
          'somebot10-a9', 'somebot13-a2', 'somebot13-a9', 'somebot15-a9', 'somebot77-a3',  // Ubuntu in ascending id
          'somebot11-a9']); // Android
        done();
      });
    });

    it('toggles columns by clicking on the boxes in the "column selector"', function(done) {
      loggedInBotlist((ele) => {
        ele._cols = ['id', 'task', 'os', 'status'];
        ele._showColSelector = true;
        ele.render();

        let keySelector = $$('.col_selector', ele);
        expect(keySelector).toBeTruthy();

        // click on first non checked checkbox.
        let keyToClick = null;
        let checkbox = null;
        for (let i = 0; i < keySelector.children.length; i++) {
          let child = keySelector.children[i];
          checkbox = $$('checkbox-sk', child);
          keyToClick = $$('.key', child);
          if (checkbox && !checkbox.checked) {
            expect(keyToClick).toBeTruthy();
            keyToClick = keyToClick.textContent.trim();
            break;
          }
        }
        checkbox.click(); // click is synchronous, it returns after
                          // the clickHandler is run.
        // Check underlying data
        expect(ele._cols).toContain(keyToClick);
        // check the rendering changed
        let colHeaders = $('.bot-table thead th');
        expect(colHeaders).toBeTruthy();
        expect(colHeaders.length).toBe(5, '(num colHeaders)');
        let expectedHeader = colHeaderMap[keyToClick] || keyToClick;
        expect(colHeaders.map((c) => c.textContent.trim())).toContain(expectedHeader);

        // We have to find the checkbox again because the order
        // shuffles to keep selected ones on top.
        checkbox = null;
        for (let i = 0; i < keySelector.children.length; i++) {
          let child = keySelector.children[i];
          checkbox = $$('checkbox-sk', child);
          let key = $$('.key', child);
          if (key && key.textContent.trim() === keyToClick) {
            break;
          }
        }

        // click it again
        checkbox.click();

        // Check underlying data
        expect(ele._cols).not.toContain(keyToClick);
        // check the rendering changed
        colHeaders = $('.bot-table thead th');
        expect(colHeaders).toBeTruthy();
        expect(colHeaders.length).toBe(4, '(num colHeaders)');
        expectedHeader = colHeaderMap[keyToClick] || keyToClick;
        expect(colHeaders.map((c) => c.textContent.trim())).not.toContain(expectedHeader);
        done();
      });
    });

    it('toggles fleet data visibility', function(done) {
      loggedInBotlist((ele) => {
        ele._showFleetCounts = false;
        ele.render();

        let showMore = $$('.fleet_header.shower expand-more-icon-sk', ele);
        expect(showMore).toBeTruthy();
        let counts = $$('#fleet_counts').parentElement;
        expect(counts).toHaveAttribute('hidden');

        showMore.click();
        showMore = $$('.fleet_header.shower expand-more-icon-sk', ele);
        let showLess = $$('.fleet_header.hider expand-less-icon-sk', ele);
        expect(showLess).toBeTruthy();
        expect(showMore).toBeFalsy();
        expect(counts).not.toHaveAttribute('hidden');

        showLess.click();
        showMore = $$('.fleet_header.shower expand-more-icon-sk', ele);
        showLess = $$('.fleet_header.hider expand-less-icon-sk', ele);
        expect(showLess).toBeFalsy();
        expect(showMore).toBeTruthy();
        expect(counts).toHaveAttribute('hidden');

        done();
      });
    });

     // Disabled because it's causing some flakes on Chrome.
     xit('reads some values from query params', function(done) {
       jasmine.clock().uninstall();
       history.pushState(null, '', window.location.origin + window.location.pathname + '?'+
                                   's=delta&l=200&c=epsilon&c=zeta&c=theta');
       // Run on a slight delay because the location doesn't seem to
       // be updated synchronously on some platforms (Chrome).
       setTimeout(() => {
         loggedInBotlist((ele) => {
             expect(ele._sort).toBe('delta');
             expect(ele._limit).toBe(200);
             expect(ele._cols).toEqual(['epsilon', 'theta', 'zeta']);

             done();
         });
       }, 50);
     });

    // Disabled because it's causing some flakes on Chrome.
    xit('updates query params on render()', function(done) {
      loggedInBotlist((ele) => {
          ele._cols = ['id', 'task', 'os', 'gamma'];
          ele._filters = ['alpha:beta'];
          ele.render();

          let queryParams = window.location.search
          expect(queryParams).toContain('c=id');
          expect(queryParams).not.toContain('c=epsilon');
          expect(queryParams).toContain('c=gamma');
          expect(queryParams).toContain('l=100');
          expect(queryParams).toContain(`f=${encodeURIComponent('alpha:beta')}`);
          expect(queryParams).not.toContain(`f=${encodeURIComponent('alpha:omega')}`);

          ele._cols = ['id', 'task', 'os', 'epsilon'];
          ele._filters = ['alpha:omega'];
          ele.render();

          queryParams = window.location.search
          expect(queryParams).toContain('c=id');
          expect(queryParams).toContain('c=epsilon');
          expect(queryParams).not.toContain('c=gamma');
          expect(queryParams).toContain('l=100');
          expect(queryParams).not.toContain(`f=${encodeURIComponent('alpha:beta')}`);
          expect(queryParams).toContain(`f=${encodeURIComponent('alpha:omega')}`);

          done();
      });
    });

    it('sorts columns so they are mostly alphabetical', function(done) {
      loggedInBotlist((ele) => {
        ele._cols = ['status', 'os', 'id', 'xcode_version', 'task', 'android_devices'];
        ele.render();

        let expectedOrder = ['id', 'task', 'android_devices', 'os', 'status', 'xcode_version'];
        expect(ele._cols).toEqual(expectedOrder);

        let expectedHeaders = ['Bot Id', 'Current Task', 'Android Devices', 'OS',
                               'Status', 'XCode Version'];
        let colHeaders = $('.bot-table thead th');
        expect(colHeaders.map((c) => c.textContent.trim())).toEqual(expectedHeaders);
        done();
      });
    });

    function getChildItemWithText(ele, value) {
      expect(ele).toBeTruthy();

      for (let i = 0; i < ele.children.length; i++) {
        let child = ele.children[i];
        let text = child.firstElementChild;
        if (text && text.textContent.trim() === value) {
          return child;
        }
      }
      // uncomment below when debugging
      // fail(`Could not find child of ${ele} with text value ${value}`);
      return null;
    }

    it('does not toggle forced columns (like "id")', function(done) {
      loggedInBotlist((ele) => {
        ele._cols = ['id', 'task', 'os', 'status'];
        ele._showColSelector = true;
        ele.render();

        let row = getChildItemWithText($$('.col_selector'), 'id', ele);
        let checkbox = $$('checkbox-sk', row);
        expect(checkbox.checked).toBeTruthy();
        checkbox.click(); // click is synchronous, it returns after
                          // the clickHandler is run.
        // Check underlying data
        expect(ele._cols).toContain('id');
        // check there are still headers.
        let colHeaders = $('.bot-table thead th');
        expect(colHeaders).toBeTruthy();
        expect(colHeaders.length).toBe(4, '(num colHeaders)');
        done();
      });
    });

    function childrenAsArray(ele) {
      return Array.prototype.slice.call(ele.children);
    }

    it('shows values when a key row is selected', function(done) {
      loggedInBotlist((ele) => {
        ele._cols = ['id', 'task', 'os', 'status'];
        ele.render();
        let row = getChildItemWithText($$('.selector.keys'), 'cpu', ele);
        expect(row).toBeTruthy();
        row.click();
        expect(row.hasAttribute('selected')).toBeTruthy();
        expect(ele._primaryKey).toBe('cpu');

        let valueSelector = $$('.selector.values');
        expect(valueSelector).toBeTruthy();
        let values = childrenAsArray(valueSelector).map((c) => c.textContent.trim());
        // spot check
        expect(values.length).toBe(8);
        expect(values).toContain('arm-32');
        expect(values).toContain('x86-64');

        let oldRow = row;
        row = getChildItemWithText($$('.selector.keys'), 'device_os', ele);
        expect(row).toBeTruthy();
        row.click();
        expect(row.hasAttribute('selected')).toBeTruthy('new row only one selected');
        expect(oldRow.hasAttribute('selected')).toBeFalsy('old row unselected');
        expect(ele._primaryKey).toBe('device_os');

        valueSelector = $$('.selector.values');
        expect(valueSelector).toBeTruthy();
        values = childrenAsArray(valueSelector).map((c) => c.textContent.trim());
        // spot check
        expect(values.length).toBe(19);
        expect(values).toContain('none');
        expect(values).toContain('LMY49K.LZC89');

        done();
      });
    });

    it('orders columns alphabetically with selected cols on top', function(done) {
      loggedInBotlist((ele) => {
        ele._cols = ['id', 'os', 'task', 'status'];
        ele._showColSelector = true;
        ele._refilterPossibleColumns(); // also calls render

        let keySelector = $$('.col_selector');
        expect(keySelector).toBeTruthy();
        let keys = childrenAsArray(keySelector).map((c) => c.textContent.trim());

        // Skip the first child, which is the input box
        expect(keys.slice(1, 7)).toEqual(['id', 'task', 'os', 'status',
                                          'android_devices', 'battery_health']);

        done();
      });
    });

    it('adds a filter when the addIcon is clicked', function(done) {
      loggedInBotlist((ele) => {
        ele._cols = ['id', 'task', 'os', 'status'];
        ele._primaryKey = 'os';  // set 'os' selected
        ele._filters = [];  // no filters
        ele.render();

        let valueRow = getChildItemWithText($$('.selector.values'), 'Android', ele);
        let addIcon = $$('add-circle-icon-sk', valueRow);
        expect(addIcon).toBeTruthy('there should be an icon for adding');
        addIcon.click();

        expect(ele._filters.length).toBe(1, 'a filter should be added');
        expect(ele._filters[0]).toEqual('os:Android');

        let chipContainer = $$('.chip_container', ele);
        expect(chipContainer).toBeTruthy('there should be a filter chip container');
        expect(chipContainer.children.length).toBe(1);
        expect(addIcon.hasAttribute('hidden'))
              .toBeTruthy('addIcon should go away after being clicked');
        done();
      });
    });

    it('removes a filter when the circle clicked', function(done) {
      loggedInBotlist((ele) => {
        ele._cols = ['id', 'task', 'os', 'status'];
        ele._primaryKey = 'id';
        ele._filters = ['device_type:bullhead', 'os:Android'];
        ele.render();

        let filterChip = getChildItemWithText($$('.chip_container'), 'os:Android', ele);
        let icon = $$('cancel-icon-sk', filterChip);
        expect(icon).toBeTruthy('there should be a icon to remove it');
        icon.click();

        expect(ele._filters.length).toBe(1, 'a filter should be removed');
        expect(ele._filters[0]).toEqual('device_type:bullhead', 'os:Android should be removed');

        let chipContainer = $$('.chip_container', ele);
        expect(chipContainer).toBeTruthy('there should be a filter chip container');
        expect(chipContainer.children.length).toBe(1);
        done();
      });
    });

    it('shows and hides the column selector', function(done) {
      loggedInBotlist((ele) => {
        ele._showColSelector = false;
        ele.render();

        let cs = $$('.col_selector', ele);
        expect(cs).toBeFalsy();

        let toClick = $$('.col_options', ele);
        expect(toClick).toBeTruthy('Thing to click to show col selector');
        toClick.click();

        cs = $$('.col_selector', ele);
        expect(cs).toBeTruthy();

        // click anywhere else to hide the column selector
        toClick = $$('.header', ele);
        expect(toClick).toBeTruthy('Thing to click to hide col selector');
        toClick.click();

        cs = $$('.col_selector', ele);
        expect(cs).toBeFalsy();

        done();
      });
    });

    it('filters the data it has when waiting for another request', function(done) {
      loggedInBotlist((ele) => {
        ele._cols = ['id', 'task', 'os', 'status'];
        ele._filters = [];
        ele.render();

        expect(ele._bots.length).toBe(10, 'All 10 at the start');

        let wasCalled = false;
        fetchMock.get('glob:/_ah/api/swarming/v1/bots/list?*', () => {
          expect(ele._bots.length).toBe(5, '5 Linux bots there now.');
          wasCalled = true;
          return '[]'; // pretend no bots match
        }, { overwriteRoutes: true });

        ele._addFilter('os:Linux');
        // The true on flush waits for res.json() to resolve too, which
        // is when we know the element has updated the _bots.
        fetchMock.flush(true).then(() => {
          expect(wasCalled).toBeTruthy();
          expect(ele._bots.length).toBe(0, 'none were actually returned');

          done();
        });
      });
    });

    it('allows filters to be added from the search box', function(done) {
      loggedInBotlist((ele) => {
        ele._filters = [];
        ele.render();

        let filterInput = $$('#filter_search', ele);
        filterInput.value = 'invalid filter';
        ele._filterSearch({key: 'Enter'});
        expect(ele._filters).toEqual([]);
        // Leave the input to let the user correct their mistake.
        expect(filterInput.value).toEqual('invalid filter');

        // Spy on the list call to make sure a request is made with the
        let calledTimes = 0;
        fetchMock.get('glob:/_ah/api/swarming/v1/bots/list?*', (url, _) => {
          expect(url).toContain(encodeURIComponent('valid:filter:gpu:can:have:many:colons'));
          calledTimes++;
          return '[]'; // pretend no bots match
        }, {overwriteRoutes: true});

        filterInput.value = 'valid:filter:gpu:can:have:many:colons';
        ele._filterSearch({key: 'Enter'});
        expect(ele._filters).toEqual(['valid:filter:gpu:can:have:many:colons']);
        // Empty the input for next time.
        expect(filterInput.value).toEqual('');
        filterInput.value = 'valid:filter:gpu:can:have:many:colons';
        ele._filterSearch({key: 'Enter'});
        // No duplicates
        expect(ele._filters).toEqual(['valid:filter:gpu:can:have:many:colons']);

        fetchMock.flush(true).then(() => {
          expect(calledTimes).toEqual(1, 'Only request bots once');

          done();
        });
      });
    });

    it('searches filters by typing in the text box', function(done) {
      loggedInBotlist((ele) => {
        ele._filters = [];
        ele.render();

        let filterInput = $$('#filter_search', ele);
        filterInput.value = 'dev';
        ele._refilterPrimaryKeys();

        // Auto selects the first one
        expect(ele._primaryKey).toEqual('android_devices');

        let row = getChildItemWithText($$('.selector.keys'), 'cpu', ele);
        expect(row).toBeFalsy('cpu should be hiding');
        row = getChildItemWithText($$('.selector.keys'), 'device_type', ele);
        expect(row).toBeTruthy('device_type should be there');

        done();
      });
    });

    it('searches columns by typing in the text box', function(done) {
      loggedInBotlist((ele) => {
        ele._cols = ['id', 'task', 'os', 'status'];
        ele._showColSelector = true;
        ele.render();

        let columnInput = $$('#column_search', ele);
        columnInput.value = 'batt';
        ele._refilterPossibleColumns();

        let colSelector = $$('.col_selector', ele);
        expect(colSelector).toBeTruthy();
        expect(colSelector.children.length).toEqual(6);

        let row = getChildItemWithText(colSelector, 'gpu');
        expect(row).toBeFalsy('gpu should be hiding');
        row = getChildItemWithText(colSelector, 'battery_status');
        expect(row).toBeTruthy('battery_status should be there');

        done();
      });
    });

    it('makes certain columns more readable', function(done) {
      loggedInBotlist((ele) => {
        ele._cols = ['id', 'mp_lease_expires', 'mp_lease_id', 'uptime'];
        ele._sort = 'mp_lease_expires';
        ele._dir = 'desc';
        ele.render();

        let rows = $('.bot-table .bot-row', ele);
        expect(rows).toBeTruthy();
        expect(rows.length).toBe(10, '10 rows');

        let cols = $('.bot-table .bot-row td', ele);
        expect(cols).toBeTruthy();
        expect(cols.length).toBe(4 * 10, '4 columns * 10 rows');
        // little helper for readability
        let cell = (r, c) => cols[4*r+c];

        // Check the content of the first few rows (after sorting)
        expect(cell(0, 0)).toMatchTextContent('somebot18-a9');
        expect(cell(0, 1)).toMatchTextContent('in 71w');
        expect(cell(0, 2).innerHTML).toContain('<a ', 'has an mp link');
        expect(cell(0, 2).innerHTML).toContain('href="https://example.com/leases/22596363b3de40b06f981fb85d82312e8c0ed511"', 'link is correct');
        expect(cell(0, 3)).toMatchTextContent('7h  1m 48s');

        expect(cell(1, 0)).toMatchTextContent('somebot10-a9');
        expect(cell(1, 1)).toMatchTextContent('N/A');
        expect(cell(1, 2).innerHTML).not.toContain('<a ', 'no mp link');
        expect(cell(1, 3)).toMatchTextContent('5h 53m 32s');
        done();
      });
    });

    it('applies aliases to certain columns', function(done) {
      loggedInBotlist((ele) => {
        ele._cols = ['id', 'device_type', 'gpu'];
        ele._sort = 'device_type';
        ele._dir = 'asc';
        ele.render();

        let rows = $('.bot-table .bot-row', ele);
        expect(rows).toBeTruthy();
        expect(rows.length).toBe(10, '10 rows');

        let cols = $('.bot-table .bot-row td', ele);
        expect(cols).toBeTruthy();
        expect(cols.length).toBe(3 * 10, '3 columns * 10 rows');
        // little helper for readability
        let cell = (r, c) => cols[3*r+c];

        // Check the content of the first few rows (after sorting)
        expect(cell(0, 0)).toMatchTextContent('somebot11-a9');
        expect(cell(0, 1)).toMatchTextContent('Nexus 5X (bullhead)');
        expect(cell(0, 2)).toMatchTextContent('none');

        expect(cell(1, 0)).toMatchTextContent('somebot10-a9');
        expect(cell(1, 1)).toMatchTextContent('none');
        expect(cell(1, 2)).toMatchTextContent('NVIDIA Quadro P400 (10de:1cb3-384.59)');
        done();
      });
    });

    it('applies aliases in the filter slots', function(done) {
      loggedInBotlist((ele) => {
        ele._primaryKey = 'gpu';
        ele.render();

        let values = $$('.values.selector', ele);
        expect(values).toBeTruthy();
        // spot check
        expect(values.children[0]).toMatchTextContent('NVIDIA (10de)');
        expect(values.children[8]).toMatchTextContent('Matrox MGA G200e (102b:0522)');
        done();
      });
    });

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
      expect(calls.length).toBe(0, 'no POSTs on bot-list');

      expectNoUnmatchedCalls();
    }

    it('makes auth\'d API calls when a logged in user views landing page', function(done) {
      loggedInBotlist((ele) => {
        let calls = fetchMock.calls(MATCHED, 'GET');
        expect(calls.length).toBe(2+4, '2 GETs from swarming-app, 4 from bot-list');
        // calls is an array of 2-length arrays with the first element
        // being the string of the url and the second element being
        // the options that were passed in
        let gets = calls.map((c) => c[0]);

        // limit=100 comes from the default limit value.
        expect(gets).toContainRegex(/\/_ah\/api\/swarming\/v1\/bots\/list.+limit=100.*/);
        expect(gets).toContain('/_ah/api/swarming/v1/bots/count');

        checkAuthorizationAndNoPosts(calls)
        done();
      });
    });

    it('makes requests on adding new filters and removing them.', function(done) {
      loggedInBotlist((ele) => {
        fetchMock.resetHistory();
        ele._addFilter('alpha:beta');

        let calls = fetchMock.calls(MATCHED, 'GET');
        expect(calls.length).toBe(2, '1 for the bots, 1 for the count');
        // calls is an array of 2-length arrays with the first element
        // being the string of the url and the second element being
        // the options that were passed in
        let gets = calls.map((c) => c[0]);

        expect(gets).toContainRegex(/\/_ah\/api\/swarming\/v1\/bots\/list.+dimensions=alpha%3Abeta.*/);
        expect(gets).toContainRegex(/\/_ah\/api\/swarming\/v1\/bots\/count.+dimensions=alpha%3Abeta.*/);
        checkAuthorizationAndNoPosts(calls);

        fetchMock.resetHistory();
        ele._removeFilter('alpha:beta');

        calls = fetchMock.calls(MATCHED, 'GET');
        gets = calls.map((c) => c[0]);
        expect(gets).not.toContainRegex(/\/_ah\/api\/swarming\/v1\/bots\/list.+alpha%3Abeta.*/);
        expect(gets).not.toContainRegex(/\/_ah\/api\/swarming\/v1\/bots\/count.+alpha%3Abeta.*/);

        checkAuthorizationAndNoPosts(calls);
        done();
      });
    });
  }); // end describe('api calls')

  describe('data parsing', function() {
    const LINUX_BOT = data_s10.items[0];
    const MULTI_ANDROID_BOT = data_s10.items[2];

    it('inflates the state', function() {
      // Make a copy of the object because _processBots will modify it in place.
      let bots = processBots([deepCopy(LINUX_BOT)]);
      expect(bots).toBeTruthy();
      expect(bots.length).toBe(1);
      expect(typeof bots[0].state).toBe('object');
    });

    it('makes a disk cache using the free space of disks', function() {
      // Make a copy of the object because _processBots will modify it in place.
      let bots = processBots([deepCopy(LINUX_BOT)]);
      let disks = bots[0].disks;
      expect(disks).toBeTruthy();
      expect(disks.length).toBe(2, 'Two disks');
      expect(disks[0]).toEqual({id: '/', mb: 680751.3}, 'biggest disk first');
      expect(disks[1]).toEqual({id: '/boot', mb: 842.2});
    });

    it('aggregates the temperatures of the host bot', function() {
      // Make a copy of the object because _processBots will modify it in place.
      let bots = processBots([deepCopy(LINUX_BOT)]);
      let temp = bots[0].state.temp;
      expect(temp).toBeTruthy();
      expect(temp.average).toBe('34.8', 'rounds to one decimal place');
      expect(temp.zones).toBe('thermal_zone0: 34.5 | thermal_zone1: 35', 'joins with |');
    });

    it('turns the dates into DateObjects', function() {
      // Make a copy of the object because _processBots will modify it in place.
      let bots = processBots([deepCopy(LINUX_BOT)]);
      let ts = bots[0].first_seen_ts
      expect(ts).toBeTruthy();
      expect(ts instanceof Date).toBeTruthy('Should be a date object');
    });

    it('turns the device map into a list', function() {
      // Make a copy of the object because _processBots will modify it in place.
      let bots = processBots([deepCopy(MULTI_ANDROID_BOT)]);
      let devices = bots[0].state.devices;
      expect(devices).toBeTruthy();
      expect(devices.length).toBe(3, '3 devices attached to this bot');

      expect(devices[0].serial).toBe('3456789ABC', 'alphabetical by serial');
      expect(devices[0].okay).toBeTruthy();
      expect(devices[0].device_type).toBe('bullhead');
      expect(devices[0].temp.average).toBe('34.4');

      expect(devices[1].serial).toBe('89ABCDEF012', 'alphabetical by serial');
      expect(devices[1].okay).toBeFalsy();
      expect(devices[1].device_type).toBe('bullhead');
      expect(devices[1].temp.average).toBe('36.2');

      expect(devices[2].serial).toBe('Z01234567', 'alphabetical by serial');
      expect(devices[2].okay).toBeFalsy();
      expect(devices[2].device_type).toBe('bullhead');
      expect(devices[2].temp.average).toBe('34.3');
    });

    it('turns the dimension map into a list', function() {
      // processDimensions may modify the passed in variable.
      let dimensions = processDimensions(deepCopy(fleetDimensions.bots_dimensions));

      expect(dimensions).toBeTruthy();
      expect(dimensions.length).toBe(14);
      expect(dimensions).toContain('id');
      expect(dimensions).toContain('cores');
      expect(dimensions).toContain('device_type');
      expect(dimensions).toContain('xcode_version');
      expect(dimensions).not.toContain('error');
    });

    it('gracefully handles null data', function() {
      // processDimensions may modify the passed in variable.
      let dimensions = processDimensions(null);

      expect(dimensions).toBeTruthy();
      expect(dimensions.length).toBe(0);

      let bots = processBots(null);

      expect(bots).toBeTruthy();
      expect(bots.length).toBe(0);
    });

    it('extracts the key->value map', function() {
      // processDimensions may modify the passed in variable.
      let pMap = processPrimaryMap(deepCopy(fleetDimensions.bots_dimensions));

      expect(pMap).toBeTruthy();
      // Note this list doesn't include the blacklisted keys.
      let expectedKeys = ['android_devices', 'cores', 'cpu', 'device', 'device_os',
          'device_type', 'gpu', 'hidpi', 'machine_type', 'os', 'pool',
          'xcode_version', 'zone', 'id', 'task', 'status', 'is_mp_bot'];
      let actualKeys = Object.keys(pMap);
      actualKeys.sort();
      expectedKeys.sort();
      expect(expectedKeys).toEqual(actualKeys);

      // Spot check a few values of the keys.
      let expected = ['0', '1', '2', '3', '4', '5', '6', '7'];
      let actual = pMap['android_devices'];
      actual.sort();
      expect(expected).toEqual(actual, 'android_devices');

      expected = ['0', '1'];
      actual = pMap['hidpi'];
      actual.sort();
      expect(expected).toEqual(actual, 'hidpi');

      // Spot check custom options
      expect(pMap['status']).toBeTruthy();
      expect(pMap['status']).toContain('alive');
      expect(pMap['is_mp_bot']).toBeTruthy();
      expect(pMap['is_mp_bot']).toContain('false');
      expect(pMap['id']).toEqual(null);
    });

    it('correctly makes query params from filters', function() {
      // We know query.fromObject is used and it puts the query params in
      // a deterministic, sorted order. This means we can compare
      let expectations = [
        { // basic 'alive'
          'limit': 256,
          'filters': ['pool:Skia','os:Android', 'status:alive'],
          'output':  'dimensions=pool%3ASkia&dimensions=os%3AAndroid'+
                     '&in_maintenance=FALSE&is_dead=FALSE&limit=256&quarantined=FALSE',
        },
        { // no filters
          'limit': 123,
          'filters': [],
          'output':  'limit=123',
        },
        { // dead
          'limit': 456,
          'filters': ['status:dead', 'device_type:bullhead'],
          'output':  'dimensions=device_type%3Abullhead&is_dead=TRUE&limit=456',
        },
        { // multiple of a filter
          'limit': 789,
          'filters': ['status:maintenance', 'device_type:bullhead', 'device_type:marlin'],
          'output':  'dimensions=device_type%3Abullhead&dimensions=device_type%3Amarlin'+
                     '&in_maintenance=TRUE&limit=789',
        },
        { // is_mp_bot
          'limit': 2,
          'filters': ['status:quarantined', 'is_mp_bot:false'],
          'output':  'is_mp=FALSE&limit=2&quarantined=TRUE',
        }
      ];

      for (let testcase of expectations) {
        let qp = listQueryParams(testcase.filters, testcase.limit);
        expect(qp).toEqual(testcase.output);
      }

      let testcase = expectations[0];
      let qp = listQueryParams(testcase.filters, testcase.limit, 'mock_cursor12345');
      expect(qp).toEqual('cursor=mock_cursor12345&'+testcase.output);

    });
  }); // end describe('data parsing')

});

// Copyright 2019 The LUCI Authors. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

/** @module swarming-ui/modules/task-list
 * @description <h2><code>bot-list</code></h2>
 *
 * <p>
 *  Task List shows a filterable list of all swarming tasks.
 * </p>
 *
 * <p>This is a top-level element.</p>
 *
 * @attr client_id - The Client ID for authenticating via OAuth.
 * @attr testing_offline - If true, the real OAuth flow won't be used.
 *    Instead, dummy data will be used. Ideal for local testing.
 */

import { html, render } from 'lit-html'
import { jsonOrThrow } from 'common-sk/modules/jsonOrThrow'

import 'elements-sk/icon/more-vert-icon-sk'
import '../swarming-app'
import '../sort-toggle'

import { column, colHeaderMap, processTasks, sortColumns } from './task-list-helpers'
import SwarmingAppBoilerplate from '../SwarmingAppBoilerplate'


const colHead = (col, ele) => html`
<th>${colHeaderMap[col] || col}
  <sort-toggle .key=${col} .currentKey=${ele._sort} .direction=${ele._dir}>
  </sort-toggle>
</th>`;

const taskCol = (col, bot, ele) => html`
<td>${column(col, bot, ele)}</td>`;

const taskRow = (task, ele) => html`
<tr class="task-row">
  ${ele._cols.map((col) => taskCol(col, task, ele))}
</tr>`;

const col_options = (ele, firstCol) => html`
<!-- Put the click action here to make it bigger, especially for mobile.-->
<th class=col_options @click=${ele._toggleColSelector}>
  <span class=show_widget>
    <more-vert-icon-sk></more-vert-icon-sk>
  </span>
  <span>${colHeaderMap[firstCol] || firstCol}</span>
  <sort-toggle @click=${e => (e.stopPropagation() && e.preventDefault())}
               key=id .currentKey=${ele._sort} .direction=${ele._dir}>
  </sort-toggle>
  <!--{col_selector(ele)}-->
</th>`;

const template = (ele) => html`
<swarming-app id=swapp
              client_id=${ele.client_id}
              ?testing_offline=${ele.testing_offline}>
  <header>
    <div class=title>Swarming Task List</div>
      <aside class=hideable>
        <a href=/>Home</a>
        <a href=/botlist>Bot List</a>
        <a href=/oldui/tasklist>Old Task List</a>
        <a href=/tasklist>Task List</a>
      </aside>
  </header>
  <main>
    <h2 class=message ?hidden=${ele.loggedInAndAuthorized}>${ele._message}</h2>

    <table class=task-table ?hidden=${!ele.loggedInAndAuthorized}>
      <thead>
        <tr>
          <tr>
          ${col_options(ele, ele._cols[0])}
          <!-- Slice off the first column so we can
               have a custom first box (including the widget to select columns).
            -->
          ${ele._cols.slice(1).map((col) => colHead(col, ele))}
        </tr>
        </tr>
      </thead>
      <tbody>${ele._tasks.map((task) => taskRow(task, ele))}</tbody>
    </table>

  </main>
  <footer></footer>
</swarming-app>`;

// How many items to load on the first load of tasks
// This is a relatively low number to make the initial page load
// seem snappier. After this, we can go up (see BATCH LOAD) to
// reduce the number of queries, since the user can expect to wait
// a bit more when their interaction (e.g. adding a filter) causes
// more data to be fetched.
const INITIAL_LOAD = 100;
// How many items to load on subsequent fetches.
// This number was picked from experience and experimentation.
const BATCH_LOAD = 200;

window.customElements.define('task-list', class extends SwarmingAppBoilerplate {

  constructor() {
    super(template);
    this._tasks = [];
    this._forcedColumns = ['name'];

    this._cols = ['name', 'state', 'bot', 'created_ts', 'pending_time',
                  'duration', 'pool'];

    this._filters = [];

    this._limit = 0;
    this._showAll = true;

    this._message = 'You must sign in to see anything useful.';
    this._fetchController = null;
  }

  connectedCallback() {
    super.connectedCallback();

    this._loginEvent =  (e) => {
      this._fetch();
      this.render();
    };
    this.addEventListener('log-in', this._loginEvent);
  }

  disconnectedCallback() {
    super.disconnectedCallback();

    this.removeEventListener('log-in', this._loginEvent);
  }

  _fetch() {
    if (!this.loggedInAndAuthorized) {
      return;
    }
    if (this._fetchController) {
      // Kill any outstanding requests that use the filters
      this._fetchController.abort();
    }
    // Make a fresh abort controller for each set of fetches. AFAIK, they
    // cannot be re-used once aborted.
    this._fetchController = new AbortController();
    let extra = {
      headers: {'authorization': this.auth_header},
      signal: this._fetchController.signal,
    };
    // Fetch the tasks
    this.app.addBusyTasks(1);
    let queryParams = `?limit=${INITIAL_LOAD}`; // TODO
    fetch(`/_ah/api/swarming/v1/tasks/list?${queryParams}`, extra)
      .then(jsonOrThrow)
      .then((json) => {
        this._tasks = [];
        const maybeLoadMore = (json) => {
          this._tasks = this._tasks.concat(processTasks(json.items));
          this.render();
          // Special case: Don't load all the tasks when filters is empty to avoid
          // loading many many tasks unintentionally. A user can over-ride this
          // with the showAll button.
          if ((this._filters.length || this._showAll) && json.cursor) {
            this._limit = BATCH_LOAD;
            queryParams = `?limit=${BATCH_LOAD}&json.cursor`; // TODO
            fetch(`/_ah/api/swarming/v1/tasks/list?${queryParams}`, extra)
              .then(jsonOrThrow)
              .then(maybeLoadMore)
              .catch((e) => this.fetchError(e, 'tasks/list (paging)'));
          } else {
            this.app.finishedTask();
          }
        }
        maybeLoadMore(json);
      })
      .catch((e) => this.fetchError(e, 'tasks/list'));
  }

  render() {
    // Incorporate any data changes before rendering.
    sortColumns(this._cols);
    super.render();
  }

});

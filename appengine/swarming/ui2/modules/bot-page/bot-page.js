// Copyright 2019 The LUCI Authors. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

import { $, $$ } from 'common-sk/modules/dom'
import { errorMessage } from 'elements-sk/errorMessage'
import { html, render } from 'lit-html'
import { ifDefined } from 'lit-html/directives/if-defined'
import { jsonOrThrow } from 'common-sk/modules/jsonOrThrow'

import 'elements-sk/checkbox-sk'
import 'elements-sk/icon/add-circle-outline-icon-sk'
import 'elements-sk/icon/remove-circle-outline-icon-sk'
import 'elements-sk/styles/buttons'
import '../dialog-pop-over'
import '../swarming-app'

import { EVENTS_QUERY_PARAMS, parseBotData, parseEvents,
         parseTasks, TASKS_QUERY_PARAMS } from './bot-page-helpers'
import { stateClass as taskClass } from '../task-page/task-page-helpers'
import { timeDiffApprox, timeDiffExact, taskPageLink } from '../util'
import SwarmingAppBoilerplate from '../SwarmingAppBoilerplate'

/**
 * @module swarming-ui/modules/bot-page
 * @description <h2><code>bot-page<code></h2>
 *
 * <p>
 *   Bot Page shows the information about a bot, including events and tasks.
 * </p>
 *
 * <p>This is a top-level element.</p>
 *
 * @attr client_id - The Client ID for authenticating via OAuth.
 * @attr testing_offline - If true, the real OAuth flow won't be used.
 *    Instead, dummy data will be used. Ideal for local testing.
 */

const idAndButtons = (ele) => {
  if (!ele._botId) {
    return html`
<div class=id_buttons>
  <input id=id_input placeholder="Bot ID" @change=${ele._updateID}></input>
  <span class=message>Enter a Bot ID to get started.</span>
</div>`;
  }
  return html`
<div class=id_buttons>
  <input id=id_input placeholder="Bot ID" @change=${ele._updateID}></input>
  <button title="Refresh data"
          @click=${ele._fetch}>refresh</button>
</div>`;
}

const statusAndTask = (ele, bot) => {
  if (!ele._botId) {
    return '';
  }
  return html`
<tr>
  <td>Last Seen</td>
  <td title=${bot.human_last_seen_ts}>${timeDiffExact(bot.last_seen_ts)} ago</td>
  <td>
      <button class=shut_down
            ?hidden=${bot.is_dead}
            ?disabled=${!ele.permissions.terminate_bot}
            @click=${ele._promptShutdown}>
        Shut down gracefully
      </button>
    </td>
</tr>
<tr>
  <td>Current Task</td>
  <td>
    <a target=_blank rel=noopener
        href="${ifDefined(taskPageLink(bot.task_id))}">
      ${bot.task_id || 'idle'}
    </a>
  </td>
  <td>
    <button class=kill
            ?hidden=${!bot.task_id}
            ?disabled=${!ele.permissions.cancel_task}
            @click=${ele._promptKill}>
        Kill task
      </button>
  </td>
</tr>`;
}

const dimensionBlock = (dimensions) => html`
<tr>
  <td rowspan=${dimensions.length+1}>
    Dimensions <!-- TODO add link -->
  </td>
</tr>
${dimensions.map(dimensionRow)}
`;

const dimensionRow = (dimension) => html`
<tr>
  <td>${dimension.key}</td>
  <td>${dimension.value.join(' | ')}</td>
</tr>
`;

const dataAndMPBlock = (ele, bot) => html`
<tr title="IP address that the server saw the connection from.">
  <td>External IP</td>
  <td colspan=2><a href=${'http://'+bot.external_ip}>${bot.external_ip}</a></td>
</tr>
<tr class=${ele.server_details.bot_version === bot.version ? '' : 'old_version'}
    title="Version is based on the content of swarming_bot.zip which is the swarming bot code.
           The bot won't update if quarantined, dead, or busy.">
  <td>Bot Version</td>
  <td colspan=2>${bot.version && bot.version.substring(0, 10)}</td>
</tr>
<tr title="The version the server expects the bot to be using.">
  <td>Expected Bot Version</td>
  <td colspan=2>${ele.server_details.bot_version &&
                  ele.server_details.bot_version.substring(0, 10)}</td>
</tr>
<tr title="First time ever a bot with this id contacted the server.">
  <td>First seen</td>
  <td colspan=2 title=${bot.human_first_seen_ts}>
    ${timeDiffApprox(bot.first_seen_ts)} ago
  </td>
</tr>
<tr title="How the bot is authenticated by the server.">
  <td>Authenticated as</td>
  <td colspan=2>${bot.authenticated_as}</td>
</tr>
<tr ?hidden=${!bot.lease_id}>
  <td>Machine Provider Lease ID</td>
  <td colspan=2>
    <a href$="[[_mpLink(_bot,_server_details.machine_provider_template)]]">
      ${bot.lease_id}
    </a>
  </td>
</tr>
<tr ?hidden=${!bot.lease_id}>
  <td>Machine Provider Lease Expires</td>
  <td colspan=2>${bot.human_lease_expiration_ts}</td>
</tr>
`

// TODO
const deviceSection = (ele) => '';

const stateSection = (ele, bot) => html`
<span class=title>State</span>
<button @click=${ele._toggleBotState}>
  <add-circle-outline-icon-sk ?hidden=${ele._showState}></add-circle-outline-icon-sk>
  <remove-circle-outline-icon-sk ?hidden=${!ele._showState}></remove-circle-outline-icon-sk>
</button>

<div ?hidden=${!ele._showState} class=bot_state>
  ${JSON.stringify(bot && bot.state || {}, null, 2)}
</div>
`;

const tasksTable = (ele, tasks) => {
  if (!ele._botId || ele._showEvents) {
    return '';
  }
  return html`
<table class=tasks_table>
  <thead>
    <tr>
      <th>Task</th>
      <th>Started</th>
      <th>Duration</th>
      <th>Result</th>
    </tr>
  </thead>
  <tbody>
    ${tasks.map(taskRow)}
  </tbody>
</table>

<button ?disabled=${!ele._taskCursor}
        @click=${ele._moreTasks}>
  Show More
</button>
`;
}

const taskRow = (task) => html`
<tr class=${taskClass(task)}>
  <td class=break-all>
    <a target=_blank rel=noopener
        href=${taskPageLink(task.task_id)}>
      ${task.name}
    </a>
  </td>
  <td>${task.human_started_ts}</td>
  <td title=${task.human_completed_ts}>${task.human_total_duration}</td>
  <td>${task.human_state}</td>
</tr>
`;

const eventsTable = (ele, events) => {
  if (!ele._botId || !ele._showEvents) {
    return '';
  }
  return html`
<div class=all-events>
  <checkbox-sk ?checked=${ele._showAll}
               @click=${ele._toggleShowAll}>
  </checkbox-sk>
  <span>Show all events</span>
</div>
<table class=events_table>
  <thead>
    <tr>
      <th>Message</th>
      <th>Type</th>
      <th>Timestamp</th>
      <th>Task ID</th>
      <th>Version</th>
    </tr>
  </thead>
  <tbody>
    ${events.map((event) => eventRow(event, ele._showAll, ele.server_details.bot_version))}
  </tbody>
</table>

<button ?disabled=${!ele._eventsCursor}
        @click=${ele._moreEvents}>
  Show More
</button>
`;
}

const eventRow = (event, showAll, serverVersion) => {
  if (!showAll && !event.message) {
    return '';
  }
  return html`
<tr>
  <td class=message>${event.message}</td>
  <td>${event.event_type}</td>
  <td>${event.human_ts}</td>
  <td>
    <a target=_blank rel=noopener
        href=${taskPageLink(event.task_id)}>
      ${event.task_id}
    </a>
  </td>
  <td class=${serverVersion === event.version ? '' : 'old_version'}>
      ${event.version && event.version.substring(0, 10)}
  </td>
</tr>`;
}

const template = (ele) => html`
<swarming-app id=swapp
              client_id=${ele.client_id}
              ?testing_offline=${ele.testing_offline}>
  <header>
    <div class=title>Swarming Task Page</div>
      <aside class=hideable>
        <a href=/>Home</a>
        <a href=/botlist>Bot List</a>
        <a href=/tasklist>Task List</a>
        <a href="/oldui/bot?id=${ele._botId}">Old Bot Page</a>
        <a href=/task>Task Page</a>
      </aside>
  </header>
  <main>
    <h2 class=message ?hidden=${ele.loggedInAndAuthorized}>${ele._message}</h2>

    <div class=top>
      ${idAndButtons(ele)}
    </div>
    <div class="horizontal layout wrap content"
         ?hidden=${!ele.loggedInAndAuthorized || !ele._botId}>
      <div class=grow>
        <table class=data_table>
          ${statusAndTask(ele, ele._bot)}
          ${dimensionBlock(ele._bot.dimensions || [])}
          ${dataAndMPBlock(ele, ele._bot)}
        </table>
        ${deviceSection(ele)}
        ${stateSection(ele, ele._bot)}
      </div>

      <div class="stats grow">Stats Table will go here</div>
    </div>

    <div class=tasks-events-picker>
      <div class=tab
           @click=${(e) => ele._setShowEvents(false)}
           ?selected=${!ele._showEvents}>
        Tasks
      </div>
      <div class=tab
           @click=${(e) => ele._setShowEvents(true)}
           ?selected=${ele._showEvents}>
        Events
      </div>
    </div>

    ${tasksTable(ele, ele._tasks)}
    ${eventsTable(ele, ele._events)}

  </main>
  <footer></footer>
  <dialog-pop-over>
    <div class='prompt-dialog content'>
      Are you sure you want to ${ele._prompt}?
      <div class="horizontal layout end">
        <button @click=${ele._closePopup} class=cancel>NO</button>
        <button @click=${ele._promptCallback} class=ok>YES</button>
      </div>
    </div>
  </dialog-pop-over>
</swarming-app>
`;

window.customElements.define('bot-page', class extends SwarmingAppBoilerplate {

  constructor() {
    super(template);

    // Set empty values to allow empty rendering while we wait for
    // stateReflector (which triggers on DomReady). Additionally, these values
    // help stateReflector with types.
    this._botId = '';
    this._showState = false;
    this._showEvents = true;
    this._showAll = true;

    this._urlParamsLoaded = true;
    this._stateChanged = () => console.log('TODO');

    this._bot = {};
    this._tasks = [];
    this._events = [];
    this._resetCursors();

    this._promptCallback = () => {};

    this._message = 'You must sign in to see anything useful.';
    // Allows us to abort fetches that are tied to the id when the id changes.
    this._fetchController = null;
  }

  connectedCallback() {
    super.connectedCallback();

    this._loginEvent = (e) => {
      this._fetch();
      this.render();
    };
    this.addEventListener('log-in', this._loginEvent);
    this.render();
  }

  disconnectedCallback() {
    super.disconnectedCallback();
    this.removeEventListener('log-in', this._loginEvent);
  }

  _closePopup() {
    $$('dialog-pop-over', this).hide();
  }

  _fetch() {
    if (!this.loggedInAndAuthorized || !this._urlParamsLoaded || !this._botId) {
      return;
    }
    if (this._fetchController) {
      // Kill any outstanding requests.
      this._fetchController.abort();
    }
    // Make a fresh abort controller for each set of fetches. AFAIK, they
    // cannot be re-used once aborted.
    this._fetchController = new AbortController();
    const extra = {
      headers: {'authorization': this.auth_header},
      signal: this._fetchController.signal,
    };
    this.app.addBusyTasks(1);
    fetch(`/_ah/api/swarming/v1/bot/${this._botId}/get`, extra)
      .then(jsonOrThrow)
      .then((json) => {
        this._bot = parseBotData(json);
        this.render();
        this.app.finishedTask();
      })
      .catch((e) => this.fetchError(e, 'bot/data'));
    if (!this._taskCursor) {
      this.app.addBusyTasks(1);
      fetch(`/_ah/api/swarming/v1/bot/${this._botId}/tasks?${TASKS_QUERY_PARAMS}`, extra)
        .then(jsonOrThrow)
        .then((json) => {
          this._taskCursor = json.cursor;
          this._tasks = parseTasks(json.items);
          this.render();
          this.app.finishedTask();
        })
        .catch((e) => this.fetchError(e, 'bot/tasks'));
    }

    if (!this._eventsCursor) {
      this.app.addBusyTasks(1);
      fetch(`/_ah/api/swarming/v1/bot/${this._botId}/events?${EVENTS_QUERY_PARAMS}`, extra)
        .then(jsonOrThrow)
        .then((json) => {
          this._eventsCursor = json.cursor;
          this._events = parseEvents(json.items);
          this.render();
          this.app.finishedTask();
        })
        .catch((e) => this.fetchError(e, 'bot/events'));
    }
  }

  _killTask() {
    const body = {
      kill_running: true,
    };
    this.app.addBusyTasks(1);
    fetch(`/_ah/api/swarming/v1/task/${this._bot.task_id}/cancel`, {
      method: 'POST',
      headers: {
        'authorization': this.auth_header,
        'content-type': 'application/json',
      },
      body: JSON.stringify(body),
    }).then(jsonOrThrow)
      .then((response) => {
        this._closePopup();
        errorMessage('Request to kill task sent', 4000);
        this.render();
        this.app.finishedTask();
      })
      .catch((e) => this.fetchError(e, 'task/kill'));
  }

  _moreEvents() {
    if (!this._eventsCursor) {
      return;
    }
    const extra = {
      headers: {'authorization': this.auth_header},
      signal: this._fetchController.signal,
    };
    this.app.addBusyTasks(1);
    fetch(`/_ah/api/swarming/v1/bot/${this._botId}/events?cursor=${this._eventsCursor}&` +
          EVENTS_QUERY_PARAMS, extra)
      .then(jsonOrThrow)
      .then((json) => {
        this._eventsCursor = json.cursor;
        this._events.push(...parseEvents(json.items));
        this.render();
        this.app.finishedTask();
      })
      .catch((e) => this.fetchError(e, 'bot/more_events'));
  }

  _moreTasks() {
    if (!this._taskCursor) {
      return;
    }
    const extra = {
      headers: {'authorization': this.auth_header},
      signal: this._fetchController.signal,
    };
    this.app.addBusyTasks(1);
    fetch(`/_ah/api/swarming/v1/bot/${this._botId}/tasks?cursor=${this._taskCursor}&` +
          EVENTS_QUERY_PARAMS, extra)
      .then(jsonOrThrow)
      .then((json) => {
        this._taskCursor = json.cursor;
        this._tasks.push(...parseTasks(json.items));
        this.render();
        this.app.finishedTask();
      })
      .catch((e) => this.fetchError(e, 'bot/more_tasks'));
  }

  _promptKill() {
    this._prompt = `kill running task ${this._bot.task_id}`;
    this._promptCallback = this._killTask;
    this.render();

    $$('dialog-pop-over', this).show();
  }

  _promptShutdown() {
    this._prompt = `gracefully shut down bot '${this._botId}'`;
    this._promptCallback = this._shutdownBot;
    this.render();

    $$('dialog-pop-over', this).show();
  }

  render() {
    super.render();
    const idInput = $$('#id_input', this);
    idInput.value = this._botId;
  }

  // _resetCursors indicates we should forget any tasks and events we have
  // seen and start over (when _fetch() is next called).
  _resetCursors() {
    this._taskCursor = '';
    this._eventsCursor = '';
  }

  _setShowEvents(shouldShow) {
    this._showEvents = shouldShow;
    this.render();
  }

  _shutdownBot() {
    this.app.addBusyTasks(1);
    fetch(`/_ah/api/swarming/v1/bot/${this._botId}/terminate`, {
      method: 'POST',
      headers: {
        'authorization': this.auth_header,
        'content-type': 'application/json',
      },
    }).then(jsonOrThrow)
      .then((response) => {
        this._closePopup();
        errorMessage('Request to shutdown bot sent', 4000);
        this.render();
        this.app.finishedTask();
      })
      .catch((e) => this.fetchError(e, 'bot/terminate'));
  }

  _toggleBotState(e) {
    this._showState = !this._showState;
    this._stateChanged();
    this.render();
  }

  _toggleShowAll(e) {
    // prevent double event
    e.preventDefault();
    this._showAll = !this._showAll;
    this._stateChanged();
    this.render();
  }

  _updateID(e) {
    const idInput = $$('#id_input', this);
    this._botId = idInput.value;
    this._resetCursors();
    this._stateChanged();
    this._fetch();
    this.render();
  }
});

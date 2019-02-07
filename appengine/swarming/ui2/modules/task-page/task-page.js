// Copyright 2019 The LUCI Authors. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

import { $$ } from 'common-sk/modules/dom'
import { html, render } from 'lit-html'
import { ifDefined } from 'lit-html/directives/if-defined';
import { jsonOrThrow } from 'common-sk/modules/jsonOrThrow'
import { stateReflector } from 'common-sk/modules/stateReflector'

import 'elements-sk/checkbox-sk'
import 'elements-sk/icon/add-circle-outline-icon-sk'
import 'elements-sk/icon/remove-circle-outline-icon-sk'
import 'elements-sk/styles/buttons'
import '../swarming-app'

import * as human from 'common-sk/modules/human'
import * as query from 'common-sk/modules/query'

import { cipdLink, hasRichOutput, humanState, isolateLink, isSummaryTask,
         parseRequest, parseResult, richLogsLink, sliceExpires, stateClass,
         taskCost, taskExpires,
         taskInfoClass, wasDeduped, wasPickedUp} from './task-page-helpers'
import { botPageLink, humanDuration, taskPageLink } from '../util'

import SwarmingAppBoilerplate from '../SwarmingAppBoilerplate'

/**
 * @module swarming-ui/modules/task-page
 * @description <h2><code>task-page<code></h2>
 *
 * <p>
 *   TODO
 * </p>
 *
 */

const taskInfoTable = (ele, request, result, currentSlice) => {
  if (!currentSlice.properties) {
    currentSlice.properties = {};
  }
  return html`
<div class=id_buttons>
  <input id=id_input placeholder="Task ID" @change=${ele._updateID}></input>
  <button title="Refresh data"
          @click=${ele._fetch}>refresh</button>
  <button title="Retry the task"
          @click=${() => alert('use old ui for now')}>retry</button>
  <button title="Re-queue the task, but don't run it automatically"
          @click=${() => alert('use old ui for now')}>debug</button>
</div>

${slicePicker(ele)}

<table class="task-info request-info ${taskInfoClass(ele, result)}">
<tbody>
  <tr>
    <td>Name</td>
    <td>${request.name}</td>
  </tr>
  ${stateLoadBlock(ele, request, result)}
  ${requestBlock(request, result, currentSlice)}
  ${dimensionBlock(currentSlice.properties.dimensions || [])}
  ${isolateBlock('Isolated Inputs', currentSlice.properties.inputs_ref || {})}
  ${arrayInTable(currentSlice.properties.outputs,
                 'Expected outputs', (output) => output)}
  ${commitBlock(request.tagMap)}

  <tr class=details>
    <td>More Details</td>
    <td>
      <button @click=${ele._toggleDetails} ?hidden=${ele._showDetails}>
        <add-circle-outline-icon-sk></add-circle-outline-icon-sk>
      </button>
      <button @click=${ele._toggleDetails} ?hidden=${!ele._showDetails}>
        <remove-circle-outline-icon-sk></remove-circle-outline-icon-sk>
      </button>
    </td>
  </tr>
</tbody>
<tbody ?hidden=${!ele._showDetails}>
  ${executionBlock(currentSlice.properties, currentSlice.properties.env || [])}

  ${arrayInTable(request.tags,
                 'Tags', (tag) => tag)}
  <tr>
    <td>Execution timeout</td>
    <td>${humanDuration(currentSlice.properties.execution_timeout_secs)}</td>
  </tr>
  <tr>
    <td>I/O timeout</td>
    <td>${humanDuration(currentSlice.properties.io_timeout_secs)}</td>
  </tr>
  <tr>
    <td>Grace period</td>
    <td>${humanDuration(currentSlice.properties.grace_period_secs)}</td>
  </tr>

  ${cipdBlock(currentSlice.properties.cipd_input, result)}
  ${arrayInTable(currentSlice.properties.caches,
                 'Named Caches',
                 (cache) => cache.name + ':' + cache.path)}
</tbody>
</table>
`;
}

const slicePicker = (ele) => {
  if (!(ele._request.task_slices && ele._request.task_slices.length > 1)) {
    return '';
  }

  return html`
<div class=slice-picker>
  ${ele._request.task_slices.map((_, idx) => sliceTab(ele, idx))}
</div>
`}

const sliceTab = (ele, idx) => html`
  <div class=tab ?selected=${ele._currentSliceIdx === idx}
                 @click=${() => ele._setSlice(idx)}>Task Slice ${idx+1}</div>
`;

const stateLoadBlock = (ele, request, result) => html`
<tr>
  <td>State</td>
  <td class=${stateClass(result)}>${humanState(result, ele._currentSliceIdx)}</td>
</tr>
${countBlocks(result, ele._capacityCounts[ele._currentSliceIdx],
                      ele._pendingCounts[ele._currentSliceIdx],
                      ele._runningCounts[ele._currentSliceIdx])}
<tr ?hidden=${!result.deduped_from}>
  <td><b>Deduped From</b></td>
  <td><a href=${taskPageLink(result.deduped_from)}</td>
</tr>
<tr ?hidden=${!result.deduped_from}>
  <td>Deduped On</td>
  <td title=${request.created_ts}>
    ${request.human_created_ts}
  </td>
</tr>
`;

const countBlocks = (result, capacityCount, pendingCount, runningCount) => html`
<tr>
  <td class=${result.state === 'PENDING'? 'bold': ''}>
    ${result.state === 'PENDING' ? 'Why Pending?' : 'Fleet Capacity'}
  </td>
  <td>
    ${count(capacityCount, 'count')} bots could possibly run this task
    (${count(capacityCount, 'busy')} busy,
    ${count(capacityCount, 'dead')} dead,
    ${count(capacityCount, 'quarantined')} quarantined,
    ${count(capacityCount, 'maintenance')} maintenance)
  </td>
</tr>
<tr>
  <td>Similar Load</td>
  <td>
      ${count(pendingCount)} similar pending tasks,
      ${count(runningCount)} similar running tasks
  </td>
</tr>
`;

const count = (obj, value) => {
  if (!obj || (value && obj[value] === undefined)) {
    return html`<span class=italic>&lt;counting&gt</span>`
  }
  if (value) {
    return obj[value];
  }
  return obj;
}

const requestBlock = (request, result, currentSlice) => html`
<tr>
  <td>Priority</td>
  <td>${request.priority}</td>
</tr>
<tr>
  <td>Wait for Capacity</td>
  <td>${!!currentSlice.wait_for_capacity}</td>
</tr>
<tr>
  <td>Slice Expires</td>
  <td>${sliceExpires(currentSlice, request)}</td>
</tr>
<tr>
  <td>User</td>
  <td>${request.user || 'None'}</td>
</tr>
<tr>
  <td>Authenticated</td>
  <td>${request.authenticated}</td>
</tr>
<tr ?hidden=${!request.service_account}>
  <td>Service Account</td>
  <td>${request.service_account}</td>
</tr>
<tr ?hidden=${!currentSlice.properties.secret_bytes}>
  <td>Has Secret Bytes</td>
  <td title="The secret bytes are present on the machine, but not in the UI/API">true</td>
</tr>
<tr ?hidden=${!request.parent_task_id}>
  <td>Parent Task</td>
  <td>
    <a href=${taskPageLink(request.parent_task_id)}>
      ${request.parent_task_id}
    </a>
  </td>
</tr>
`;

const dimensionBlock = (dimensions) => html`
<tr>
  <td rowspan=${dimensions.length+1}>
    Dimensions
    <!-- TODO(kjlubick) add links to bots/tasks-->
  </td>
</tr>
${dimensions.map(dimension_row)}
`;

const dimension_row = (dimension) => html`
<tr>
  <td class=break-all><b>${dimension.key}:</b> ${dimension.value}</td>
</tr>
`;

const isolateBlock = (title, ref) => {
  if (!ref.isolated) {
    return '';
  }
  return html`
<tr>
  <td>${title}</td>
  <td>
    <a href=${isolateLink(ref)}>
      ${ref.isolated}
    </a>
  </td>
</tr>`;
};

const arrayInTable = (array, label, keyFn) => {
  if (!array || !array.length) {
    return html`
<tr>
  <td>${label}</td>
  <td>--</td>
</tr>`;
  }
  return html`
<tr>
  <td rowspan=${array.length+1}>${label}</td>
</tr>
${array.map(arrayRow(keyFn))}`;
}

const arrayRow = (keyFn) => {
  return (key) => html`
<tr>
  <td class=break-all>${keyFn(key)}</td>
</tr>
`;
}

const commitBlock = (tagMap) => {
  if (!tagMap || !tagMap.source_revision) {
    return '';
  }
    return html`
<tr>
  <td>Associated Commit</td>
  <td>
    <a href=${tagMap.source_repo.replace('%s', tagMap.source_revision)}>
      ${tagMap.source_revision.substring(0, 12)}
    </a>
  </td>
</tr>
`};

const executionBlock = (properties, env) => html`
<tr>
  <td>Extra Args</td>
  <td class="code break-all">${(properties.extra_args || []).join(' ') || '--'}</td>
</tr>
<tr>
  <td>Command</td>
  <td class="code break-all">${(properties.command || []).join(' ') || '--'}</td>
</tr>
${arrayInTable(env, 'Environment Vars',
              (env) => env.key + '=' + env.value)}
<tr>
  <td>Idempotent</td>
  <td>${!!properties.idempotent}</td>
</tr>
`;

const cipdBlock = (cipdInput, result) => {
  if (!cipdInput) {
    return html`
<tr>
  <td>Uses CIPD</td>
  <td>false</td>
</tr>`;
  }
  const requestedPackages = cipdInput.packages || [];
  const actualPackages = (result.cipd_pins && result.cipd_pins.packages) || [];
  for (let i = 0; i < requestedPackages.length; i++) {
    const p = requestedPackages[i];
    p.requested = `${p.package_name}:${p.version}`;
    // This makes the key assumption that the actual cipd array is in the same order
    // as the requested one. Otherwise, there's no easy way to match them up, because
    // of the wildcards (e.g. requested is foo/${platform} and actual is foo/linux-amd64)
    if (actualPackages[i]) {
      p.actual = `${actualPackages[i].package_name}:${actualPackages[i].version}`;
    }
  }
  let packageName = '(available when task is run)';
  if (result.cipd_pins && result.cipd_pins.client_package) {
    packageName = result.cipd_pins.client_package.package_name;
  }
  // We always need to at least double the number of packages because we
  // show the path and then the requested.  If the actual package info
  // is available, we triple the number of packages to account for that.
  let cipdRowspan = requestedPackages.length;
  if (actualPackages.length) {
    cipdRowspan *= 3;
  } else {
    cipdRowspan *=2;
  }
  // Add one because rowSpan counts from 1.
  cipdRowspan += 1;
  return html`
<tr>
  <td>CIPD server</td>
  <td>
    <a href=${cipdInput.server}>${cipdInput.server}</a>
  </td>
</tr>
<tr>
  <td>CIPD version</td>
  <td class=break-all>${cipdInput.client_package && cipdInput.client_package.version}</td>
</tr>
<tr>
  <td>CIPD package name</td>
  <td>${packageName}</td>
</tr>
<tr>
  <td rowspan=${cipdRowspan}>CIPD packages</td>
</tr>
${requestedPackages.map((pkg) => cipdRowSet(pkg, cipdInput, !!actualPackages.length))}
`;
}

const cipdRowSet = (pkg, cipdInput, actualAvailable) => html`
<tr>
  <td>${pkg.path}/</td>
</tr>
<tr>
  <td class=break-all>
    <span class=cipd-header>Requested: </span>${pkg.requested}
  </td>
</tr>
<tr ?hidden=${!actualAvailable}>
  <td class=break-all>
    <span class=cipd-header>Actual: </span>
    <a href=${cipdLink(pkg.actual, cipdInput.server)}
       target=_blank rel=noopener>
      ${pkg.actual}
    </a>
  </td>
</tr>
`;

const taskTimingSection = (ele, request, result) => {
  if (wasDeduped(result)) {
    // Don't show timing info when task was deduped because the info
    // in the result is from the original task, which can be confusing
    // when juxtaposed with the data from this task.
    return '';
  }
  const performanceStats = result.performance_stats || {};
  return html`
<div class=title>Task Timing Information</div>
<div class="horizontal layout wrap">
  <table class="task-info task-timing left">
    <tbody>
      <tr>
        <td>Created</td>
        <td title=${request.created_ts}>${request.human_created_ts}</td>
      </tr>
      <tr ?hidden=${!wasPickedUp(result)}>
        <td>Started</td>
        <td title=${result.started_ts}>${result.human_started_ts}</td>
      </tr>
      <tr>
        <td>Expires</td>
        <td>${taskExpires(request)}</td>
      </tr>
      <tr ?hidden=${!result.completed_ts}>
        <td>Completed</td>
        <td title=${result.completed_ts}>${result.human_completed_ts}</td>
      </tr>
      <tr ?hidden=${!result.abandoned_ts}>
        <td>Abandoned</td>
        <td title=${result.abandoned_ts}>${result.human_abandoned_ts}</td>
      </tr>
      <tr>
        <td>Last updated</td>
        <td title=${result.modified_ts}>${result.human_modified_ts}</td>
      </tr>
      <tr>
        <td>Pending Time</td>
        <td class=pending>${result.human_pending}</td>
      </tr>
      <tr>
        <td>Total Overhead</td>
        <td class=overhead>${humanDuration(performanceStats.bot_overhead)}</td>
      </tr>
      <tr>
        <td>Running Time</td>
        <td class=running title="An asterisk indicates the task is still running and thus the time is dynamic.">
          ${result.human_duration}
        </td>
      </tr>
    </tbody>
  </table>
  <div class=right>
    <!-- TODO(kjlubick) -->
    <stacked-time-chart
      labels='["Pending", "Overhead", "Running", "Overhead"]'
      colors='["#E69F00", "#D55E00", "#0072B2", "#D55E00"]'
      values='[[_durationChart(_result, _result.*)}'>
    </stacked-time-chart>
  </div>
</div>
`;
}

const taskExecutionSection = (ele, request, result, currentSlice) => {
  if (!result || !wasPickedUp(result)) {
    return html`
<div class=title>Task Execution</div>
<div class=task-execution>This space left blank until a bot is assigned to the task.</div>
`;
  }
  if (wasDeduped(result)) {
    // Don't show timing info when task was deduped because the info
    // in the result is from the original task, which can be confusing
    // when juxtaposed with the data from this task.
    return '';
  }

  if (!currentSlice.properties) {
    currentSlice.properties = {};
  }
  // Pre-process the dimensions so we can highlight those that were matched
  // against, with a bold on the subset of dimensions that matched.
  const botDimensions = result.bot_dimensions || [];
  const usedDimensions = currentSlice.properties.dimensions || [];

  for (const dim of botDimensions) {
    for (const d of usedDimensions) {
      if (d.key === dim.key) {
        dim.highlight = true;
      }
    }
    const values = [];
    // despite the name, dim.value is an array of values
    for (const v of dim.value) {
      const newValue = {name: v};
      for (const d of usedDimensions) {
        if (d.key === dim.key && d.value === v) {
          newValue.bold = true;
        }
      }
      values.push(newValue);
    }
    dim.values = values;
  }

  return html`
<div class=title>Task Execution</div>
<table class=task-execution>
  <tr>
    <td>Bot assigned to task</td>
    <td><a href=${botPageLink(result.bot_id)}>${result.bot_id}</td>
  </tr>
  <tr>
    <td rowspan=${botDimensions.length+1}>
      Dimensions
    </td>
  </tr>
  ${botDimensions.map((dim) => botDimensionRow(dim, usedDimensions))}
  <tr>
    <td>Exit code</td>
    <td>${result.exit_code}</td>
  </tr>
  <tr>
    <td>Try number</td>
    <td>${result.try_number}</td>
  </tr>
  <tr>
    <td>Failure</td>
    <td class=${result.failure ? 'failed_task': ''}>${!!result.failure}</td>
  </tr>
  <tr>
    <td>Internal Failure</td>
    <td class=${result.internal_failure ? 'exception': ''}>${result.internal_failure}</td>
  </tr>
  <tr>
    <td>Cost (USD)</td>
    <td>$${taskCost(result)}</td>
  </tr>
  ${isolateBlock('Isolated Outputs', result.outputs_ref || {})}
  <tr>
    <td>Bot version</td>
    <td>${result.bot_version}</td>
  </tr>
  <tr>
    <td>Server version</td>
    <td>${result.server_versions}</td>
  </tr>
</table>`;
};

const botDimensionRow = (dim, usedDimensions) => html`
<tr>
  <td class=${dim.highlight ? 'highlight': ''}><b>${dim.key}:</b>
    ${dim.values.map(botDimensionValue)}
  </td>
</tr>
`;

const botDimensionValue = (value) => html`
<span class="break-all dim ${value.bold ? 'bold': ''}">${value.name}</span>
`;

const performanceStatsSection = (ele, performanceStats) => {
  if (!performanceStats) {
    return '';
  }
  return html`
<div class=title>Performance Stats</div>
<table class=performance-stats>
  <tr>
    <td title="This includes time taken to download inputs, isolate outputs, and setup CIPD">Total Overhead</td>
    <td>${humanDuration(performanceStats.bot_overhead)}</td>
  </tr>
  <tr>
    <td>Downloading Inputs From Isolate</td>
    <td>${humanDuration(performanceStats.isolated_download.duration)}</td>
  </tr>
  <tr>
    <td>Uploading Outputs To Isolate</td>
    <td>${humanDuration(performanceStats.isolated_upload.duration)}</td>
  </tr>
  <tr>
    <td>Initial bot cache</td>
    <td>${performanceStats.isolated_download.initial_number_items || 0} items;
    ${human.bytes(performanceStats.isolated_download.initial_size || 0)}</td>
  </tr>
  <tr>
    <td>Downloaded Cold Items</td>
    <td>${performanceStats.isolated_download.num_items_cold || 0} items;
     ${human.bytes(performanceStats.isolated_download.total_bytes_items_cold || 0)}</td>
  </tr>
  <tr>
    <td>Downloaded Hot Items</td>
    <td>${performanceStats.isolated_download.num_items_hot || 0} items;
     ${human.bytes(performanceStats.isolated_download.total_bytes_items_hot || 0)}</td>
  </tr>
  <tr>
    <td>Uploaded Cold Items</td>
    <td>${performanceStats.isolated_upload.num_items_cold || 0} items;
     ${human.bytes(performanceStats.isolated_upload.total_bytes_items_cold || 0)}</td>
  </tr>
  <tr>
    <td>Uploaded Hot Items</td>
    <td>${performanceStats.isolated_upload.num_items_hot || 0} items;
     ${human.bytes(performanceStats.isolated_upload.total_bytes_items_hot || 0)}</td>
  </tr>
</table>`;
}

const reproduceSection = (ele, currentSlice) => {
  const ref =  currentSlice.properties && currentSlice.properties.inputs_ref || {};
  const hasIsolated = !!ref.isolated;
  const hostUrl = window.location.hostname;
  return html`
<div class=title>Reproducing the task locally</div>
<div class=reproduce>
  <div ?hidden=${!hasIsolated}>Download inputs files into directory <i>foo</i>:</div>
  <div class="code bottom_space" ?hidden=${!hasIsolated}>
    # (if needed) git clone https://chromium.googlesource.com/infra/luci/client-py<br>
    python ./client-py/isolateserver.py download -I ${ref.isolatedserver}
    --namespace ${ref.namespace}
    -s ${ref.isolated} --target foo
  </div>

  <div>Run this task locally:</div>
  <div class="code bottom_space">
    # (if needed) git clone https://chromium.googlesource.com/infra/luci/client-py<br>
    python ./client-py/swarming.py reproduce -S ${hostUrl} ${ele._taskId}
  </div>

  <div>Download output results into directory <i>foo</i>:</div>
  <div class="code bottom_space">
    # (if needed) git clone https://chromium.googlesource.com/infra/luci/client-py<br>
    python ./client-py/swarming.py collect -S ${hostUrl} --task-output-dir=foo ${ele._taskId}
  </div>
</div>
`;
}

const taskLogs = (ele) => html`
<div class="horizontal layout">
  <div class=output-picker>
    <div class=tab ?selected=${!ele._showRawOutput && hasRichOutput(ele)}
                   ?disabled=${!hasRichOutput(ele)}
                   @click=${ele._toggleOutput}>
      <a rel=noopener target=_blank href=${ifDefined(richLogsLink(ele))}>
        Rich Output
      </a>
    </div>
    <div class=tab ?selected=${ele._showRawOutput || !hasRichOutput(ele)}
                   @click=${ele._toggleOutput}>
      Raw Output
    </div>
    <checkbox-sk id=wide_logs ?checked=${ele._wideLogs} @click=${ele._toggleWidth}></checkbox-sk>
    <span>Full Width Logs</span>
  </div>
</div>
${richOrRawLogs(ele)}
`;

const richOrRawLogs = (ele) => {
  if (ele._showRawOutput || !hasRichOutput(ele)) {
    return html`
<div class="code stdout tabbed break-all ${ele._wideLogs ? 'wide' : ''}">${ele._stdout}</div>
`;
  }
  if (!isSummaryTask(ele._taskId)) {
    return html`
<div class=tabbed>
  Milo results are only generated for task summaries, that is, tasks whose ids end in 0.
  Tasks ending in 1 or 2 represent possible retries of tasks.
  See <a href="//goo.gl/LE4rwV">the docs</a> for more.
</div>`;
  }
  return html`
<iframe id=richLogsFrame class=tabbed src=${ifDefined(richLogsLink(ele))}></iframe>
`;
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
        <a href=/bot>Bot Page</a>
        <a href="/oldui/task?id=${ele._taskId}">Old Task Page</a>
      </aside>
  </header>
  <main class="horizontal layout wrap">
    <h2 class=message ?hidden=${ele.loggedInAndAuthorized}>${ele._message}</h2>

    <div class="left grow" ?hidden=${!ele.loggedInAndAuthorized}>
    ${taskInfoTable(ele, ele._request, ele._result, ele._currentSlice)}

    ${taskTimingSection(ele, ele._request, ele._result)}

    ${taskExecutionSection(ele, ele._request, ele._result, ele._currentSlice)}

    ${performanceStatsSection(ele, ele._result.performance_stats)}

    ${reproduceSection(ele, ele._currentSlice.properties)}
    </div>
    <div class="right grow" ?hidden=${!ele.loggedInAndAuthorized}>
    ${taskLogs(ele)}
    </div>
  </main>
  <footer></footer>
</swarming-app>
`;

window.customElements.define('task-page', class extends SwarmingAppBoilerplate {

  constructor() {
    super(template);
    // Set empty values to allow empty rendering while we wait for
    // stateReflector (which triggers on DomReady). Additionally, these values
    // help stateReflector with types.
    this._taskId = '';
    this._showDetails = false;
    this._showRawOutput = false;
    this._wideLogs = false;
    this._urlParamsLoaded = false;

    this._stateChanged = stateReflector(
      /*getState*/() => {
        return {
          // provide empty values
          'id': this._taskId,
          'd': this._showDetails,
          'o': this._showRawOutput,
          'w': this._wideLogs,
        }
    }, /*setState*/(newState) => {
      // default values if not specified.
      this._taskId = newState.id || this._taskId;
      this._showDetails = newState.d; // default to false
      this._showRawOutput = newState.o; // default to false
      this._wideLogs = newState.w;
      this._urlParamsLoaded = true;
      this._fetch();
      this.render();
    });

    this._request = {};
    this._result = {};
    this._currentSlice = {};
    this._currentSliceIdx = -1;
    // Track counts a set of parallel arrays, that is, the nth index in
    // each of these corresponds to the counts for the nth slice.
    // They will be filled in index by index when each fetch from
    // _fetchCounts returns a value.
    this._capacityCounts = [];
    this._pendingCounts = [];
    this._runningCounts = [];
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

  _fetch() {
    if (!this.loggedInAndAuthorized || !this._urlParamsLoaded) {
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
    this.app.addBusyTasks(3);
    let currIdx = -1;
    fetch(`/_ah/api/swarming/v1/task/${this._taskId}/request`, extra)
      .then(jsonOrThrow)
      .then((json) => {
        this._request = parseRequest(json);
        // Note, this triggers more fetch requests, which also adds to
        // app's busy task counts.
        this._fetchCounts(this._request, extra);
        // We need to set the slice if result has been loaded, otherwise
        // when the slice loads, it will take care of it for us.
        if (currIdx >= 0) {
          this._setSlice(currIdx); // calls render
        } else {
          this.render();
        }
        this.app.finishedTask();
      })
      .catch((e) => this.fetchError(e, 'task/request'));
    fetch(`/_ah/api/swarming/v1/task/${this._taskId}/result?include_performance_stats=true`, extra)
      .then(jsonOrThrow)
      .then((json) => {
        this._result = parseResult(json);
        currIdx = +this._result.current_task_slice;
        this._setSlice(currIdx); // calls render
        this.app.finishedTask();
      })
      .catch((e) => this.fetchError(e, 'task/result'));
    fetch(`/_ah/api/swarming/v1/task/${this._taskId}/stdout`, extra)
      .then(jsonOrThrow)
      .then((json) => {
        this._stdout = json.output;
        this.render();
        this.app.finishedTask();
      })
      .catch((e) => this.fetchError(e, 'task/request'));
  }

  _fetchCounts(request, extra) {
    const numSlices = request.task_slices.length;
    this.app.addBusyTasks(numSlices * 3);
    // reset current viewings
    this._capacityCounts = [];
    this._pendingCounts = [];
    this._runningCounts = [];
    for (let i = 0; i < numSlices; i++) {
      const bParams = {
        dimensions: [],
      }
      for (const dim of request.task_slices[i].properties.dimensions) {
        bParams.dimensions.push(`${dim.key}:${dim.value}`);
      }
      fetch(`/_ah/api/swarming/v1/bots/count?${query.fromObject(bParams)}`, extra)
        .then(jsonOrThrow)
        .then((json) => {
          this._capacityCounts[i] = json;
          this.render();
          this.app.finishedTask();
        })
        .catch((e) => this.fetchError(e, 'bots/count slice ' + i));

      let start = new Date();
      start.setSeconds(0);
      // go back 24 hours, rounded to the nearest minute for better caching.
      start = '' + (start.getTime() - 24*60*60*1000);
      // convert to seconds, because that's what the API expects.
      start = start.substring(0, start.length-3);
      const tParams = {
        start: [start],
        state: ['RUNNING'],
        tags: bParams.dimensions,
      }
      fetch(`/_ah/api/swarming/v1/tasks/count?${query.fromObject(tParams)}`, extra)
        .then(jsonOrThrow)
        .then((json) => {
          this._runningCounts[i] = json.count;
          this.render();
          this.app.finishedTask();
        })
        .catch((e) => this.fetchError(e, 'tasks/running slice ' + i));

      tParams.state = ['PENDING'];
      fetch(`/_ah/api/swarming/v1/tasks/count?${query.fromObject(tParams)}`, extra)
        .then(jsonOrThrow)
        .then((json) => {
          this._pendingCounts[i] = json.count;
          this.render();
          this.app.finishedTask();
        })
        .catch((e) => this.fetchError(e, 'tasks/pending slice ' + i));
    }
  }

  render() {
    super.render();
    const idInput = $$('#id_input', this);
    idInput.value = this._taskId;
  }

  _setSlice(idx) {
    this._currentSliceIdx = idx;
    if (!this._request.task_slices) {
      return;
    }
    this._currentSlice = this._request.task_slices[idx];
    this.render();
  }

  _toggleDetails(e) {
    this._showDetails = !this._showDetails;
    this._stateChanged();
    this.render();
  }

  _toggleOutput(e) {
    this._showRawOutput = !this._showRawOutput;
    this._stateChanged();
    this.render();
  }

  _toggleWidth(e) {
    // This prevents the checkbox from toggling twice.
    e.preventDefault();
    this._wideLogs = !this._wideLogs;
    this._stateChanged();
    this.render();
  }

  _updateID(e) {
    const idInput = $$('#id_input', this);
    this._taskId = idInput.value;
    this._stateChanged();
    this._fetch();
  }
});

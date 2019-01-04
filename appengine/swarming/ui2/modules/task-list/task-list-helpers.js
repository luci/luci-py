// Copyright 2018 The LUCI Authors. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

import { html } from 'lit-html'
import { botPageLink, compareWithFixedOrder, humanDuration, sanitizeAndHumanizeTime,
         taskPageLink } from '../util'

/** column returns the display-ready value for a column (aka key)
 *  from a task. It requires the entire state (ele) for potentially complicated
 *  data lookups (and also visibility of the 'verbose' setting).
 *  A custom version can be specified in colMap, with the default being
 *  the attribute of task in col or 'none'.
 *
 * @param {string} col - The 'key' of the data to pull.
 * @param {Object} task - The task from which to extract data.
 * @param {Object} ele - The entire task-list object, for context.
 *
 * @returns {String} - The requested column, ready for display.
 */
export function column(col, task, ele) {
  if (!task) {
    console.warn('falsey task passed into column');
    return '';
  }
  let c = colMap[col];
  if (c) {
    return c(task, ele);
  }
  return task[col] || 'none';
}

// This puts the times in a aesthetically pleasing order, roughly in
// the order everything happened.
const specialColOrder = ['name', 'created_ts', 'pending_time',
    'started_ts', 'duration', 'completed_ts', 'abandoned_ts', 'modified_ts'];

/** sortColumns sorts the task-list columns in mostly alphabetical order. Some
 *  columns go in a fixed order to make them easier to reason about.
 *
 * @param cols Array<String> The columns
 */
export function sortColumns(cols) {
  cols.sort(compareWithFixedOrder(specialColOrder));
}

const TASK_TIMES = ['abandoned_ts', 'completed_ts', 'created_ts', 'modified_ts',
                  'started_ts'];

/** processTasks processes the array of tasks from the server and returns it.
 *  The primary goal is to get the data ready for display.
 *
 * @param cols Array<Object> The raw tasks objects.
 */
export function processTasks(arr) {
  if (!arr) {
    return [];
  }
  let now = new Date();

  for (let task of arr) {
    for (let time of TASK_TIMES) {
      sanitizeAndHumanizeTime(task, time);

      // Running tasks have no duration set, so we can figure it out.
      if (!task.duration && task.state === 'RUNNING' && task.started_ts) {
        task.duration = (now - task.started_ts) / 1000;
      }
      // Make the duration human readable
      task.human_duration = humanDuration(task.duration);
      if (task.state === 'RUNNING' && task.started_ts) {
        task.human_duration = task.human_duration + '*';
      }

      // Deduplicated tasks usually have tasks that ended before they were
      // created, so we need to account for that.
      let et = task.started_ts || task.abandoned_ts || new Date();
      let deduped = (task.created_ts && et < task.created_ts);

      task.pending_time = null;
      if (!deduped && task.created_ts) {
        task.pending_time = (et - task.created_ts) / 1000;
      }
      task.human_pending_time = humanDuration(task.pending_time);
      if (!deduped && task.created_ts && !task.started_ts && !task.abandoned_ts) {
        task.human_pending_time = task.human_pending_time + '*';
      }

    };
  }
  // TODO(kjlubick): more data processing
  return arr;
}


/** colHeaderMap maps keys to their human readable name.*/
export const colHeaderMap = {
  'abandoned_ts': 'Abandoned On',
  'completed_ts': 'Completed On',
  'costs_usd': 'Cost (USD)',
  'created_ts': 'Created On',
  'duration': 'Duration',
  'modified_ts': 'Last Modified',
  'started_ts': 'Started Working On',
  'user': 'Requesting User',
  'pending_time': 'Time Spent Pending',
  // TODO(kjlubick) old version has special handling of tags -
  // turns foo-tag into foo (tag)
}

// Given a time attribute like 'abandoned_ts', humanTime returns a function
// that returns the human-friendly version of that attribute. The human
// friendly time was created in task-list-data.
function humanTime(attr) {
  return (task) => {
    return task['human_' + attr];
  }
}

const colMap = {
  abandoned_ts: humanTime('abandoned_ts'),
  bot: function(task) {
    let id = task.bot_id;
    return html`<a target=_blank
                   rel=noopener
                   href=${botPageLink(id)}>${id}</a>`;
  },
  completed_ts: humanTime('completed_ts'),
  costs_usd: function(task) {
    return task.costs_usd || 0;
  },
  created_ts: humanTime('created_ts'),
  duration: humanTime('duration'),
  modified_ts: humanTime('modified_ts'),
  name: (task, ele) => {
    let name = task.name;
    if (!ele._verbose && task.name.length > 70) {
      name = name.slice(0, 67) + '...';
    }
    return html`<a target=_blank
                   rel=noopener
                   title=${task.name}
                   href=${taskPageLink(task.task_id)}>${name}</a>`;
  },
  pending_time: humanTime('pending_time'),
  source_revision: function(task) {
    let r = task.source_revision;
    return r.substring(0, 8);
  },
  started_ts: humanTime('started_ts'),
  state: function(task) {
    let state = task.state;
    if (state === 'COMPLETED') {
      if (task.failure) {
        return 'COMPLETED (FAILURE)';
      }
      if (task.try_number === '0') {
        return 'COMPLETED (DEDUPED)';
      }
      return 'COMPLETED (SUCCESS)';
    }
    return state;
  },
}
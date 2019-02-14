// Copyright 2019 The LUCI Authors. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

import { applyAlias } from '../alias'
import { humanDuration, sanitizeAndHumanizeTime, timeDiffExact } from '../util'

/** parseBotData pre-processes any data in the bot data object.
 *  @param {Object} bot - The raw bot object
 */
export function parseBotData(bot) {
  if (!bot) {
    return {};
  }
  bot.state = bot.state || '{}';
  bot.state = JSON.parse(bot.state) || {};

  // get the disks in an easier to deal with format, sorted by size.
  const disks = bot.state.disks || {};
  const keys = Object.keys(disks);
  if (!keys.length) {
    bot.disks = [{'id': 'unknown', 'mb': 0}];
  } else {
    bot.disks = [];
    for (let i = 0; i < keys.length; i++) {
      bot.disks.push({'id':keys[i], 'mb':disks[keys[i]].free_mb});
    }
    // Sort these so the biggest disk comes first.
    bot.disks.sort(function(a, b) {
      return b.mb - a.mb;
    });
  }

  bot.dimensions = bot.dimensions || [];
  for (const dim of bot.dimensions) {
    dim.value.forEach(function(value, i) {
      dim.value[i] = applyAlias(value, dim.key);
    });
  }

  for (const time of BOT_TIMES) {
    sanitizeAndHumanizeTime(bot, time);
  }
  return bot;
}

/** parseBotData pre-processes the events to get them ready to display.
 *  @param {Array<Object>} events - The raw event objects
 */
export function parseEvents(events) {
  if (!events) {
    return [];
  }
  for (const event of events) {
    sanitizeAndHumanizeTime(event, 'ts');
  }

  // Sort the most recent events first.
  events.sort((a, b) => {
    return b.ts - a.ts;
  });
  return events;
}

/** parseTasks pre-processes the tasks to get them ready to display.
 *  @param {Array<Object>} tasks - The raw task objects
 */
export function parseTasks(tasks) {
  if (!tasks) {
    return [];
  }
  for (const task of tasks) {
    for (const time of TASK_TIMES) {
      sanitizeAndHumanizeTime(task, time);
    }
    if (task.duration) {
      // Task is finished
      task.human_duration = humanDuration(task.duration);
    } else {
      const end = task.completed_ts || task.abandoned_ts || task.modified_ts || new Date();
      task.human_duration = timeDiffExact(task.started_ts, end);
      task.duration = (end.getTime() - task.started_ts) / 1000;
    }
    const total_overhead = (task.performance_stats &&
                            task.performance_stats.bot_overhead) || 0;
    // total_duration includes overhead, to give a better sense of the bot
    // being "busy", e.g. when uploading isolated outputs.
    task.total_duration = task.duration + total_overhead;
    task.human_total_duration = humanDuration(task.total_duration);

    task.human_state = task.state || 'UNKNOWN';
    if (task.state === 'COMPLETED') {
      // use SUCCESS or FAILURE in ambiguous COMPLETED case.
      if (task.failure) {
        task.human_state = 'FAILURE';
      } else if (task.state !== 'RUNNING') {
        task.human_state = 'SUCCESS';
      }
    }
  }
  tasks.sort((a, b) => {
    return b.started_ts - a.started_ts;
  });
  return tasks;
}

const BOT_TIMES = ['first_seen_ts', 'last_seen_ts', 'lease_expiration_ts'];
const TASK_TIMES = ['started_ts', 'completed_ts', 'abandoned_ts', 'modified_ts'];

// These field filters trim down the data we get per task, which
// may speed up the server time and should speed up the network time.
export const TASKS_QUERY_PARAMS = 'include_performance_stats=true&limit=30&fields=cursor%2Citems(bot_version%2Ccompleted_ts%2Ccreated_ts%2Cduration%2Cexit_code%2Cfailure%2Cinternal_failure%2Cmodified_ts%2Cname%2Cperformance_stats(bot_overhead%2Cisolated_download(duration%2Cinitial_number_items%2Cinitial_size%2Cnum_items_cold%2Cnum_items_hot%2Ctotal_bytes_items_cold%2Ctotal_bytes_items_hot)%2Cisolated_upload(duration%2Cinitial_number_items%2Cinitial_size%2Cnum_items_cold%2Cnum_items_hot%2Ctotal_bytes_items_cold%2Ctotal_bytes_items_hot))%2Cserver_versions%2Cstarted_ts%2Ctask_id)';

export const EVENTS_QUERY_PARAMS = 'limit=50&fields=cursor%2Citems(event_type%2Cmaintenance_msg%2Cmessage%2Cquarantined%2Ctask_id%2Cts%2Cversion)';
// Copyright 2019 The LUCI Authors. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

import { applyAlias } from '../alias'
import { sanitizeAndHumanizeTime } from '../util'

/** parseBotData pre-processes any data in the bot data object.
 */
export function parseBotData(bot) {
  if (!bot) {
    return {};
  }
  // Do any preprocessing here
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

const BOT_TIMES = ['first_seen_ts', 'last_seen_ts', 'lease_expiration_ts'];
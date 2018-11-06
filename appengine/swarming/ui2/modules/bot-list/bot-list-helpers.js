// Copyright 2018 The LUCI Authors. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

// This file contains a large portion of the JS logic of bot-list.
// By keeping JS logic here, the functions can more easily be unit tested
// and it declutters the main bot-list.js.
// If a function doesn't refer to 'this', it should go here, otherwise
// it should go inside the element declaration.

import * as human from 'common-sk/modules/human'
// query.fromObject is more readable than just 'fromObject'
import * as query from 'common-sk/modules/query'
import { html } from 'lit-html'
import naturalSort from 'javascript-natural-sort/naturalSort'
import { sanitizeAndHumanizeTime } from '../util'
import { applyAlias } from '../alias'

/** aggregateTemps looks through the temperature data and computes an
 *  average temp. Beyond that, it prepares the temperature data for
 *  better displaying.
 */
export function aggregateTemps(temps) {
  if (!temps) {
    return {};
  }
  let zones = [];
  let avg = 0;
  for (let k in temps) {
    zones.push(k +': '+temps[k]);
    avg += (+temps[k]);
  }
  avg = avg / zones.length
  if (avg) {
    avg = avg.toFixed(1);
  } else {
    avg = 'unknown';
  }
  return {
    average: avg,
    zones: zones.join(' | ') || 'unknown',
  }
}

/** attribute looks first in dimension and then in state for the
 * specified attribute. This will always return an array. If there is
 * no matching attribute, ['UNKNOWN'] will be returned. The typical
 * caller of this is column(), so the return value is generally a string.
 *
 * @param {Object} bot - The bot from which to extract data.
 * @param {string} attr - The 'key' of the data to pull.
 * @param {Object|string} none - (Optional) a none value
 *
 * @returns {String} - The requested attribute, potentially ready for display.
 */
export function attribute(bot, attr, none) {
  none = none || 'UNKNOWN';
  return fromDimension(bot, attr) || fromState(bot, attr) || [none];
}

/** botLink creates the href attribute for linking to a single bot.*/
export function botLink(botId) {
  return `/bot?id=${botId}`;
}

/** column returns the display-ready value for a column (aka key)
 *  from a bot. It requires the entire state for potentially complicated
 *  data lookups (and also visibility of the 'verbose' setting).
 *  A custom version can be specified in colMap, with the default being
 *  The longest (assumed to be most specific) item returned from
 *  attribute()).
 */
export function column(col, bot, ele) {
  if (!bot) {
    console.warn('falsey bot passed into column');
    return '';
  }
  let c = colMap[col];
  if (c) {
    return c(bot, ele);
  }
  let values = attribute(bot, col, 'none');
  values = values.map((v) => applyAlias(v, col));
  return longestOrAll(values, ele._verbose);
}

/** devices returns a potentially empty list of devices (e.g. Android devices)
 *  that are on this machine. This will generally be length 1 or 0 (although)
 *  the UI has some support for multiple devices.
 */
export function devices(bot) {
  return bot.state.devices || [];
}

/** filterPossibleColumns shows only those columns that match the given query.
 *  This means, if there is a part of the query in the column (ignoring case).
 */
export function filterPossibleColumns(allCols, query) {
  if (!query) {
    return allCols;
  }
  return allCols.filter((c) => {
    return matchPartCaseInsensitive(c, query);
  });
}

/** filterPossibleKeys shows only those keys that match the given query.
 *  This means, if there is a part of the key in the column (ignoring case),
 *  or if any value associated to the key (via keyMap) matches.
 */
export function filterPossibleKeys(allKeys, keyMap, query) {
  if (!query) {
    return allKeys;
  }
  // Allow partially typed filters to still match parts.
  query = query.replace(':', ' ').trim();
  return allKeys.filter((k) => {
    if (matchPartCaseInsensitive(k, query)) {
      return true;
    }
    let values = keyMap[k] || [];
    for (let value of values) {
      value = applyAlias(value, k);
      if (matchPartCaseInsensitive(value, query)) {
        return true;
      }
    }
    return false;
  });
}

/** filterPossibleValues shows some values associated with the given query.
 * If the user has typed in a query, show all secondary elements if
 * their primary element matches.  If it doesn't match the primary
 * element, only show those secondary elements that do.
 */
export function filterPossibleValues(allValues, selectedKey, query) {
  query = query.replace(':', ' ').trim();
  if (!query || matchPartCaseInsensitive(selectedKey, query)) {
    return allValues;
  }
  return allValues.filter((v) => {
    v = applyAlias(v, selectedKey);
    if (matchPartCaseInsensitive(v, query)) {
      return true;
    }
    return false;
  });
}

// A list of special rules for filters. In practice, this is anything
// that is not a dimension, since the API only supports filtering by
// dimensions and these values.
export const specialFilters = {
  id: function(bot, id) {
    return bot.bot_id === id;
  },
  is_mp_bot: function(bot, match) {
    if (match === 'true') {
      return !!bot.lease_id;
    } else if (match === 'false') {
      return !bot.lease_id;
    }
    return true;
  },
  status: function(bot, status) {
    if (status === 'quarantined') {
      return bot.quarantined;
    } else if (status === 'maintenance') {
      return !!bot.maintenance_msg;
    } else if (status === 'dead') {
      return bot.is_dead;
    } else {
      // Status must be 'alive'.
      return !bot.quarantined && !bot.is_dead;
    }
  },
  task: function(bot, task) {
    if (task === 'idle') {
      return !bot.task_id;
    }
    // Task must be 'busy'.
    return !!bot.task_id;
  }
};

/** Filters the bots like they would be filtered from the server
 * filters: Array<String>: like ['alpha:beta']
 * bots: Array<Object>: the bot objects.
 *
 * returns the bots that match the filters.
*/
export function filterBots(filters, bots) {
  let parsedFilters = [];
  // Preprocess the filters
  for (let filterString of filters) {
    let idx = filterString.indexOf(':');
    let key = filterString.slice(0, idx);
    let value = filterString.slice(idx + 1);
    parsedFilters.push([key, value]);
  }
  // apply the filters in an AND way, that is, it must
  // match all the filters
  return bots.filter((bot) => {
    let matches = true;
    for (let filter of parsedFilters) {
      let [key, value] = filter;
      if (specialFilters[key]) {
        matches &= specialFilters[key](bot, value);
      } else {
        // it's a dimension
        matches &= (attribute(bot, key, []).indexOf(value) !== -1);
      }
    }
    return matches;
  });
}

/** fromDimensions returns the array of dimension values that match the given
 *  key or null if this bot doesn't have that dimension.
 * @param {Object} bot - The bot from which to extract data.
 * @param {string} dim - The 'key' of the dimension to look for.
 */
export function fromDimension(bot, dim) {
  if (!bot || !bot.dimensions || !dim) {
    return null;
  }
  for (let i = 0; i < bot.dimensions.length; i++) {
    if (bot.dimensions[i].key === dim) {
      return bot.dimensions[i].value;
    }
  }
  return null;
}

/** fromState returns the array of values that match the given key
 *  from a bot's state or null if this bot doesn't have it..
 * @param {Object} bot - The bot from which to extract data.
 * @param {string} attr - The 'key' of the state data to look for.
 */
export function fromState(bot, attr) {
  if (!bot || !bot.state || !bot.state[attr]) {
    return null;
  }
  let state = bot.state[attr];
  if (Array.isArray(state)) {
    return state;
  }
  return [state];
}

// The list of things we do count data for, in the order they are presented.
const countTypes = ['All', 'Alive', 'Busy', 'Idle', 'Dead',
                    'Quarantined', 'Maintenance'];

/** initCounts creates the default list of objects for displaying counts.
 */
export function initCounts() {
  return countTypes.map((label) => {return {'label': label, 'key': ''}});
}

/** listQueryParams returns a query string for the /list API based on the
 *  passed in args.
 *  @param {Array<string>} filters - a list of colon-separated key-values.
 *  @param {Number} limit - the limit of results to return.
 *  @param {String} cursor - An optional cursor for server pagination.
 */
export function listQueryParams(filters, limit, cursor) {
  let params = {};
  let dims = [];
  filters.forEach((f) => {
    let split = f.split(':', 1)
    let col = split[0];
    let rest = f.substring(col.length + 1);
    if (col === 'status') {
      if (rest === 'alive') {
        params['is_dead'] = ['FALSE'];
        params['quarantined'] = ['FALSE'];
        params['in_maintenance'] = ['FALSE'];
      } else if (rest === 'quarantined') {
        params['quarantined'] = ['TRUE'];
      } else if (rest === 'maintenance') {
        params['in_maintenance'] = ['TRUE'];
      } else if (rest === 'dead') {
        params['is_dead'] = ['TRUE'];
      }
    } else if (col === 'is_mp_bot') {
      if (rest === 'true') {
        params['is_mp'] = ['TRUE'];
      } else if (rest === 'false') {
        params['is_mp'] = ['FALSE'];
      }
    } else if (col === 'task') {
       if (rest === 'busy') {
        params['is_busy'] = ['TRUE'];
      } else if (rest === 'idle') {
        params['is_busy'] = ['FALSE'];
      }
    } else {
      // We can assume dimension here. The only other possibility
      // is that a user has changed their filters w/o using the UI
      // (which checks proper dimensions) and garbage in == garbage out.
      dims.push(col + ':' + rest);
    }
  });
  params['dimensions'] = dims;
  params['limit'] = limit;
  if (cursor) {
    params['cursor'] = cursor;
  }
  return query.fromObject(params);
}

/** longestOrAll returns the longest (by string length) value of the array
 *  or all the values of the array joined with | if verbose is true.
 *  @param {Array<string>} arr - Any list of string values.
 *  @param {Boolean} verbose - If all the values should be returned.
 *  @return {string} the longest value.
 */
export function longestOrAll(arr, verbose) {
  if (verbose) {
    return arr.join(' | ');
  }
  let most = '';
  for(let i = 0; i < arr.length; i++) {
    if (arr[i] && arr[i].length > most.length) {
      most = arr[i];
    }
  }
  return most;
}

/** makeFilter returns a filter based on the key and value. */
export function makeFilter(key, value) {
  return `${key}:${value}`;
}

/** matchPartCaseInsensitive returns true or false if str matches any
 *of a space separated list of queries,
 */
export function matchPartCaseInsensitive(str, queries) {
  if (!queries) {
    return true;
  }
  if (!str) {
    return false
  }
  queries = queries.trim().toLocaleLowerCase();
  str = str.toLocaleLowerCase();
  let xq = queries.split(' ');
  for (let query of xq) {
    let idx = str.indexOf(query);
    if (idx !== -1) {
      return true;
    }
  }
  return false;
};

const BOT_TIMES = ['first_seen_ts', 'last_seen_ts', 'lease_expiration_ts'];

/** processBots processes the array of bots from the server and returns it.
 *  The primary goal is to get the data ready for display.
 */
export function processBots(arr) {
  if (!arr) {
    return [];
  }
  for (let bot of arr) {
    bot.state = (bot.state && JSON.parse(bot.state)) || {};
    // get the disks in an easier to deal with format, sorted by size.
    let disks = bot.state.disks || {};
    let keys = Object.keys(disks);
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

    // Make sure every bot has a state.temp object and precompute
    // average and list of temps by zone if applicable.
    bot.state.temp = aggregateTemps(bot.state.temp);

    let devices = [];
    let d = (bot && bot.state && bot.state.devices) || {};
    // state.devices is like {Serial:Object}, so we need to keep the serial
    for (let key in d) {
      let o = d[key];
      o.serial = key;
      o.okay = (o.state === 'available');
      // It is easier to assume all devices on a bot are of the same type
      // than to pick through the (incomplete) device state and find it.
      // Bots that are quarantined because they have no devices
      // still have devices in their state (the last known device attached)
      // but don't have the device_type dimension. In that case, we punt
      // on device type.
      let types = fromDimension(bot, 'device_type') || ['UNKNOWN'];
      o.device_type = types[0];
      o.temp = aggregateTemps(o.temp);
      devices.push(o);
    }
    // For determinism, sort by device id
    devices.sort((a,b) => {
      // Don't use natural sort because that can confusingly put
      // 89ABCDEF012 before 3456789ABC
      if (a.serial < b.serial) {
        return -1;
      } else if (a.serial > b.serial) {
        return 1;
      }
      return 0;
    });
    bot.state.devices = devices;

    for (let time of BOT_TIMES) {
      sanitizeAndHumanizeTime(bot, time);
    };
  };

  return arr;
}

/** processCounts picks the data from the passed in JSON and feeds it into
 *  the passed in array of objects (see initCounts).
 */
export function processCounts(output, countJSON) {
  // output is expected to be in the order described by countTypes.
  output[0].value = parseInt(countJSON.count);                            // All
  output[1].value = parseInt(countJSON.count) - parseInt(countJSON.dead)
                  - parseInt(countJSON.quarantined)
                  - parseInt(countJSON.maintenance);                      // Alive
  output[2].value = parseInt(countJSON.busy);                             // Busy
  output[3].value = parseInt(countJSON.count) - parseInt(countJSON.busy); // Idle
  output[4].value = parseInt(countJSON.dead);                             // Dead
  output[5].value = parseInt(countJSON.quarantined);                      // Quarantined
  output[6].value = parseInt(countJSON.maintenance);                      // Maintenance
  return output;
}

/** processDimensions processes the array of dimensions from the server
 *  and returns it. The primary objective is to remove blacklisted
 *  dimensions and make sure any are there that the server doesn't provide.
 *  It will get fed into processPrimaryMap.
 */
export function processDimensions(arr) {
  if (!arr) {
    return [];
  }
  let dims = [];
  arr.forEach(function(d) {
    if (blacklistDimensions.indexOf(d.key) === -1) {
      dims.push(d.key);
    }
  });
  // Make sure 'id' is in there, but not duplicated (see blacklistDimensions)
  dims.push('id');
  dims.sort();
  return dims;
}

/** processDimensions creates a map of primary keys (e.g. left column) based
 *  on dimensions and other interesting options (e.g. device-related things).
 *  The primary keys map to the values they could be filtered by.
 */
export function processPrimaryMap(dimensions) {
  // pMap will have a list of columns to available values (primary key
  // to secondary values). This includes bot dimensions, but also
  // includes state like disk_space, quarantined, busy, etc.
  dimensions = dimensions || [];

  var pMap = {};
  dimensions.forEach(function(d) {
    if (blacklistDimensions.indexOf(d.key) >= 0) {
      return;
    }
    // TODO(kjlubick): Are we aliasing things?
    if (true || swarming.alias.DIMENSIONS_WITH_ALIASES.indexOf(d.key) === -1) {
      // value is an array of all seen values for the dimension d.key
      pMap[d.key] = d.value;
    } else {
      var aliased = [];
      d.value.forEach(function(value) {
        aliased.push(swarming.alias.apply(value, d.key));
      });
      pMap[d.key] = aliased;
    }
  });

  // Add some options that might not show up.
  pMap['android_devices'] && pMap['android_devices'].push('0');
  pMap['device_os'] && pMap['device_os'].push('none');
  pMap['device_type'] && pMap['device_type'].push('none');

  pMap['id'] = null;

  // Create custom filter/sorting options
  pMap['task'] = ['busy', 'idle'];
  pMap['status'] = ['alive', 'dead', 'quarantined', 'maintenance'];
  pMap['is_mp_bot'] = ['true', 'false'];

  // No need to sort any of this, bot-filters sorts secondary items
  // automatically, especially when the user types a query.
  return pMap;
}


const specialColOrder = ['id', 'task'];

// Returns the sort order of 2 columns. Puts 'special' columns first and then
// sorts the rest alphabetically.
function compareColumns(a, b) {
  let aSpecial = specialColOrder.indexOf(a);
  if (aSpecial === -1) {
    aSpecial = specialColOrder.length+1;
  }
  let bSpecial = specialColOrder.indexOf(b);
  if (bSpecial === -1) {
    bSpecial = specialColOrder.length+1;
  }
  if (aSpecial === bSpecial) {
    // Don't need naturalSort unless we get some funky column names.
    return a.localeCompare(b);
  }
  // Lower rank in specialColOrder prevails.
  return aSpecial - bSpecial;
}

/** sortColumns sorts the bot-list columns in mostly alphabetical order. Some
  columns (id, task) go first to maintain with behavior from previous
  versions.
  @param cols Array<String> The columns
*/
export function sortColumns(cols) {
  cols.sort(compareColumns);
}

/** sortPossibleColumns sorts the columns in the column selector. It puts the
 *  selected ones on top in the order they are displayed and the rest below
 *  in alphabetical order.
 */
export function sortPossibleColumns(keys, selectedCols) {
  let selected = {};
  for (let c of selectedCols) {
    selected[c] = true;
  }

  keys.sort((a, b) => {
      // Show selected columns above non selected columns
      let selA = selected[a];
      let selB = selected[b];
      if (selA && !selB) {
        return -1;
      }
      if (selB && !selA) {
        return 1;
      }
      if (selA && selB) {
        // Both keys are selected, thus we put them in display order.
        return compareColumns(a, b);
      }
      // neither column was selected, fallback to alphabetical sorting.
      return a.localeCompare(b);
  });
}

/** taskLink creates the href attribute for linking to a single task.*/
export function taskLink(taskId, disableCanonicalID) {
  if (!taskId) {
    return undefined;
  }
  if (!disableCanonicalID) {
    // task abcefgh0 is the 'canonical' task id. The first try has the id
    // abcefgh1. If there is a second (transparent retry), it will be
    // abcefgh2.  We almost always want to link to the canonical one,
    // because the milo output (if any) will only be generated for
    // abcefgh0, not abcefgh1 or abcefgh2.
    taskId = taskId.substring(0, taskId.length - 1) + '0';
  }
  return `/task?id=${taskId}`;
}

function timeDiffApprox(date) {
  if (!date) {
    return 'eons';
  }
  return human.diffDate(date.getTime());
}

const blacklistDimensions = ['quarantined', 'error', 'id'];

/** extraKeys is a list of things we want to be able to sort by or display
 *  that are not dimensions.
.*/
export const extraKeys = ['disk_space', 'uptime', 'running_time', 'task',
'status', 'version', 'external_ip', 'internal_ip', 'mp_lease_id',
'mp_lease_expires', 'last_seen', 'first_seen', 'battery_level',
'battery_voltage', 'battery_temperature', 'battery_status', 'battery_health',
'bot_temperature', 'device_temperature', 'serial_number'];

/** colHeaderMap maps keys to their human readable name.*/
export const colHeaderMap = {
  'id': 'Bot Id',
  'mp_lease_id': 'Machine Provider Lease Id',
  'task': 'Current Task',
  'android_devices': 'Android Devices',
  'battery_health': 'Battery Health',
  'battery_level': 'Battery Level (%)',
  'battery_status': 'Battery Status',
  'battery_temperature': 'Battery Temp (°C)',
  'battery_voltage': 'Battery Voltage (mV)',
  'bot_temperature': 'Bot Temp (°C)',
  'cores': 'CPU Core Count',
  'cpu': 'CPU type',
  'device': 'Non-android Device',
  'device_os': 'Device OS',
  'device_temperature': 'Device Temp (°C)',
  'device_type': 'Device Type',
  'disk_space': 'Free Space (MB)',
  'external_ip': 'External IP',
  'first_seen': 'First Seen',
  'gpu': 'GPU type',
  'internal_ip': 'Internal or Local IP',
  'last_seen': 'Last Seen',
  'mp_lease_expires': 'Machine Provider Lease Expires',
  'os': 'OS',
  'pool': 'Pool',
  'running_time': 'Swarming Uptime',
  'serial_number': 'Device Serial Number',
  'status': 'Status',
  'uptime': 'Bot Uptime',
  'xcode_version': 'XCode Version',
  'version': 'Client Code Version',
};

// Taken from http://developer.android.com/reference/android/os/BatteryManager.html
const BATTERY_HEALTH_ALIASES = {
  1: 'Unknown',
  2: 'Good',
  3: 'Overheated',
  4: 'Dead',
  5: 'Over Voltage',
  6: 'Unspecified Failure',
  7: 'Too Cold',
}

const BATTERY_STATUS_ALIASES = {
  1: 'Unknown',
  2: 'Charging',
  3: 'Discharging',
  4: 'Not Charging',
  5: 'Full',
}

/** specialSortMap maps keys to their special sort rules, encapsulated in a
 *  function. The function takes in the current sort direction (1 for ascending)
 *  and -1 for descending and both bots and should return a number a la compare.
 */
export const specialSortMap = {
  id: (dir, botA, botB) => dir * naturalSort(botA.bot_id, botB.bot_id),
};

function deviceHelper(callback) {
  return (bot, ele) => {
    let devices = bot.state.devices;
    if (!devices || !devices.length) {
      return 'N/A - no devices';
    }
    return devices.map(callback).join(' | ');
  };
}

const colMap = {
  android_devices: (bot, ele) => {
    let devs = attribute(bot, 'android_devices', '0');
    if (ele._verbose) {
      return devs.join(' | ') + ' devices available';
    }
    // max() works on strings as long as they can be coerced to Number.
    return Math.max(...devs) + ' devices available';
  },
  battery_health: deviceHelper((device) => {
    let h = (device.battery && device.battery.health) || 'UNKNOWN';
    let alias = BATTERY_HEALTH_ALIASES[h] || '';
    return `${alias} (${h})`;
  }),
  battery_level: deviceHelper((device) => {
    return (device.battery && device.battery.level) || 'UNKNOWN';
  }),
  battery_status: deviceHelper((device) => {
    let h = (device.battery && device.battery.status) || 'UNKNOWN';
    let alias = BATTERY_STATUS_ALIASES[h] || '';
    return `${alias} (${h})`;
  }),
  battery_temperature: deviceHelper((device) => {
    // Battery temps are in tenths of degrees C - convert to more human range.
    return (device.battery && device.battery.temperature / 10) || 'UNKNOWN';
  }),
  battery_voltage: deviceHelper((device) => {
    return (device.battery && device.battery.voltage) || 'UNKNOWN';
  }),
  bot_temperature: (bot, ele) => {
    if (ele._verbose) {
      return bot.state.temp.zones || 'UNKNOWN';
    }
    return bot.state.temp.average || 'UNKNOWN';
  },
  device_temperature: (bot, ele) => {
    let devices = bot.state.devices;
    if (!devices || !devices.length) {
      return 'N/A - no devices';
    }
    return devices.map((device) => {
      if (ele._verbose) {
        return device.temp.zones || UNKNOWN;
      }
      return device.temp.average || UNKNOWN;
    }).join(' | ');
  },
  disk_space: (bot, ele) => {
    let aliased = [];
    for (let disk of bot.disks) {
      let alias = human.bytes(disk.mb, human.MB);
      aliased.push(`${disk.id} ${alias} (${disk.mb})`);
    }
    if (ele._verbose) {
      return aliased.join(' | ');
    }
    return aliased[0];
  },
  external_ip: (bot, ele) => {
    return bot.external_ip || 'none';
  },
  first_seen: (bot, ele) => {
    return human.localeTime(bot.first_seen_ts);
  },
  id: (bot, ele) => html`<a target=_blank
                            rel=noopener
                            href=${botLink(bot.bot_id)}>${bot.bot_id}</a>`,
  internal_ip: (bot, ele) => {
    return attribute(bot, 'ip', 'none')[0];
  },
  last_seen: (bot, ele) => {
    if (ele._verbose) {
      return human.localeTime(bot.last_seen_ts);
    }
    return timeDiffApprox(bot.last_seen_ts) + ' ago';
  },
  mp_lease_id: (bot, ele) => {
    if (!bot.lease_id) {
      return 'none';
    }
    let id = bot.lease_id;
    if (!ele._verbose) {
      id = id.substring(0, 10);
    }
    if (ele.server_details && ele.server_details.machine_provider_template) {
      // Might not be loaded yet.
      let mp_url = ele.server_details.machine_provider_template
                      .replace('%s', bot.lease_id);
      return html`<a target=_blank
                     rel=noopener
                     href=${mp_url}>${id}</a>`;
    }
    return id;
  },
  mp_lease_expires: (bot, ele) => {
    if (!bot.lease_expiration_ts) {
      return 'N/A';
    }
    if (ele._verbose) {
      return human.localeTime(bot.lease_expiration_ts);
    }
    if (bot.lease_expiration_ts < new Date()) {
      return timeDiffApprox(bot.lease_expiration_ts) + ' ago';
    }
    return 'in ' + timeDiffApprox(bot.lease_expiration_ts);
  },
  running_time:  (bot, ele) => {
    let r = fromState(bot, 'running_time');
    if (!r) {
      return 'UNKNOWN';
    }
    return human.strDuration(r);
  },
  serial_number: deviceHelper((device) => {
    return device.serial || 'UNKNOWN';
  }),
  status: (bot, ele) => {
    if (bot.is_dead) {
      return `Dead. Last seen ${human.diffDate(bot.last_seen_ts)} ago`;
    }
    if (bot.quarantined) {
      let msg = fromState(bot, 'quarantined');
      if (msg) {
        msg = msg[0];
      };
      // Sometimes, the quarantined message is actually in 'error'.  This
      // happens when the bot code has thrown an exception.
      if (!msg || msg === 'true' || msg === true) {
        msg = attribute(bot, 'error')[0];
      }
      // Other times, the bot has reported it is quarantined by setting the
      // dimension 'quarantined' to be something.
      if (msg === 'UNKNOWN') {
        msg = fromDimension(bot, 'quarantined') || 'UNKNOWN';
      }
      let deviceStates = [];
      // Show all the errors that are active on devices to make it more
      // clear if this is a transient error (e.g. device is too hot)
      // or if it is requires human interaction (e.g. device is unauthorized)
      devices(bot).forEach(function(d) {
          deviceStates.push(d.state);
      });
      if (deviceStates.length) {
        msg += ` devices: [${deviceStates.join(', ')}]`;
      }
      return `Quarantined: ${msg}`;
    }
    if (bot.maintenance_msg) {
      return `Maintenance: ${bot.maintenance_msg}`;
    }
    return 'Alive';
  },
  task: (bot, ele) => {
    if (!bot.task_id) {
      return 'idle';
    }
    return html`<a target=_blank
                   rel=noopener
                   title=${bot.task_name}
                   href=${taskLink(bot.task_id)}>${bot.task_id}</a>`;
  },
  uptime: (bot, ele) => {
    let u = fromState(bot, 'uptime');
    if (!u) {
      return 'UNKNOWN';
    }
    return human.strDuration(u);
  },
  version: (bot, ele) => {
    let v = bot.version || 'UNKNOWN';
    if (ele._verbose) {
      return v;
    }
    return v.substring(0, 10);
  }
};

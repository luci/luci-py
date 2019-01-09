// Copyright 2018 The LUCI Authors. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

/** @module swarming-ui/util
 * @description
 *
 * <p>
 *  A general set of useful functions.
 * </p>
 */

import * as human from 'common-sk/modules/human'
import * as query from 'common-sk/modules/query'

export function botPageLink(bot_id) {
  if (!bot_id) {
    return undefined;
  }
  return '/bot?id=' + bot_id;
}

/** compareWithFixedOrder returns the sort order of 2 strings. It puts
 * fixedOrder elements first and then sorts the rest alphabetically.
 *
 * @param {Array<String>} fixedOrder - special elements that should be sorted
 *    in the order provided.
 */
export function compareWithFixedOrder(fixedOrder) {
  if (!fixedOrder) {
    fixedOrder = [];
  }
  return function(a, b) {
    let aSpecial = fixedOrder.indexOf(a);
    if (aSpecial === -1) {
      aSpecial = fixedOrder.length+1;
    }
    let bSpecial = fixedOrder.indexOf(b);
    if (bSpecial === -1) {
      bSpecial = fixedOrder.length+1;
    }
    if (aSpecial === bSpecial) {
      // Don't need naturalSort since elements shouldn't
      // have numbers as prefixes.
      return a.localeCompare(b);
    }
    // Lower rank in fixedOrder prevails.
    return aSpecial - bSpecial;
  }
}

/** humanDuration formats a duration to be more human readable.
 *
 * @param {Number} timeInSecs - The duration to be formatted.
 */
export function humanDuration(timeInSecs) {
  // If the timeInSecs is 0 (e.g. duration of Terminate bot tasks), we
  // still want to display 0s.
  if (timeInSecs === 0 || timeInSecs === '0') {
    return '0s';
  }
  // Otherwise, if timeInSecs is falsey (e.g. undefined), return empty
  // string to reflect that.
  if (!timeInSecs) {
    return '--';
  }
  let ptimeInSecs = parseFloat(timeInSecs);
  // On a bad parse (shouldn't happen), show original.
  if (!ptimeInSecs) {
    return timeInSecs + ' seconds';
  }

  // For times greater than a minute, make them human readable
  // e.g. 2h 43m or 13m 42s
  if (ptimeInSecs > 60) {
    return human.strDuration(ptimeInSecs);
  }
  // For times less than a minute, add 10ms resolution.
  return ptimeInSecs.toFixed(2)+'s';
}

/** sanitizeAndHumanizeTime parses a date string or ms_since_epoch into a JS
 *  Date object, assuming UTC time. It also creates a human readable form in
 *  the obj under a key with a human_ prefix.  E.g.
 *  sanitizeAndHumanizeTime(foo, 'some_ts')
 *  parses the string/int at foo['some_ts'] such that foo['some_ts'] is now a
 *  Date object and foo['human_some_ts'] is the human formated version from
 *  human.localeTime.
 */
export function sanitizeAndHumanizeTime(obj, key) {
  obj['human_'+key] = '--';
  if (obj[key]) {
    if (obj[key].endsWith && !obj[key].endsWith('Z')) {
      // Timestamps from the server are missing the 'Z' that specifies Zulu
      // (UTC) time. If that's not the case, add the Z. Otherwise, some
      // browsers interpret this as local time, which throws off everything.
      // TODO(kjlubick): Should the server output milliseconds since the
      // epoch?  That would be more consistent.
      // See http://crbug.com/714599
      obj[key] += 'Z';
    }
    obj[key] = new Date(obj[key]);

    // Extract the timezone.
    var str = obj[key].toString();
    var timezone = str.substring(str.indexOf('('));

    // If timestamp is today, skip the date.
    var now = new Date();
    if (obj[key].getDate() == now.getDate() &&
        obj[key].getMonth() == now.getMonth() &&
        obj[key].getYear() == now.getYear()) {
      obj['human_'+key] = obj[key].toLocaleTimeString() + ' ' + timezone;
    } else {
      obj['human_'+key] = obj[key].toLocaleString() + ' ' + timezone;
    }
  }
}

/** taskListLink creates a link to a task list with the preloaded
 *  filters and columns.
 *  @param {Array<String|Object> filters - If Array<Object>, Object
 *    should be {key:String, value:String} or
 *    {key:String, value:Array<String>}. If Array<String>, the Strings
 *    should be valid filters (e.g. 'foo:bar').
 *  @param {Array<String>} columns - the column names that should be shown.
 *
 *  Any trailing args after columns will be assumed to be strings that
 *  should be treated as valid filters.
 */
export function taskListLink(filters, columns) {
  filters = filters || [];
  columns = columns || [];
  let fArr = [];
  for (let f of filters) {
    if (f.key && f.value) {
      if (Array.isArray(f.value)) {
        f.value.forEach(function(v) {
          fArr.push(f.key + ':' + v);
        });
      } else {
        fArr.push(f.key + ':' + f.value);
      }
    } else {
      fArr.push(f);
    }
  }
  // can't use .foreach, as arguments isn't really an Array.
  for (let i = 2; i < arguments.length; i++) {
    fArr.push(arguments[i]);
  }
  let obj = {
    f: fArr,
    c: columns,
  }
  return '/tasklist?' + query.fromParamSet(obj);
}

/** taskPageLink creates the href attribute for linking to a single task.
 *
 * @param {String} taskId - The full taskID
 * @param {Boolean} disableCanonicalID - For a given task, a canonical task id
 *   will look like 'abcefgh0'. The first try has the id
 *   abcefgh1. If there is a second (transparent retry), it will be
 *   abcefgh2.  We almost always want to link to the canonical one,
 *   because the milo output (if any) will only be generated for
 *   abcefgh0, not abcefgh1 or abcefgh2.
 */
export function taskPageLink(taskId, disableCanonicalID) {
  if (!taskId) {
    return undefined;
  }
  if (!disableCanonicalID) {
    taskId = taskId.substring(0, taskId.length - 1) + '0';
  }
  return `/task?id=${taskId}`;
}

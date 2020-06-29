// Copyright 2019 The LUCI Authors. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

export const ONGOING_STATES = new Set([
  'PENDING',
  'RUNNING',
]);

export const EXCEPTIONAL_STATES = new Set([
  'TIMED_OUT',
  'EXPIRED',
  'NO_RESOURCE',
  'CANCELED',
  'KILLED',
]);

export const COUNT_FILTERS = [
  {label: 'Total', value: '...'},
  {label: 'Success', value: '...', filter: 'COMPLETED_SUCCESS'},
  {label: 'Failure', value: '...', filter: 'COMPLETED_FAILURE'},
  {label: 'Pending', value: '...', filter: 'PENDING'},
  {label: 'Running', value: '...', filter: 'RUNNING'},
  {label: 'Timed Out', value: '...', filter: 'TIMED_OUT'},
  {label: 'Bot Died', value: '...', filter: 'BOT_DIED'},
  {label: 'Deduplicated', value: '...', filter: 'DEDUPED'},
  {label: 'Expired', value: '...', filter: 'EXPIRED'},
  {label: 'No Resource', value: '...', filter: 'NO_RESOURCE'},
  {label: 'Canceled', value: '...', filter: 'CANCELED'},
  {label: 'Killed', value: '...', filter: 'KILLED'},
];

export const FILTER_STATES = [
  'ALL',
  'COMPLETED',
  'COMPLETED_SUCCESS',
  'COMPLETED_FAILURE',
  'RUNNING',
  'PENDING',
  'PENDING_RUNNING',
  'BOT_DIED',
  'DEDUPED',
  'TIMED_OUT',
  'EXPIRED',
  'NO_RESOURCE',
  'CANCELED',
  'KILLED',
];

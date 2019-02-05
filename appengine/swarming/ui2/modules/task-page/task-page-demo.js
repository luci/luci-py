// Copyright 2019 The LUCI Authors. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.
import './index.js'

import { taskOutput, taskResult, taskRequest } from './test_data'
import { requireLogin, mockAuthdAppGETs } from '../test_util'
import { $$ } from 'common-sk/modules/dom'

(function(){
// Can't use import fetch-mock because the library isn't quite set up
// correctly for it, and we get strange errors about 'this' not being defined.
const fetchMock = require('fetch-mock');

mockAuthdAppGETs(fetchMock, {
  cancel_task: true,
});

fetchMock.get('glob:/_ah/api/swarming/v1/task/*/request',
              requireLogin(taskRequest, 100));

fetchMock.get('glob:/_ah/api/swarming/v1/task/*/result?include_performance_stats=true',
              requireLogin(taskResult, 200));

fetchMock.get('glob:/_ah/api/swarming/v1/task/*/stdout',
              requireLogin(taskOutput, 100));

// Everything else
fetchMock.catch(404);

const ele = $$('task-page');
if (!ele._taskId) {
  ele._taskId = 'testid000';
}

// autologin for ease of testing locally - comment this out if using the real flow.
$$('oauth-login')._logIn();
})();
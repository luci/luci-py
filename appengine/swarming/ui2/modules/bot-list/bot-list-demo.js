// Copyright 2018 The LUCI Authors. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

import './index.js'

import { data_s10, fleetCount, fleetDimensions, queryCount } from './test_data'
import { requireLogin, mockAuthdAppGETs } from '../test_util'

(function(){
// Can't use import fetch-mock because the library isn't quite set up
// correctly for it, and we get strange errors about 'this' not being defined.
const fetchMock = require('fetch-mock');

// uncomment to stress test with 5120 items
// data_s10.items.push(...data_s10.items);
// data_s10.items.push(...data_s10.items);
// data_s10.items.push(...data_s10.items);
// data_s10.items.push(...data_s10.items);
// data_s10.items.push(...data_s10.items);
// data_s10.items.push(...data_s10.items);
// data_s10.items.push(...data_s10.items);
// data_s10.items.push(...data_s10.items);
// data_s10.items.push(...data_s10.items);

mockAuthdAppGETs(fetchMock, {
  delete_bot: false,
});

fetchMock.get('glob:/_ah/api/swarming/v1/bots/list?*',
              requireLogin(data_s10, 500));

fetchMock.get('/_ah/api/swarming/v1/bots/dimensions',
              requireLogin(fleetDimensions));

fetchMock.get('/_ah/api/swarming/v1/bots/count',
              requireLogin(fleetCount));
fetchMock.get('glob:/_ah/api/swarming/v1/bots/count?*',
              requireLogin(queryCount));

// Everything else
fetchMock.catch(404);

// autologin for ease of testing locally
document.querySelector('oauth-login')._logIn();
})();
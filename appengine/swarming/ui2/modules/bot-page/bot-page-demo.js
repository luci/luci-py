// Copyright 2019 The LUCI Authors. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.
import './index.js'

import { botData } from './test_data'
import { requireLogin, mockAuthdAppGETs } from '../test_util'
import { $$ } from 'common-sk/modules/dom'

(function(){
// Can't use import fetch-mock because the library isn't quite set up
// correctly for it, and we get strange errors about 'this' not being defined.
const fetchMock = require('fetch-mock');

mockAuthdAppGETs(fetchMock, {
  delete_bot: true,
  terminate_bot: true,
});

fetchMock.get('glob:/_ah/api/swarming/v1/bot/*/get',
              requireLogin(botData, 100));

// Everything else
fetchMock.catch(404);

const ele = $$('bot-page');
if (!ele._botId) {
  ele._botId = 'running';
}
// autologin for ease of testing locally - comment this out if using the real flow.
$$('oauth-login')._logIn();
})();
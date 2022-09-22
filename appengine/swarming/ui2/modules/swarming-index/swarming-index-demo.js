// Copyright 2018 The LUCI Authors. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

import './index.js';
import {requireLogin, mockAuthdAppGETs} from '../test_util';
import fetchMock from 'fetch-mock';

(function() {
  mockAuthdAppGETs(fetchMock, {
    get_bootstrap_token: true,
  });

  const logged_in_token = {
    bootstrap_token: '8675309JennyDontChangeYourNumber8675309',
  };

  fetchMock.post('/_ah/api/swarming/v1/server/token',
      requireLogin(logged_in_token, 1500));

  // Everything else
  fetchMock.catch(404);
})();

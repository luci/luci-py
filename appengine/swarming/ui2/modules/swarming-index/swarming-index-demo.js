// Copyright 2018 The LUCI Authors. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

import './index.js'

// Can't use import fetch-mock because the library isn't quite set up
// correctly for it, and we get strange errors about 'this' not being defined.
const fetchMock = require('fetch-mock');

function requireLogin(logged_in, delay=500) {
  return function(url, opts){
    if (opts && opts.headers && opts.headers.authorization) {
      return new Promise((resolve) => {
        setTimeout(resolve, delay);
      }).then(() => {
        return {
          status: 200,
          body: JSON.stringify(logged_in),
          headers: {'Content-Type':'application/json'},
        };
      });
    } else {
      return new Promise((resolve) => {
        setTimeout(resolve, delay);
      }).then(() => {
        return {
          status: 403,
          body: 'Try logging in',
          headers: {'Content-Type':'text/plain'},
        };
      });
    }
  };
}

const details = {
  server_version: '1234-deadbeef',
  bot_version: 'abcdoeraymeyouandme',
};

fetchMock.get('/_ah/api/swarming/v1/server/details', requireLogin(details));


const logged_in_permissions = {
  get_bootstrap_token: true
};


fetchMock.get('/_ah/api/swarming/v1/server/permissions',
              requireLogin(logged_in_permissions));

const logged_in_token = {
  bootstrap_token: '8675309JennyDontChangeYourNumber8675309'
};

fetchMock.post('/_ah/api/swarming/v1/server/token',
              requireLogin(logged_in_token, 1500));

// Everything else
fetchMock.catch(404);
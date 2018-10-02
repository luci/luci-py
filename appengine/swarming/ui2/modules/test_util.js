// Copyright 2018 The LUCI Authors. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

/** @module swarming-ui/test_util
 * @description
 *
 * <p>
 *  A general set of useful functions for tests and demos,
 *  e.g. reducing boilerplate.
 * </p>
 */

export const toContainRegexMatcher = {
  // see https://jasmine.github.io/tutorials/custom_matcher
  // for docs on the factory that returns a matcher.
  'toContainRegex': function(util, customEqualityTesters) {
    return {
      'compare': function(actual, regex) {
        if (!(regex instanceof RegExp)) {
          throw `toContainRegex expects a regex, got ${JSON.stringify(regex)}`;
        }
        let result = {};

        if (!actual || !actual.length) {
          result.pass = false;
          result.message = `Expected ${actual} to be a non-empty array `+
                           `containing something matching ${regex}`;
          return result;
        }
        for (let s of actual) {
          if (s.match && s.match(regex)) {
            result.pass = true;
            // craft the message for the negated version (i.e. using .not)
            result.message = `Expected ${actual} not to have anyting `+
                             `matching ${regex}, but ${s} did`;
            return result;
          }
        }
        result.message = `Expected ${actual} to have element matching ${regex}`;
        result.pass = false;
        return result;
      },
    };
  },
};

export function mockAppGETs(fetchMock, permissions) {
  fetchMock.get('/_ah/api/swarming/v1/server/details', {
    server_version: '1234-abcdefg',
    bot_version: 'abcdoeraymeyouandme',
  });


  fetchMock.get('/_ah/api/swarming/v1/server/permissions', permissions);
}

export function mockAuthdAppGETs(fetchMock, permissions) {
  fetchMock.get('/_ah/api/swarming/v1/server/details', requireLogin({
    server_version: '1234-abcdefg',
    bot_version: 'abcdoeraymeyouandme',
  }));


  fetchMock.get('/_ah/api/swarming/v1/server/permissions',
                requireLogin(permissions));
}

// TODO(kjlubick): put delay back to 500 or so
export function requireLogin(logged_in, delay=100) {
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
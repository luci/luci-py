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

export const customMatchers = {
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

  'toHaveAttribute': function(util, customEqualityTesters) {
    return {
      'compare': function(actual, attribute) {
        if (!isElement(actual)) {
          throw `${actual} is not a DOM element`;
        }
        return {
          pass: actual.hasAttribute(attribute),
        };
      },
    };
  },

  // Trims off whitespace before comparing
  'toMatchTextContent': function(util, customEqualityTesters) {
    return {
      'compare': function(actual, text) {
        if (!isElement(actual)) {
          throw `${actual} is not a DOM element`;
        }
        text = text.trim();
        let actualText = actual.textContent.trim();
        if (actualText === text) {
          return {
            // craft the message for the negated version
            message: `Expected ${actualText} to not equal ${text} `+
                     `(ignoring whitespace)`,
            pass: true,
          };
        }
        return {
          message: `Expected ${actualText} to equal ${text} `+
                   `(ignoring whitespace)`,
          pass: false,
        };
      },
    };
  },
};

function isElement(ele) {
  //https://stackoverflow.com/a/36894871
  return ele instanceof Element || ele instanceof HTMLDocument;
}

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
    machine_provider_template: 'https://example.com/leases/%s',
  }));


  fetchMock.get('/_ah/api/swarming/v1/server/permissions',
                requireLogin(permissions));
}

export function requireLogin(logged_in, delay=100) {
  const original_items = logged_in.items && logged_in.items.slice();
  return function(url, opts) {
    if (opts && opts.headers && opts.headers.authorization) {
      return new Promise((resolve) => {
        setTimeout(resolve, delay);
      }).then(() => {
        if (logged_in.items instanceof Array) {
          // pretend there are two pages
          if (!logged_in.cursor) {
            // first page
            logged_in.cursor = 'fake_cursor12345';
            logged_in.items = original_items.slice(0, original_items.length/2);
          } else {
            // second page
            logged_in.cursor = undefined;
            logged_in.items = original_items.slice(original_items.length/2);
          }
        }
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
// Copyright 2017 The LUCI Authors. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

var lookup = (function() {
'use strict';

var exports = {};


// A defer with the currently executing lookup operation.
//
// Used internally inside 'lookup' to ensure UI follows only the last initiated
// lookup operation.
var currentLookupOp = null;


// Extracts a string to lookup from the current page URL.
var principalFromURL = function() {
  return common.getQueryParameter('p');
};


// Returns root URL to this page with '?p=...' filled in.
var thisPageURL = function(principal) {
  return common.getLookupURL(principal);
};


// Sets the initial history state and hooks up to onpopstate callback.
var initializeHistory = function(cb) {
  var p = principalFromURL();
  window.history.replaceState({'principal': p}, null, thisPageURL(p));

  window.onpopstate = function(event) {
    var s = event.state;
    if (s && s.hasOwnProperty('principal')) {
      cb(s.principal);
    }
  };
};


// Updates the history state on lookup request, to put '&p=...' there.
var updateHistory = function(principal) {
  if (principal != principalFromURL()) {
    window.history.pushState(
        {'principal': principal}, null, thisPageURL(principal));
  }
};


// lookup initiates a lookup request.
//
// Fills in the lookup edit box and updates browser history to match the given
// value.
var lookup = function(principal) {
  $('#lookup-input').val(principal);
  updateHistory(principal);

  // Reset the state before starting the lookup.
  resetUIState();
  currentLookupOp = null;

  if (!principal) {
    return;
  }

  // Normalize the principal to the form the API expects. As everywhere in the
  // UI, we assume 'user:' prefix is implied in emails and globs. In addition,
  // emails all have '@' symbol and (unlike external groups such as google/a@b)
  // don't have '/', and globs all have '*' symbol. Everything else is assumed
  // to be a group.
  var isEmail = principal.indexOf('@') != -1 && principal.indexOf('/') == -1;
  var isGlob = principal.indexOf('*') != -1;
  if ((isEmail || isGlob) && principal.indexOf(':') == -1) {
    principal = 'user:' + principal;
  }

  // Show in the UI that we are searching.
  setBusyIndicator(true);

  // Ask the backend to do the lookup for us. Ignore the result if some other
  // lookup has been started while we were waiting.
  currentLookupOp = api.fetchRelevantSubgraph(principal);
  currentLookupOp.then(function(response) {
    if (this === currentLookupOp) {
      setBusyIndicator(false);
      currentLookupOp = null;
      setLookupResults(principal, response.data.subgraph);
    }
  }, function(error) {
    if (this === currentLookupOp) {
      setBusyIndicator(false);
      currentLookupOp = null;
      setErrorText(error.text);
    }
  });
};


// Resets the state of the lookup UI to the default one.
var resetUIState = function() {
  setBusyIndicator(false);
  setErrorText('');
  setLookupResults('', null);
};


// Displays or hides "we are searching now" indicator.
var setBusyIndicator = function(isBusy) {
  var $indicator = $('#lookup-busy-indicator');
  if (isBusy) {
    $indicator.show();
  } else {
    $indicator.hide();
  }
};


// Displays (if text is not empty) or hides (if it is empty) the error box.
var setErrorText = function(text) {
  var $box = $('#lookup-error-box');
  $('#lookup-error-message', $box).text(text);
  if (text) {
    $box.show();
  } else {
    $box.hide();
  }
};


// Called when the API replies with the results to render them.
var setLookupResults = function(principal, subgraph) {
  var $box = $('#lookup-results-box');
  var $content = $('#lookup-results');
  if (!subgraph) {
    $box.hide();
    $content.empty();
  } else {
    var vars = interpretLookupResults(principal, subgraph);
    $content.html(common.render('lookup-results-template', vars));
    $box.show();
  }
};


// Takes subgraph returned by API and produces an environment for the template.
//
// See 'lookup-results-template' template in lookup.html to see how the vars
// are used.
var interpretLookupResults = function(principal, subgraph) {
  // TODO(vadimsh): Implement.
  return {
    'text': JSON.stringify(subgraph, null, 2)
  };
};


// Called when HTML body of a page is loaded.
exports.onContentLoaded = function() {
  // Setup a reaction to clicking Enter while in the edit box.
  $('#lookup-form').submit(function(event) {
    lookup($('#lookup-input').val());
    return false;
  });
  // Setup a reaction to the back button.
  initializeHistory(lookup);
  // Do the lookup of a principal provided through URL (if any).
  lookup(principalFromURL());
  // Show the UI before the lookup has finished, we have the busy indicator.
  common.presentContent();
};


return exports;
}());

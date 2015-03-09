// Copyright 2014 The Swarming Authors. All rights reserved.
// Use of this source code is governed by the Apache v2.0 license that can be
// found in the LICENSE file.

var common = (function() {
var exports = {};
var templatesCache = {};


// Simple string formatter: String.format('{0} {1}', 'a', 'b') -> 'a b'.
// Taken from http://stackoverflow.com/a/4673436
String.format = function(format) {
  var args = Array.prototype.slice.call(arguments, 1);
  var sprintfRegex = /\{(\d+)\}/g;
  var sprintf = function(match, number) {
    return number in args ? args[number] : match;
  };
  return format.replace(sprintfRegex, sprintf);
};


// Converts UTC timestamp (in microseconds) to a readable string in local TZ.
exports.utcTimestampToString = function(utc) {
  return (new Date(Number(utc / 1000.0))).toLocaleString();
};


// Fetches handlebars template code from #<templateId> element and renders it.
// Returns rendered string.
exports.render = function(templateId, context) {
  var template = templatesCache[templateId];
  if (!template) {
    var source = $('#' + templateId).html();
    template = Handlebars.compile(source);
    templatesCache[templateId] = template;
  }
  return template(context);
};


// Returns chunk of HTML code with form alert mark up.
// Args:
//   type: 'success' or 'error'.
//   title: title of the message, will be in bold.
//   message: body of the message.
exports.getAlertBoxHtml = function(type, title, message) {
  var cls = (type == 'success') ? 'alert-success' : 'alert-danger';
  return exports.render(
      'alert-box-template', {cls: cls, title: title, message: message});
};


// Disables or enabled input controls in an element.
exports.setInteractionDisabled = function($element, disabled) {
  $('button, input, textarea', $element).attr('disabled', disabled);
};


// Called during initial page load to show contents of a page once
// Javascript part complete building it.
exports.presentContent = function() {
  $('#content-box').show();
  $('#error-box').hide();
};


// Fatal error happened during loading. Show it instead of a content.
exports.presentError = function(errorText) {
  $('#error-box #error-message').text(errorText);
  $('#error-box').show();
  $('#content-box').hide();
};


// Double checks with user and redirects to logout url.
exports.logout = function() {
  if (confirm('You\'ll be signed out from ALL your google accounts.')) {
    window.location = config.logout_url;
  }
};


// Shows confirmation modal dialog. Returns deferred.
exports.confirm = function(message) {
  var defer = $.Deferred();
  if (window.confirm(message))
    defer.resolve();
  else
    defer.reject();
  return defer.promise();
};


// Used in setAnchor and onAnchorChange to filter out unneeded event.
var knownAnchor;


// Returns #anchor part of the current location.
exports.getAnchor = function() {
  return window.location.hash.substring(1);
};


// Changes #anchor part of the current location. Calling this method does not
// trigger 'onAnchorChange' event.
exports.setAnchor = function(a) {
  knownAnchor = a;
  window.location.hash = '#' + a;
};


// Sets a callback to watch for changes to #anchor part of the location.
exports.onAnchorChange = function(cb) {
  window.onhashchange = function() {
    var a = exports.getAnchor();
    if (a != knownAnchor) {
      knownAnchor = a;
      cb();
    }
  };
};


// Wrapper around 'Im busy' UI indicator.
var ProgressSpinner = function() {
  this.$element = $('#progress-spinner');
  this.counter = 0;
};


// Shows progress indicator.
ProgressSpinner.prototype.show = function() {
  this.counter += 1;
  if (this.counter == 1) {
    this.$element.removeClass('not-spinning').addClass('spinning');
  }
};


// Hides progress indicator.
ProgressSpinner.prototype.hide = function() {
  if (this.counter != 0) {
    this.counter -= 1;
    if (this.counter == 0) {
      this.$element.removeClass('spinning').addClass('not-spinning');
    }
  }
};


// Configure state common for all pages.
exports.onContentLoaded = function() {
  // Configure form validation plugin to work with bootstrap styles.
  // See http://stackoverflow.com/a/18754780
  $.validator.setDefaults({
    highlight: function(element) {
      $(element).closest('.form-group').addClass('has-error');
    },
    unhighlight: function(element) {
      $(element).closest('.form-group').removeClass('has-error');
    },
    errorElement: 'span',
    errorClass: 'help-block',
    errorPlacement: function(error, element) {
      if(element.parent('.input-group').length) {
        error.insertAfter(element.parent());
      } else {
        error.insertAfter(element);
      }
    }
  });

  // Install 'Ajax is in progress' indicator.
  var progressSpinner = new ProgressSpinner();
  $(document).ajaxStart(function() {
    progressSpinner.show();
  });
  $(document).ajaxStop(function() {
    progressSpinner.hide();
  });
};


return exports;
}());

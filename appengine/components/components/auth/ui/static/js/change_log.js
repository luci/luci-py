// Copyright 2015 The Swarming Authors. All rights reserved.
// Use of this source code is governed by the Apache v2.0 license that can be
// found in the LICENSE file.

var change_log = (function() {
'use strict';

var exports = {};


// Called when HTML body of a page is loaded.
exports.onContentLoaded = function() {
  // Just show current information about the group for now.
  api.groupRead(common.getQueryParameter('group')).then(function(response) {
    var group = response.data.group;
    group.created_by = common.stripPrefix('user', group.created_by);
    group.created_ts = common.utcTimestampToString(group.created_ts);
    group.modified_by = common.stripPrefix('user', group.modified_by);
    group.modified_ts = common.utcTimestampToString(group.modified_ts);
    $('#group-info').append($(common.render('group-info-template', group)));
    common.presentContent();
  }, function(error) {
    common.presentError(error.text);
  });
};


return exports;
}());

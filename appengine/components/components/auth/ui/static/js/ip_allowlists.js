// Copyright 2014 The LUCI Authors. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

var ip_allowlists = (function() {
'use strict';

var exports = {};


// Multiline string with subnets -> list of subnet strings.
var splitSubnetsList = function(subnets) {
  var mapper = function(str) { return str.trim(); };
  var filter = function(str) { return str !== ''; };
  return _.filter(_.map(subnets.split('\n'), mapper), filter);
};


////////////////////////////////////////////////////////////////////////////////
// Selector is a combo box with IP allowlist names and "Create new" item.


var Selector = function($element, readonly) {
  this.$element = $element;
  this.readonly = readonly;
  this.onCreateAllowlist = null;
  this.onAllowlistSelected = null;

  var that = this;
  $element.change(function() {
    that.onSelectionChanged();
  });
};


// Rebuilds the list.
Selector.prototype.populate = function(allowlists, selection) {
  this.$element.empty();

  var that = this;
  var selected = null;
  var addToSelector = function(name, data) {
    var option = $(document.createElement('option'));
    option.text(name);
    option.data('selector-data', data);
    that.$element.append(option);
    if (selected === null || name == selection) {
      selected = option;
    }
  };

  // All allowlists.
  _.each(allowlists, function(allowlist) {
    addToSelector(allowlist.name, allowlist);
  });

  // Separator and "New list" option.
  if (!this.readonly) {
    addToSelector('----------------------------', 'SEPARATOR');
    addToSelector('Create new IP allowlist', 'CREATE');
  } else {
    // Empty list looks ugly, put something in there.
    if (selected === null) {
      addToSelector('No IP allowlists', 'SEPARATOR');
    }
  }

  // Make the selection.
  selected.attr('selected', 'selected');
  this.onSelectionChanged();
};


// Called whenever selected item in combo box changes.
Selector.prototype.onSelectionChanged = function() {
  var selectedOption = $('option:selected', this.$element);
  var selectedData = selectedOption.data('selector-data');
  if (selectedData === 'SEPARATOR') {
    if (this.onAllowlistSelected !== null) {
      this.onAllowlistSelected(null);
    }
  } else if (selectedData === 'CREATE') {
    if (this.onAllowlistSelected !== null) {
      this.onAllowlistSelected(null);
    }
    if (this.onCreateAllowlist !== null) {
      this.onCreateAllowlist();
    }
  } else {
    if (this.onAllowlistSelected !== null) {
      this.onAllowlistSelected(selectedData);
    }
  }
};


////////////////////////////////////////////////////////////////////////////////
// "Create new IP allowlist" modal dialog.


var NewAllowlistDialog = function($element) {
  this.$element = $element;
  this.$alerts = $('#alerts-box', $element);

  this.onCreateAllowlist = null;

  var that = this;
  $('#create-btn', $element).on('click', function() {
    that.onCreateClicked();
  });
};


// Cleans previous values and alerts, presents the dialog.
NewAllowlistDialog.prototype.show = function() {
  $('input[name="name"]', this.$element).val('');
  $('input[name="description"]', this.$element).val('');
  $('textarea[name="subnets"]', this.$element).val('');
  this.$alerts.empty();
  this.$element.modal('show');
};


// Cleans alerts, hides the dialog.
NewAllowlistDialog.prototype.hide = function() {
  this.$alerts.empty();
  this.$element.modal('hide');
};


// Displays error message in the dialog.
NewAllowlistDialog.prototype.showError = function(text) {
  this.$alerts.html(common.getAlertBoxHtml('error', 'Oh snap!', text));
};


// Called when "Create" button is clicked. Invokes onCreateAllowlist callback.
NewAllowlistDialog.prototype.onCreateClicked = function() {
  if (this.onCreateAllowlist === null) {
    return;
  }

  var name = $('input[name="name"]', this.$element).val();
  var desc = $('input[name="description"]', this.$element).val();
  var list = $('textarea[name="subnets"]', this.$element).val();

  this.onCreateAllowlist({
    name: name,
    description: desc,
    subnets: splitSubnetsList(list)
  });
};


////////////////////////////////////////////////////////////////////////////////
// The panel with information about some selected IP allowlist.


var AllowlistPane = function($element) {
  this.$element = $element;
  this.$alerts = $('#alerts-box', $element);

  this.ipAllowlist = null;
  this.lastModified = null;

  this.onUpdateAllowlist = null;
  this.onDeleteAllowlist = null;

  var that = this;
  $('#update-btn', $element).on('click', function() { that.onUpdateClick(); });
  $('#delete-btn', $element).on('click', function() { that.onDeleteClick(); });
};


// Shows the pane.
AllowlistPane.prototype.show = function() {
  this.$element.show();
};


// Hides the pane.
AllowlistPane.prototype.hide = function() {
  this.$element.hide();
};


// Displays error message in the pane.
AllowlistPane.prototype.showError = function(text) {
  this.$alerts.html(common.getAlertBoxHtml('error', 'Oh snap!', text));
};


// Displays success message in the pane.
AllowlistPane.prototype.showSuccess = function(text) {
  this.$alerts.html(common.getAlertBoxHtml('success', 'Done!', text));
};


// Fills in the form with details about some IP allowlist.
AllowlistPane.prototype.populate = function(ipAllowlist) {
  // TODO(vadimsh): Convert ipAllowlist.modified_ts to a value compatible with
  // 'If-Unmodified-Since' header and put it into this.lastModified.
  this.ipAllowlist = ipAllowlist;
  $('input[name="description"]', this.$element).val(ipAllowlist.description);
  $('textarea[name="subnets"]', this.$element).val(
      (ipAllowlist.subnets || []).join('\n'));
  this.$alerts.empty();
};


// Called whenever 'Update' button is clicked.
AllowlistPane.prototype.onUpdateClick = function() {
  if (!this.onUpdateAllowlist) {
    return;
  }

  var desc = $('input[name="description"]', this.$element).val();
  var list = $('textarea[name="subnets"]', this.$element).val();

  var updatedIpAllowlist = _.clone(this.ipAllowlist);
  updatedIpAllowlist.description = desc;
  updatedIpAllowlist.subnets = splitSubnetsList(list);

  this.onUpdateAllowlist(updatedIpAllowlist, this.lastModified);
};


// Called whenever 'Delete' button is clicked.
AllowlistPane.prototype.onDeleteClick = function() {
  if (this.onDeleteAllowlist) {
    this.onDeleteAllowlist(this.ipAllowlist.name, this.lastModified);
  }
};


////////////////////////////////////////////////////////////////////////////////
// Top level logic.


// Fetches all IP allowlists, adds them to the selector, selects some.
var reloadAllowlists = function(selector, selection) {
  var done = $.Deferred();
  api.ipWhitelists().then(function(response) {
    selector.populate(response.data.ip_whitelists, selection);
    common.presentContent();
    done.resolve(response);
  }, function(error) {
    common.presentError(error.text);
    done.reject(error);
  });
  return done.promise();
};


// Called when HTML body of a page is loaded.
exports.onContentLoaded = function() {
  var readonly = config.auth_service_config_locked || !config.is_admin;
  var selector = new Selector($('#ip-allowlists-selector'), readonly);
  var newListDialog = new NewAllowlistDialog($('#create-ip-allowlist'));
  var allowlistPane = new AllowlistPane($('#selected-ip-allowlist'));

  // Enable\disable UI interactions on the page.
  var setInteractionDisabled = function(disabled) {
    common.setInteractionDisabled(selector.$element, disabled);
    common.setInteractionDisabled(newListDialog.$element, disabled);
    common.setInteractionDisabled(allowlistPane.$element, disabled);
  };

  // Disable UI, wait for defer, reload allowlists, enable UI.
  var wrapDefer = function(defer, selection) {
    var done = $.Deferred();
    setInteractionDisabled(true);
    defer.then(function(response) {
      reloadAllowlists(selector, selection).done(function() {
        setInteractionDisabled(false);
        done.resolve(response);
      });
    }, function(error) {
      setInteractionDisabled(false);
      done.reject(error);
    });
    return done.promise();
  };

  // Show the dialog when 'Create IP allowlist' item is selected.
  selector.onCreateAllowlist = function() {
    newListDialog.show();
  };

  // Update allowlistPane with selected allowlist on a selection changes.
  selector.onAllowlistSelected = function(ipAllowlist) {
    if (ipAllowlist === null) {
      allowlistPane.hide();
    } else {
      allowlistPane.populate(ipAllowlist);
      allowlistPane.show();
    }
  };

  // Wire dialog's "Create" button.
  newListDialog.onCreateAllowlist = function(ipAllowlist) {
    // Some minimal client side validation, otherwise handlers nay return 404.
    if (!ipAllowlist.name.match(/^[0-9a-zA-Z_\-\+\.\ ]{2,200}$/)) {
      newListDialog.showError('Invalid IP allowlist name.');
      return;
    }
    var defer = wrapDefer(api.ipWhitelistCreate(ipAllowlist), ipAllowlist.name);
    defer.then(function(response) {
      newListDialog.hide();
      allowlistPane.showSuccess('Created.');
    }, function(error) {
      newListDialog.showError(error.text || 'Unknown error');
    });
  };

  // Wire 'Delete allowlist' button.
  allowlistPane.onDeleteAllowlist = function(name, lastModified) {
    var defer = wrapDefer(api.ipWhitelistDelete(name, lastModified), null);
    defer.fail(function(error) {
      allowlistPane.showError(error.text || 'Unknown error');
    });
  };

  // Wire 'Update allowlist' button.
  allowlistPane.onUpdateAllowlist = function(ipAllowlist, lastModified) {
    var defer = wrapDefer(
        api.ipWhitelistUpdate(ipAllowlist, lastModified), ipAllowlist.name);
    defer.then(function(response) {
      allowlistPane.showSuccess('Updated.');
    }, function(error) {
      allowlistPane.showError(error.text || 'Unknown error');
    });
  };

  // Initial data fetch.
  allowlistPane.hide();
  reloadAllowlists(selector, null);

  // Enable XSRF token auto-updater.
  api.setXSRFTokenAutoupdate(true);
};


return exports;
}());

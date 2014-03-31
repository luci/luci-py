// Copyright 2014 The Swarming Authors. All rights reserved.
// Use of this source code is governed by the Apache v2.0 license that can be
// found in the LICENSE file.

var groups = (function() {
var exports = {};


////////////////////////////////////////////////////////////////////////////////
// Group chooser UI element: list of groups + 'Create new group' button.


var GroupChooser = function($element) {
  // Root jquery DOM element.
  this.$element = $element;
  // Currently known list of groups as shown in UI.
  this.groupList = [];
  // Same list, but as a dict: group name -> group object.
  this.groupMap = {};
  // If true, selection won't change on clicks in UI.
  this.interactionDisabled = false;
};


// Loads list of groups from a server. Updates group chooser UI.
// Returns deferred.
GroupChooser.prototype.refetchGroups = function() {
  var defer = api.groups();
  var self = this;
  defer.then(function(response) {
    self.setGroupList(response.data.groups);
  });
  return defer;
};


// Updates DOM of a group chooser.
GroupChooser.prototype.setGroupList = function(groups) {
  var self = this;

  // Remember new list (sorted by name).
  self.groupList = _.sortBy(groups, 'name');
  self.groupMap = {};
  _.each(groups, function(group) {
    self.groupMap[group.name] = group;
  });

  // Helper function to add children to DOM.
  var addElement = function(markup, groupName) {
    var item = $(markup);
    item.addClass('chooser-element');
    item.data('group-name', groupName);
    item.appendTo(self.$element);
  };

  // Rebuild DOM: list of groups + 'Create new group' button.
  self.$element.addClass('list-group');
  self.$element.empty();
  _.each(groups, function(group) {
    addElement(common.render('group-chooser-item-template', group), group.name);
  });
  addElement(common.render('group-chooser-button-template'), null);

  // Setup click event handlers. Clicks change selection.
  $('.chooser-element', self.$element).click(function() {
    if (!self.interactionDisabled) {
      self.setSelection($(this).data('group-name'));
    }
    return false;
  });
};


// Returns name of the selected group or null if 'Create new group' is selected.
// Returns 'undefined' if nothing is selected.
GroupChooser.prototype.getSelection = function() {
  var active = $('.chooser-element.active', self.$element);
  // 'group-name' attribute of 'Create new group' button is 'null'.
  return active.length ? active.data('group-name') : undefined;
};


// Highlights a group as chosen in group list.
// If |name| is null, then highlights 'Create new group' button.
// Also triggers 'selectionChanged' event.
GroupChooser.prototype.setSelection = function(name) {
  // Nothing to do?
  if (this.getSelection() === name) {
    return;
  }
  var selectionMade = false;
  $('.chooser-element', self.$element).each(function() {
    if ($(this).data('group-name') === name) {
      $(this).addClass('active');
      selectionMade = true;
    } else {
      $(this).removeClass('active');
    }
  });
  if (selectionMade) {
    this.$element.triggerHandler('selectionChanged', {group: name});
  }
};


// Selects top element.
GroupChooser.prototype.selectDefault = function() {
  var elements = $('.chooser-element', self.$element);
  if (elements.length) {
    this.setSelection(elements.first().data('group-name'));
  }
};


// Registers new event listener that is called whenever selection changes.
GroupChooser.prototype.onSelectionChanged = function(listener) {
  this.$element.on('selectionChanged', function(event, selection) {
    listener(selection.group);
  });
};


// Disables an ability to change selection.
GroupChooser.prototype.setInteractionDisabled = function(disabled) {
  this.interactionDisabled = disabled;
};


////////////////////////////////////////////////////////////////////////////////
// Main content frame: a parent for forms to create a group or edit an existing.


var ContentFrame = function($element) {
  this.$element = $element;
  this.content = null;
  this.loading = null;
};


// Registers new event listener that is called when content is loaded and show.
ContentFrame.prototype.onContentShown = function(listener) {
  this.$element.on('contentShown', function() {
    listener();
  });
};


// Replaces frame's content with another one.
// |content| is an instance of GroupForm class.
ContentFrame.prototype.setContent = function(content) {
  if (this.content) {
    this.content.hide();
    this.content = null;
  }
  this.$element.empty();
  this.content = content;
  this.loading = null;
  if (this.content) {
    this.content.show(this.$element);
    this.$element.triggerHandler('contentShown');
  }
};


// Loads new content asynchronously using content.load(...) call.
// |content| is an instance of GroupForm class.
ContentFrame.prototype.loadContent = function(content) {
  var self = this;
  if (self.content) {
    self.content.setInteractionDisabled(true);
  }
  self.loading = content;
  content.load().then(function() {
    // Switch content only if another 'loadContent' wasn't called before.
    if (self.loading == content) {
      self.setContent(content);
    }
  }, function(error) {
    // Still loading same content?
    if (self.loading == content) {
      self.setContent(null);
      self.$element.append($(common.render('frame-error-pane', error)));
    }
  });
};


////////////////////////////////////////////////////////////////////////////////
// Common code for 'New group' and 'Edit group' forms.


var GroupForm = function($element) {
  this.$element = $element;
  this.visible = false;
};


// Presents this form in $parent.
GroupForm.prototype.show = function($parent) {
  this.visible = true;
  this.$element.appendTo($parent);
};


// Hides this form.
GroupForm.prototype.hide = function() {
  this.visible = false;
  this.$element.detach();
};


// Load contents of this from the server.
// Returns deferred.
GroupForm.prototype.load = function() {
  // Subclasses implement this. Base class just returns resolved deferred.
  var defer = $.Deferred();
  defer.resolve()
  return defer;
};


// Disables or enables controls on the form.
GroupForm.prototype.setInteractionDisabled = function(disabled) {
  $('button', this.$element).attr('disabled', disabled);
};


// Shows a message on a form. |type| can be 'success' or 'error'.
GroupForm.prototype.showMessage = function(type, title, message) {
  $('#alerts', this.$element).html(
      common.getAlertBoxHtml(type, title, message));
};


// Hides a message previously shown with 'showMessage'.
GroupForm.prototype.hideMessage = function() {
  $('#alerts', this.$element).empty();
};


////////////////////////////////////////////////////////////////////////////////
// Form to view\edit existing group.


EditGroupForm = function(groupName) {
  // Call parent constructor.
  GroupForm.call(this, null);
  // Name of the group this form operates on.
  this.groupName = groupName;
  // Last-Modified header of content (once loaded).
  this.lastModified = null;
  // Called when 'Delete group' action is invoked.
  this.onDeleteGroup = null;
  // Called when group form is submitted.
  this.onUpdateGroup = null;
};


// Inherit from GroupForm.
EditGroupForm.prototype = Object.create(GroupForm.prototype);


// Loads contents of this from the server.
EditGroupForm.prototype.load = function() {
  var self = this;
  var defer = api.groupRead(this.groupName);
  defer.then(function(response) {
    self.buildForm(response.data.group, response.headers['Last-Modified']);
  });
  return defer;
};


// Builds DOM element with this form given group object.
EditGroupForm.prototype.buildForm = function(group, lastModified) {
  // Convert fields to text.
  group = _.clone(group);
  group.created_ts = common.utcTimestampToString(group.created_ts);
  group.members = (group.members || []).join('\n') + '\n';
  group.globs = (group.globs || []).join('\n') + '\n';
  group.nested = (group.nested || []).join('\n') + '\n';

  // Build the actual DOM element.
  this.$element = $(common.render('edit-group-form-template', group));
  this.lastModified = lastModified;

  // 'Delete' button handler. Asks confirmation and calls 'onDeleteGroup'.
  var self = this;
  $('#delete-btn', this.$element).click(function() {
    common.confirm('Delete this group?').done(function() {
      self.onDeleteGroup(self.groupName, self.lastModified);
    });
  });

  // Submit handler.
  $('form', this.$element).submit(function() {
    // TODO(vadimsh): Validate the form, extract group information from it.
    var group = {};
    self.onUpdateGroup(group, self.lastModified);
    return false;
  });
};


////////////////////////////////////////////////////////////////////////////////
// Main entry point, sets up all high-level UI logic.


// Wrapper around a REST API call that originated from some form.
// Locks UI while call is running, refreshes a list of groups once it completes.
var waitForResult = function(defer, groupChooser, form) {
  // Deferred triggered when update is finished (successfully or not). Return
  // values of this function.
  var done = $.Deferred();

  // Lock UI while running the request, unlock once it finishes.
  groupChooser.setInteractionDisabled(true);
  form.setInteractionDisabled(true);
  done.always(function() {
    groupChooser.setInteractionDisabled(false);
    form.setInteractionDisabled(false);
  });

  // Hide previous error message (if any).
  form.hideMessage();

  // Wait for request to finish, refetch the list of groups and trigger |done|.
  defer.then(function(response) {
    // Call succeeded: refetch the list of groups and return the result.
    groupChooser.refetchGroups().then(function() {
      done.resolve(response);
    }, function(error) {
      // Show page-wide error message, since without the list of groups the page
      // is useless.
      common.presentError(error.text);
      done.reject(error);
    });
  }, function(error) {
    // Show error message on the form, since it's local error with the request.
    form.showMessage('error', 'Oh snap!', error.text);
    done.reject(error);
  });

  return done.promise();
};


exports.onContentLoaded = function() {
  // Setup global UI elements.
  var groupChooser = new GroupChooser($('#group-chooser'));
  var mainFrame = new ContentFrame($('#main-content-pane'));

  // Called to setup 'Create new group' flow.
  var startNewGroupFlow = function() {
    // TODO(vadimsh): Implement.
    mainFrame.loadContent(new GroupForm($('<div>New group</div>')));
  };

  // Called to setup 'Edit the group' flow (including deletion of a group).
  var startEditGroupFlow = function(groupName) {
    var form = new EditGroupForm(groupName);

    // Called when 'Delete' button is clicked.
    form.onDeleteGroup = function(groupName, lastModified) {
      var request = api.groupDelete(groupName, lastModified);
      waitForResult(request, groupChooser, form).done(function() {
        groupChooser.selectDefault();
      });
    };

    // Called when 'Update' button is clicked.
    form.onUpdateGroup = function(groupObj, lastModified) {
      // TODO(vadimsh): Implement group updates.
      alert('Not implemented');
    };

    mainFrame.loadContent(form);
  };

  // Attach event handlers.
  groupChooser.onSelectionChanged(function(selection) {
    if (selection === null) {
      startNewGroupFlow();
    } else {
      startEditGroupFlow(selection);
    }
  });

  // Present the page only when main content pane is loaded.
  mainFrame.onContentShown(common.presentContent);

  // Load and show data.
  groupChooser.refetchGroups().then(function() {
    groupChooser.selectDefault();
  }, function(error) {
    common.presentError(error.text);
  });

  // Enable XSRF token auto-updater.
  api.setXSRFTokenAutoupdate(true);
};

return exports;

}());

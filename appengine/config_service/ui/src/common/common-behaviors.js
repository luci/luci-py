/**
 * To add legacy behaviors to class-style element, use the mixinBehaviors
 * function:
 *     class XClass extends mixinBehaviors([CommonBehavior], PolymerElement)
 *
 * CommonBehavior contains shared methods to ease templating, such
 * _isEmpty() and _not(), as well as general utility methods such as
 * _formatDate and _getTimestamp.
 *
 * @license
 * Copyright 2020 The LUCI Authors. All rights reserved.
 * Use of this source code is governed under the Apache License, Version 2.0
 * that can be found in the LICENSE file.
 */
export const CommonBehavior = {
  _formatDate: function(timestamp) {
    if (timestamp === null) return "Not Found";

    var date = new Date(timestamp / 1000);
    var seconds = Math.floor((new Date() - date) / 1000);

    var interval = Math.floor(seconds / 31536000);
    if (interval > 1) {
      return interval + " years ago";
    }
    interval = Math.floor(seconds / 2592000);
    if (interval > 1) {
      return interval + " months ago";
    }
    interval = Math.floor(seconds / 86400);
    if (interval > 1) {
      return interval + " days ago";
    }
    interval = Math.floor(seconds / 3600);
    if (interval > 1) {
      return interval + " hours ago";
    }
    interval = Math.floor(seconds / 60);
    if (interval > 1) {
      return interval + " minutes ago";
    }
    return Math.floor(seconds) + " seconds ago";
  },

  _formatRevision: function(revision) {
    if (revision === "Not Found") return revision;
    return revision.substring(0, 7);
  },

  _frontPageIsActive: function() {
    if (this.frontPageIsActive === false) {
      this.isLoading = true;
      if (!this.initialized) {
        document.addEventListener('fetch-configs', function() {
          this.$.requestConfigs.generateRequest();
        }.bind(this));
      } else {
        this.$.requestConfigs.generateRequest();
      }
    }
  },

  _getExactTime: function(timestamp) {
    if (timestamp === null) return "Not Found";
    var date = new Date(timestamp / 1000);
    return date.toString();
  },

  _getTimestamp: function(lastImportAttempt, revision) {
    if (lastImportAttempt && lastImportAttempt.success) {
      return lastImportAttempt.revision.timestamp;
    } else if (revision && revision.timestamp) {
      return revision.timestamp;
    } else {
      return null;
    }
  },

  _getRevision: function(revision) {
    if (revision) {
      return revision.id;
    } else {
      return "Not Found"
    }
  },

  _isEmpty: function(list) {
    return list.length === 0;
  },

  _not: function(a) {
    return !a;
  }
};

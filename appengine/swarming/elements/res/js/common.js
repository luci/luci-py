// Copyright 2016 The LUCI Authors. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

this.swarming = this.swarming || function() {

  var swarming = {};

  swarming.stableSort = function(arr, comp) {
    if (!arr || !comp) {
      console.log("missing arguments to stableSort", arr, comp);
      return;
    }
    // We can guarantee a potential non-stable sort (like V8's
    // Array.prototype.sort()) to be stable by first storing the index in the
    // original sorting and using that if the original compare was 0.
    arr.forEach(function(e, i){
      if (e !== undefined && e !== null) {
        e.__sortIdx = i;
      }
    });

    arr.sort(function(a, b){
      // undefined and null elements always go last.
      if (a === undefined || a === null) {
        if (b === undefined || b === null) {
          return 0;
        }
        return 1;
      }
      if (b === undefined || b === null) {
        return -1;
      }
      var c = comp(a, b);
      if (c === 0) {
        return a.__sortIdx - b.__sortIdx;
      }
      return c;
    });
  }

  // naturalCompare tries to use natural sorting (e.g. sort ints by value).
  swarming.naturalCompare = function(a, b) {
    // Try numeric, aka "natural" sort and use it if ns is not NaN.
    // Javascript will try to corece these to numbers or return NaN.
    var ns = a - b;
    if (!isNaN(ns)) {
      return ns;
    }
    return String(a).localeCompare(b);
  };

  // postWithToast makes a post request and updates the error-toast
  // element with the response, regardless of failure.  See error-toast.html
  // for more information.
  swarming.postWithToast = function(url, msg, auth_headers) {
    // Keep toast displayed until we hear back from the request.
    sk.errorMessage(msg, 0);

    sk.request("POST", url, undefined, auth_headers).then(function(response) {
      sk.errorMessage("Request sent.  Response: "+response, 3000);
    }).catch(function(reason) {
      console.log("Request failed", reason);
      sk.errorMessage("Request failed.  Reason: "+reason, 5000);
    });
  }

  return swarming;
}();
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
    return a.localeCompare(b);
  };


  var TIME_DELTAS = [
    { units: "w", delta: 7*24*60*60 },
    { units: "d", delta:   24*60*60 },
    { units: "h", delta:      60*60 },
    { units: "m", delta:         60 },
    { units: "s", delta:          1 },
  ];

  /**
   * Returns the difference between the specified time and 's' as a string in a
   * human friendly format.
   * If left unspecified, "now" defaults to Date.now()
   * If 's' is a number it is assumed to contain the time in milliseconds
   * otherwise it is assumed to contain a time string.
   *
   * For example, a difference of 123 seconds between 's' and the current time
   * would return "2m".
   */
  swarming.diffDate = function(s, now) {
    var ms = (typeof(s) == "number") ? s : Date.parse(s);
    now = now || Date.now();
    var diff = (ms - now)/1000;
    if (diff < 0) {
      diff = -1.0 * diff;
    }
    return humanize(diff, TIME_DELTAS);
  };

  swarming.KB = 1024;
  swarming.MB = swarming.KB * 1024;
  swarming.GB = swarming.MB * 1024

  var BYTES_DELTAS = [
    { units: " TB", delta: 1024*swarming.GB},
    { units: " GB", delta:      swarming.GB},
    { units: " MB", delta:      swarming.MB},
    { units: " KB", delta:      swarming.KB},
    { units: " B",  delta:                1},
  ];

  swarming.humanBytes = function(b, unit) {
    if (Number.isInteger(unit)) {
      b = b * unit;
    }
    return humanize(b, BYTES_DELTAS);
  }

  function humanize(n, deltas) {
    for (var i=0; i<deltas.length-1; i++) {
      // If n would round to '60s', return '1m' instead.
      var nextDeltaRounded =
          Math.round(n/deltas[i+1].delta)*deltas[i+1].delta;
      if (nextDeltaRounded/deltas[i].delta >= 1) {
        return Math.round(n/deltas[i].delta)+deltas[i].units;
      }
    }
    var i = DELTAS.length-1;
    return Math.round(n/DELTAS[i].delta)+DELTAS[i].units;
  }

  return swarming;
}();
// Copyright 2016 The LUCI Authors. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

// TODO(kjlubick): add tests for this code

this.swarming = this.swarming || {};
this.swarming.alias = this.swarming.alias || (function(){
  var ANDROID_ALIASES = {
    "angler": "Nexus 6p",
    "bullhead": "Nexus 5X",
    "flo": "Nexus 7 (2013)",
    "flounder": "Nexus 9",
    "foster": "NVIDIA Shield",
    "fugu": "Nexus Player",
    "grouper": "Nexus 7 (2012)",
    "hammerhead": "Nexus 5",
    "heroqlteatt": "Galaxy S7",
    "m0": "Galaxy S3",
    "mako": "Nexus 4",
    "manta": "Nexus 10",
    "shamu": "Nexus 6",
    "sprout": "Android One",
  };

  var UNKNOWN = "unknown";

  var GPU_ALIASES = {
    "1002": "AMD",
    "1002:6779": "AMD Radeon HD 6450/7450/8450",
    "1002:6821": "AMD Radeon HD 8870M",
    "1002:683d": "AMD Radeon HD 7770/8760",
    "1002:9830": "AMD Radeon HD 8400",
    "102b":      "Matrox",
    "102b:0522": "Matrox MGA G200e",
    "102b:0532": "Matrox MGA G200eW",
    "102b:0534": "Matrox G200eR2",
    "10de":      "NVIDIA",
    "10de:08a4": "NVIDIA GeForce 320M",
    "10de:08aa": "NVIDIA GeForce 320M",
    "10de:0fe9": "NVIDIA GeForce GT 750M Mac Edition",
    "10de:104a": "NVIDIA GeForce GT 610",
    "10de:11c0": "NVIDIA GeForce GTX 660",
    "10de:1244": "NVIDIA GeForce GTX 550 Ti",
    "10de:1401": "NVIDIA GeForce GTX 960",
    "8086":      "Intel",
    "8086:0412": "Intel Haswell Integrated",
    "8086:041a": "Intel Xeon Integrated",
    "8086:0a2e": "Intel Haswell Integrated",
    "8086:0d26": "Intel Crystal Well Integrated",
    "8086:22b1": "Intel Braswell Integrated",
  }

  // For consistency, all aliases are displayed like:
  // Nexus 5X (bullhead)
  // This regex matches a string like "ALIAS (ORIG)", with ORIG as group 1.
  var ALIAS_REGEXP = /.+ \((.*)\)/;

  var alias = {};

  alias.DIMENSIONS_WITH_ALIASES = ["device_type", "gpu"];

  alias.android = function(dt) {
    return ANDROID_ALIASES[dt] || UNKNOWN;
  };

  // alias.apply tries to alias the string "orig" based on what "type" it is.
  // If type is in DIMENSIONS_WITH_ALIASES, the appropriate alias (e.g. gpu)
  // is automatically applied.  Otherwise, "type" is treated as the alias.
  // If type is known, but there is no matching alias (e.g. for gpu: FOOBAR)
  // the original will be returned.
  alias.apply = function(orig, type) {
    var aliaser = aliasMap[type];
    if (!aliaser) {
      // treat type as the actual alias
      return type + " ("+orig+")";
    }
    var alias = aliaser(orig);
    if (alias !== "unknown") {
      return alias + " ("+orig+")";
    }
    return orig;
  };

  alias.gpu = function(gpu) {
    return GPU_ALIASES[gpu] || UNKNOWN;
  };

  // alias.unapply will return the base dimension/state with its alias removed
  // if it had one.  This is handy for sorting and filtering.
  alias.unapply = function(str) {
    var match = ALIAS_REGEXP.exec(str);
    if (match) {
      return match[1];
    }
    return str;
  };

  var aliasMap = {
    "device_type": alias.android,
    "gpu": alias.gpu,
  }

  return alias;
})();
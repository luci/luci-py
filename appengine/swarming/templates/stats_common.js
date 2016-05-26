{# Copyright 2014 The LUCI Authors. All rights reserved.
   Use of this source code is governed under the Apache License, Version 2.0
   that can be found in the LICENSE file.
#}
// Formats a number into IEC 60027-2 A.2 / ISO 80000.
var BINARY_SUFFIXES = [
  'B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB'
];

function formatToBinaryUnit(val) {
  // Prefer to display 1023 as 0.99KiB.
  for (var n = 0; val >= 1000; n++) {
    val /= 1024;
  }
  // Enforce 2 decimals.
  if (n > 0) {
    val = val.toFixed(2);
  }
  return val + BINARY_SUFFIXES[n];
}

// Formats data in a chart into 1024 based units.
function formatDataColumnToBinaryUnit(data, column) {
  for (var i = 0; i < data.getNumberOfRows(); i++) {
    data.setFormattedValue(
        i, column, formatToBinaryUnit(data.getValue(i, column)));
  }
}

var ISO_SUFFIXES = ['', 'k', 'M', 'G', 'T', 'P', 'E', 'Z'];

function formatToIsoUnit(val) {
  for (var n = 0; val >= 1000; n++) {
    val /= 1000;
  }
  // Enforce 2 decimals.
  if (n > 0) {
    val = val.toFixed(2);
  }
  return val + ISO_SUFFIXES[n];
}

// Formats data in a chart into 1000 based units.
function formatDataColumnToIsoUnit(data, column) {
  for (var i = 0; i < data.getNumberOfRows(); i++) {
    data.setFormattedValue(
        i, column, formatToIsoUnit(data.getValue(i, column)));
  }
}

// Makes sure custom formatting is removed for a specific column.
function resetFormattedDataColumn(data, column) {
  for (var i = 0; i < data.getNumberOfRows(); i++) {
    data.setFormattedValue(i, column, null);
  }
}

// Makes sure ALL custom formatting is removed.
function resetFormattedData(data) {
  for (var i = 0; i < data.getNumberOfColumns(); i++) {
    resetFormattedDataColumn(data, i);
  }
}

// Removes any ticks. It is necessary when the data changes.
function clearCustomTicks(chart) {
  // TODO(maruel): Make this automatic when chart.setDataTable() is used.
  var options = chart.getOptions();
  if (typeof options.vAxis != "undefined") {
    delete options.vAxis.ticks;
  }
  if (typeof options.vAxes != "undefined") {
    for (var i in options.vAxes) {
      delete options.vAxes[i].ticks;
    }
  }
}

// Resets the axis 0 of the chart to binary prefix (1024).
function setAxisTicksToUnitsOnNextDraw(chart, as_binary, axe_index) {
  // TODO(maruel): This code is racy and sometimes does not trigger correctly.
  // It can be reproduced with:
  // 1. Reload
  // 2. click Day
  // 3. click Hour
  // 4. Repeat 2 and 3 until reproduced.
  function callback() {
    google.visualization.events.removeListener(runOnce);
    var ticks = [];
    // Warning: ChartWrapper really wraps the actual Chart, and the proxy fails
    // to expose some methods, like .getChartLayoutInterface(). In this case,
    // the user must call .getChart() to retrieve the underlying object.
    var cli;
    if (typeof chart.getChartLayoutInterface == "undefined") {
      cli = chart.getChart().getChartLayoutInterface();
    } else {
      cli = chart.getChartLayoutInterface();
    }
    var power = 1000;
    var suffixes = ISO_SUFFIXES;
    if (as_binary) {
      power = 1024;
      suffixes = BINARY_SUFFIXES;
    }
    var bb;
    for (var i = 0; bb = cli.getBoundingBox('vAxis#0#gridline#' + i); i++) {
      var val = cli.getVAxisValue(bb.top);
      // The axis value may fall 1/2 way though the pixel height of the
      // gridline, so add in 1/2 the height. This assumes that all axis values
      // will be integers.
      if (val != parseInt(val)) {
        val = cli.getVAxisValue(bb.top + bb.height / 2, axe_index);
      }
      // Converts the auto-selected base-10 values to 2^10 'rounded' values if
      // necessary.
      for (var n = 0; val >= 1000; n++) {
        val /= 1000;
      }
      // Keep 2 decimals. Note that this code assumes the items are all
      // integers. Fix accordingly if needed.
      var formattedVal = val;
      // TODO(maruel): Detect "almost equal".
      if (n > 0 || formattedVal != formattedVal.toFixed(0)) {
        formattedVal = formattedVal.toFixed(2);
      }
      val *= Math.pow(power, n);
      ticks.push({v: val, f: formattedVal + suffixes[n]});
    }
    if (typeof axe_index == "undefined") {
      // It's possible the object vAxis is not defined yet.
      chart.getOptions().vAxis = chart.getOptions().vAxis || {};
      chart.getOptions().vAxis.ticks = ticks;
    } else {
      // If axe_indexes is specified, vAxes must be defined.
      chart.getOptions().vAxes = chart.getOptions().vAxes || [];
      chart.getOptions().vAxes[axe_index] = chart.getOptions(
          ).vAxes[axe_index] || {};
      chart.getOptions().vAxes[axe_index].ticks = ticks;
    }
    // Draw a second time.
    // TODO(maruel): Sadly, this second draw is user visible.
    chart.draw();
  }

  // TODO(maruel): This codes cause a visible redraw, it'd be nice to figure out
  // a way to not cause it.
  var runOnce = google.visualization.events.addListener(
    chart, 'ready', callback);
}

// Sends a single query to feed data to two charts.
function sendQuery(url, redrawCharts) {
  if (current_query != null) {
    current_query.abort();
  }
  current_query = new google.visualization.Query(url);
  current_query.send(
    function(response) {
      if (response.isError()) {
        alert('Error in query: ' + response.getMessage() + ' ' +
            response.getDetailedMessage());
        return;
      }

      redrawCharts(response.getDataTable());
    });
}


// The following functions assume a certain HTML layout, current_resolution is
// defined and google.visualization was loaded.
// TODO(maruel): Better refactor this so this code is also shared with isolate
// server front end.

function get_key_formatter(resolution) {
  if (resolution) {
    if (resolution == 'days') {
      return new google.visualization.DateFormat({pattern: 'yyyy/MM/dd'});
    } else {
      return new google.visualization.DateFormat({pattern: 'MM/dd HH:mm'});
    }
  }
  if (current_resolution == 'days') {
    return new google.visualization.DateFormat({pattern: 'yyyy/MM/dd'});
  } else {
    return new google.visualization.DateFormat({pattern: 'MM/dd HH:mm'});
  }
}

function set_resolution_internal(res) {
  document.getElementById('resolution_days').checked = false;
  document.getElementById('resolution_hours').checked = false;
  document.getElementById('resolution_minutes').checked = false;
  document.getElementById('resolution_' + res).checked = true;
  current_resolution = res;
}

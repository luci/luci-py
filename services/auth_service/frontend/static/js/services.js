// Copyright 2014 The Swarming Authors. All rights reserved.
// Use of this source code is governed by the Apache v2.0 license that can be
// found in the LICENSE file.

var services = (function() {
var exports = {};


var listServices = function() {
  return api.call('GET', '/auth_service/api/v1/services');
};


var generateLink = function(serviсe_app_id) {
  return api.call(
      'POST',
      '/auth_service/api/v1/services/' + serviсe_app_id + '/linking_url');
};


var updateServiceListing = function() {
  var defer = listServices();

  // Show the page when first fetch completes, refetch after X sec.
  defer.always(function() {
    common.presentContent();
    setTimeout(updateServiceListing, 10000);
  });

  defer.then(function(result) {
    var services = [];
    var now = result.data['now'];
    var auth_db_rev = result.data['auth_db_rev']['rev'];
    _.each(result.data['services'], function(service) {
      services.push({
        app_id: service.app_id,
        replica_url: service.replica_url,
        status_label: 'default',
        status_text: 'not implemented'
      });
    });
    $('#services-list').html(
        common.render('services-list-template', {services: services}));
    $('#services-list-alerts').empty();
  }, function(error) {
    $('#services-list-alerts').html(
        common.getAlertBoxHtml('error', 'Oh shap!', error.text));
  });
};


var initAddServiceForm = function() {
  $('#add-service-form').validate({
    submitHandler: function($form) {
      // Disable form UI while the request is in flight.
      common.setInteractionDisabled($form, true);
      // Launch the request.
      var app_id = $('input[name=serviсe_app_id]', $form).val();
      var defer = generateLink(app_id);
      // Enable UI back when it finishes.
      defer.always(function() {
        common.setInteractionDisabled($form, false);
      });
      defer.then(function(result) {
        // On success, show the URL.
        var url = result.data['url'];
        $('#add-service-form-alerts').html(
            common.render('present-link-template', {url: url, app_id: app_id}));
      }, function(error) {
        // On a error, show the error message.
        $('#add-service-form-alerts').html(
            common.getAlertBoxHtml('error', 'Oh shap!', error.text));
      });
      return false;
    },
    rules: {
      'serviсe_app_id': {
        required: true
      }
    },
  });
};


// Called when HTML body of a page is loaded.
exports.onContentLoaded = function() {
  api.setXSRFTokenAutoupdate(true);
  initAddServiceForm();
  updateServiceListing();
};


return exports;
}());

## Authentication management REST API and UI

The REST API is defined in the rest_api.py module.

The UI implemented as a client side JavaScript application that uses REST API.

UI is structured as a set of tabs in a navigation bar, each tab is a separate
HTML page (see https://getbootstrap.com/docs/3.4/components/#navbar) served by
some handler defined in the `ui.py` module.

Each page has a JavaScript module associated with it that implements all UI
logic. The entry point in such modules is the `onContentLoaded` function that
is called once main page body is loaded. A JavaScript module defined in
`<module_name>.js` will look like this:

      var module_name = (function() {
      var exports = {};

      exports.symbol = ....

      return exports;
      }());

Code in `base.html` relies on correspondence of JavaScript file name and name of
a module object it exports.

Each module has access to several global objects (loaded in `base.html`)
provided by third party libraries:

 * `$` - jQuery library, see static/third_party/jquery/.
 * `_` - underscore library, see static/third_party/underscore/.
 * `Handlebars` - handlebars template library, see static/third_party/handlebars/.

Additional global objects are:

 * `common` - a module with utility functions, see `common.js`.
 * `api` - a module with wrappers around auth service REST API, see `api.js`.
 * `config` - an object with page configuration passed from python code,
      see UIHandler class ui.py module.

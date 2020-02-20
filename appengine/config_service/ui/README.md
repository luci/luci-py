# LUCI Config UI

This is a UI for the configuration service.


## Prerequisites

*	Install npm & npx (From npm@5.2.0, npm ships with npx included)
* Install [Google App Engine SDK](https://cloud.google.com/appengine/downloads).

## Deploy

To build the ui for App Engine deployment, you must run:

    make release

You should expect new *node_modules* and *build* directory getting created after running the command. The output of *build/default* folder should have the bundled HTML, CSS and JS files.

Note: release target create a clean build according to *package-lock.json*. Please always commit the change for that file if there's any dependency changes.


## Running locally

* Change all the URLs in the iron-ajax elements. Simply add "https://luci-config.appspot.com" before each URL.
  * One in src/config-ui/front-page.html
  * Two in src/config-ui/config-set.html
  * One in src/config-ui/config-ui.html
*	Run `make build` in the ui directory to build the ui app
*	In the config-service folder run `dev_appserver.py app.yaml`
*	Visit [http://localhost:8080](http://localhost:8080)


## Running Tests

Your application is already set up to be tested via [web-component-tester](https://github.com/Polymer/web-component-tester).
Run `make test` inside ui folder to run your application's test suites locally.
The command will run tests for all browsers installed on your computer.

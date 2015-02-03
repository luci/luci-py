Develop Polymer Web Components
==============================

Components should be built into files before deploy to production. Currently
there are two files needed in production: stats-app-build.html and
stats-overview-build.html. To generate them see the 'Build' section.


Setup
-----

  1. From http://nodejs.org/download/, download the relevant nodejs.
  2. Add path/to/node/bin to your PATH.
  3. Run the following.

    $ npm install -g bower
    $ bower install
    $ npm install -g vulcanize


Build
-----

    $ vulcanize --inline --csp -o stats-app-build.html stats-app.html
    $ vulcanize --inline --csp -o stats-overview-build.html stats-overview.html


Using non-vulcanized files
--------------------------

To develop locally, install Polymer and change the import statement in root.html
and stats.html from stats-app-build.html to stats-app.html, and from
stats-overview-build.html to stats-overview.html.

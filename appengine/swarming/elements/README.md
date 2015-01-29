Develop Polymer Web Components
==============================

Components should be built into files before deploy to production. Currently
there are two files needed in production: stats-app-build.html and
stats-overview-build.html. To generate them see the 'Build' section.

To develop locally, install polymer and change the import statement in root.html
and stats.html from stats-app-build.html to stats-app.html, and
stats-overview-build.html to stats-overview.html.

Install Polymer
---------------

  $ bower install


Install Vulcanize
-----------------

  $ npm install -g vulcanize


Build
-----

  $ vulcanize --inline --csp -o stats-app-build.html stats-app.html
  $ vulcanize --inline --csp -o stats-overview-build.html stats-overview.html


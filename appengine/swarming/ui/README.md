This contains all the Polymer 1.X elements used in swarming.

To clean and build the pages for deploying, run

    npm install   # Need to only run once to set up dev dependencies.
    make vulcanize

This combines all of the elements needed to display the page into several
"single-page" apps, like the bot-list.
These are checked into version control so that they may be easily redeployed w/o
having to rebuild the pages if there were no changes.


To do a full clean rebuild, run

    make clean_vulcanize


To vulcanize and run appengine locally, run

    make local_deploy


To run appengine locally without vulcanizing (preferred debugging mode), run

    make debug_local_deploy


To access the demo pages on localhost:9050, run

    make run


Prerequisites
=============

You will need to install a recent version of node.js, npm, and bower:

    sudo apt-get install npm nodejs-legacy
    # node and npm installed at this point are ancient, we need to update
    sudo npm install npm@latest -g
    # uninstall old npm
    sudo apt-get purge npm
    # make sure npm shows version 3.X or newer
    npm -v
    # you may need to add /usr/local/bin/npm to your superuser path
    # or just use /usr/local/bin/npm instead of npm below
    sudo npm cache clean -f
    sudo npm install -g n
    sudo n stable

    # should return 6.x or 7.x
    node -v

    sudo npm install -g bower


If you don't want to install npm globally, try

    echo prefix = ~/foo/bar >> ~/.npmrc
    curl https://www.npmjs.org/install.sh | sh
    ~/foo/bar/npm install -g bower
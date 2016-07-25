This contains all the Polymer 1.X elements used in swarming.

To clean and build the pages for deploying, run

    npm install   # Need to only run once to set up dev dependencies.
    make vulcanize

This combines all of the elements needed to display the page into several "single-page" apps, like
the bot-list.
These are checked into version control so that they may be easily redeployed w/o having to rebuild
the pages if there were no changes.


To do a full clean rebuild, run

    make clean_vulcanize


To access the demo pages on localhost:8080, run

    make && make run


You will need to install node.js, npm, and bower, for example:

    sudo apt-get install npm nodejs-legacy
    sudo npm install -g bower


If you don't want to install npm globally, try

    echo prefix = ~/foo/bar >> ~/.npmrc
    curl https://www.npmjs.org/install.sh | sh
    ~/foo/bar/npm install -g bower
# Epitaph for Polymer Swarming web UI

**Oct 2016 - April 2019**

Here (in version control history) lies the Polymer v1 version of the Swarming UI.
At its birth, it was a huge upgrade from the previous UI, which was
not much more than a raw JSON dump of the Datastore entities.

It allowed the UI to be interactive - the lists had dynamic columns, making finding relevent,
information easier; the bot and task pages were more compact and loaded faster.

See img/ for screenshots of the 4 main pages: Bot List, Bot Page, Task List, Task Page.

As time went on, this new shiny UI began to form patina. The Custom Elements V0 web spec (and others)
that Polymer v1 was built on was rejected by many web browsers in favor of Custom Elements V1.
Thus, the Polymer UI was destined to always run on polyfills.

The Polymer UI lacked any tests and became fragile and a burden to maintain as
new features were added on, sometimes breaking old features. Polymer did a lot of re-draws
 by "magic", based on observing when data objects would change. This "magic" didn't work well
for arrays of data, which Swarming has a lot of. Furthermore, the two-way bindings that made
small Polymer apps easy to write did not scale up well in terms of complexity.

Eventually, Polymer v1 reached end of life and an alternative was found based off of
[lit-html](https://lit-html.polymer-project.org/): a subcomponent of Polymer that did
one thing well - efficiently render and re-render HTML templates.
While porting the UI, tests were added and certain pages were given a facelift to aid
in usability. The final result was a faster, smaller, better tested, less complex UI.

Good bye Polymer UI. Thank you for being better than once was.
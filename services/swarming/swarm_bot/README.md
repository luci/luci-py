Swarming Server bot code
========================

Contains the code to run on the bots. This code has to be resilient enough to be
able to self-update in case of partial destruction. The user provided
start_slave.py should also actively try to self-register itself on startup if
desired.


Behavior
--------

When the bot starts, the first thing it looks up is if it is the primary copy.
If so, it immediately sets itself as swarming_bot.1.zip. Then it starts up
itself. When running as swarming_bot.1.zip and it needs to upgrade, it switches
to swarming_bot.2.zip. On the next upgrade, it goes back to .1.zip.

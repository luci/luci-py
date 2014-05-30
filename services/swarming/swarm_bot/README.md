Swarming Server bot code
========================

Contains the code to run on the bots. This code has to be resilient enough to be
able to self-update in case of partial destruction. The user provided
start_slave.py should also actively try to self-register itself on startup if
desired.

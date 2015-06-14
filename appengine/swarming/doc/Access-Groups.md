# Overview of the access controls

## Introduction

This is the list of the access control groups.


### swarming-users

Members of this group can:
  - Trigger a task.
  - Query tasks the user triggered and get results.
  - List the tasks they triggered.
  - Cancel their own tasks.

Members has limited visibility over the whole system.


### swarming-bots

Members of this group can:
  - Run swarming_bot.zip successfully to run a bot locally. VMs that runs the
    bot code must be in this group via IP whitelisting.
  - Trigger tasks and get results.


### swarming-privileged-users

Members of this group can do everything that swarming-users can do plus:
  - See other people's tasks.
  - See all the bots connected.


### swarming-admins

Members of this group can do everything that swarming-privileged-users can do
plus:
  - Cancel anyone's task.
  - Delete bots.
  - Update bootstrap.py and bot_config.py.

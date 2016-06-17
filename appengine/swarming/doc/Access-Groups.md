# Overview of the access controls

## Introduction

This is the list of the access control groups. By default in a fresh instance
all groups are empty and no one can do anything. Add relevant users and IPs in
the following groups. Make sure that users that have access to the swarming
server also have equivalent access to the isolate server.


## Format

You can refer to the whitelisted IPs using `bots:*` in one of the group. You can
refer to user accounts with `user@example.org` and can refer to all users in a
domain with `*@chromium.org`.


## Groups


### swarming-users

Members of this group can:

*   Trigger a task.
*   Query tasks the user triggered and get results.
*   List the tasks they triggered.
*   Cancel their own tasks.

Members has limited visibility over the whole system, cannot view other user
tasks or bots.

Make sure members of this group are also member of _isolate-access_.


### swarming-privileged-users

Members of this group can do everything that _swarming-users_ can do plus:

*   See other people's tasks.
*   See all the bots connected.


### swarming-admins

Members of this group can do everything that _swarming-privileged-users_ can do
plus:

*   Cancel anyone's task.
*   Delete bots.
*   Update _bootstrap.py_ and _bot_config.py_.

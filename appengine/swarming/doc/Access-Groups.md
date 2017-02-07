# Overview of the access controls

## Introduction

Access control in swarming is managed by roles that are defined by the
access control groups, and are managed by going to the `/auth` URL of
your app. In a fresh instance, group names default to
`administrators`, so only those can use the service.  To get started:

* Configure the group names for each role in the configuration file;
  see the
  [`config_service`](https://github.com/luci/luci-py/tree/master/appengine/config_service)
  for details, and AuthSettings in
  [proto/config.proto](../proto/config.proto) for the schema.
* Create the appropriate groups under `/auth`.
* Add relevant users and IPs to the groups.  Make sure that users who have
access to the swarming server also have equivalent access to the isolate server.


## Format

When specifying members of the auth groups, you can refer to the whitelisted IPs
using `bots:*`. For individual user accounts simply use their email,
e.g. `user@example.org`.  All users in a domain can be specified with a glob,
e.g. `*@chromium.org`.


## Groups

### `users_group`

Members of this group can:

*   Trigger a task.
*   Query tasks the user triggered and get results.
*   List the tasks they triggered.
*   Cancel their own tasks.

Members has limited visibility over the whole system, cannot view other user
tasks or bots.

Make sure members of this group are also member of `isolate-access`.

### `privileged_users_group`

Members of this group can do everything that members of the `users_group` can do
plus:

*   See other people's tasks.
*   See all the bots connected.

### `bot_bootstrap_group`

Members of this group can fetch swarming bot code and bootstrap bots.

### `admins_group`

Members of this group can do all of the above plus:

*   Cancel anyone's task.
*   Delete bots.
*   Update `bootstrap.py` and `bot_config.py`.

# Magic Values

Describes magic values on the swarming server

## Introduction

There are a few "magic" values in the isolate and swarming server. Also some
dimensions and state values have special meaning.


## client tools environment variables

  - `ISOLATE_SERVER` sets the default value of --isolate-server.
  - `ISOLATE_DEBUG` sets --verbose verbosity.
  - `SWARMING_SERVER` sets the default value for --swarming.


## run_isolated

`run_isolated.py/.zip` understands the following. These values should be set in
the `command` section of the .isolate file.

  - `${ISOLATED_OUTDIR}`: If found on command line argument, replaced by the
    temporary directory that is uploaded back to the server after the task
    execution. This causes `run_isolated` to print a `[run_isolated_out_hack]`
    statement after the task.
  - `${SWARMING_BOT_FILE}`: If found on command line argument, replaced by a
    file written to by the swarming bot's on_before_task() hook in the swarming
    server's custom bot_config.py. This is used by a swarming bot to communicate
    state of the bot to tasks.


## Swarming

### Task execution environment

When a Swarming bot is running a task, the following environment variables are
always set:

  - `SWARMING_HEADLESS=1` is always set.
  - `SWARMING_BOT_ID` is set to the bot id.
  - `SWARMING_TASK_ID` is set to the task id.

The following environment variables may be set to alter bot behavior:

  - `SWARMING_EXTERNAL_BOT_SETUP=1` disables bot_config.setup_bot hook.
  - `SWARMING_GRPC_PROXY=<url>` and `ISOLATED_GRPC_PROXY=<url>` override the
    equivalent value in the bot config.
  - `LUCI_GRPC_PROXY_VERBOSE` dumps out additional gRPC proxy information if set
    to a truthy value (e.g. `1`).
  - `LUCI_GRPC_PROXY_TLS_ROOTS=<file>` and points to a .crt file containing
    certificate authorities. `LUCI_GRPC_PROXY_TLS_OVERRIDE=<name>` specifies the
    name of the server in the certificate. These are useful for testing a gRPC
    proxy running on localhost but with TLS enabled. Unlike the `*_GRPC_PROXY`
    env vars, these are shared between Swarming and Isolated since they're only
    used in the limited case when you need to override TLS. See
    [/client/utils/grpc_proxy.py](../../../client/utils/grpc_proxy.py) for more
    information.
  - `SWARMING_BOT_ID` can be used to override hostname-based bot id with a
    custom value. Must be specified before swarming script is started. Note that
    this environment variable will be set even if it was not specified manually
    and will always contain the bot id used.


### dimensions

  - `id`: must be a single value in the list, which also must be unique. It's
    what uniquely identify the bot.
  - `quarantined`: if present, it specifies the bot self-quarantined, as it
    found out it needs manual sysadmin assistance before being able to accept
    any task. An example is that it doesn't enough free disk space.


### state

  - `cost_usd_hour`: reports the base cost of this bot in $USD/hour.
  - `lease_expiration_ts`: when set to an integer or floating point value,
    informs the server of the time (in UTC seconds since epoch) that the bot
    will disconnect from the server. The server will not allow the bot to
    reap any tasks projected to end after the bot disconnects.
  - `periodic_reboot_secs`: when set to a integer, instructs the server to send
    a reboot command after this period. The actual period is fuzzed with a 10%
    delta.
  - `quarantined`: has the same meaning as in `dimensions`. It's also
    supported as a state.
  - `bot_group_cfg_version`: version identifier of the server defined
    configuration (extract from bots.cfg), applied to the bot during initial
    handshake. The server will ask the bot to restart if this configuration
    changes.

### tags

  - `source_revision`: if present, it specifies the SCM revision related to the
    task.  This allows the UI to link to the relevant revision.
  - `source_repo`: if present, it is a url to the hosted SCM related to the
    task, with a %s where the revision should be placed.  This allows the UI
    to link to the relevant revision.

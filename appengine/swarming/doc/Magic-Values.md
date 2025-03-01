# Magic Values

Describes magic values on the Swarming server

## Introduction

There are a few "magic" values in the isolate and Swarming server. Also some
dimensions and state values have special meaning.


## client tool environment variables

For tools in luci-py/client, the following environment variables have effect:

*   `SWARMING_SERVER` sets the default value for --swarming.


## run_isolated

`run_isolated.py/.zip` understands the following. These values should be set in
either the `command` section of the .isolate file, or the values for the
environment variables passed in the command line with the `--env` option.

*   `${ISOLATED_OUTDIR}`: If found on command line argument or environment
    variable, it will be replaced by the temporary directory that is uploaded
    back to the server after the task execution.
*   `${SWARMING_BOT_FILE}`: If found on command line argument or environment
    variable, it will be replaced by a file written to by the Swarming bot's
    on_before_task() hook in the Swarming server's custom bot_config.py. This is
    used by a Swarming bot to communicate state of the bot to tasks.
*   `${SWARMING_TASK_ID}`: if found on the command line argument, it will be
    replaced by the SWARMING_TASK_ID value passed with the --env option.


## Swarming

### Bot

The bot exposes two different set of values, the dimensions and the states. The
dimensions are what is used for task selection, so they are very important. The
states are for monitoring purposes, thus are not strictly required but are
useful to report information about the bot like the amount of free disk space.


#### Bot environment variables

The following environment variables may be set by the administrator when
starting `swarming_bot.zip` to alter the bot's behavior:

*   `DOCKER_HOST_HOSTNAME` dumps the value of this env var to the bot's state
    under the `docker_host_hostname` field. Used to advertise the hostname of
    the host machine when the bot is running within a container.
*   `SWARMING_BOT_ID` can be used to override hostname-based bot ID with a
    custom value. Must be specified before Swarming script is started. Note that
    this environment variable will be set even if it was not specified manually
    and will always contain the bot ID used.
*   `SWARMING_EXTERNAL_BOT_SETUP=1` disables `bot_config.setup_bot()` hook.


#### Bot dimensions

**Required**:

*   `id`: **must** be a list with a single value, which also must be **unique**
    across the whole fleet. It's what uniquely identifies the bot so it's kinda
    important.
*   `pool`: **must** be a list with at least one value. Pools are used to
    provide some isolation between categories of tasks, and as a secondary
    access control mechanism. See
    [pools.proto](../proto/config/pools.proto)
    for more information.

**Optional**:

*   `quarantined`: if present, regardless of its value, it specifies the bot is
    self-quarantined. This means that the [self-health
    check](Bot.md#health-self_check) failed and the bot considers itself to be
    in bad shape to run tasks. This normally means the bot needs manual sysadmin
    assistance before being able to accept any task. An example is that it
    doesn't enough free disk space.
    *   This can also be used for temporary slow down, like if the device under
        test (DUT) is overheating, and letting it idle for a while will help it
        perform better (for example for performance testing).


#### Bot states

All *states* are optional!

*   `cost_usd_hour`: reports the base cost of this bot in $USD/hour. This is
    used to calculate task's cost.
*   `maintenance`: if present, regardless of its value, is similar to
    `quarantined` except that it means the bot is in temporary and expected
    self-maintenance state and doesn't need to be looked at. An example is that
    puppet is running.
*   `quarantined`: has the same meaning as in `dimensions`. It's also
    supported as a state. It's mere presence is the indicator.


### Task

#### Task runtime environment variables

When a Swarming bot is running a task, the following environment variables are
always set while running the task:

*   `SWARMING_HEADLESS=1` is always set.
*   `SWARMING_BOT_ID` is set to the bot ID.
*   `SWARMING_TASK_ID` is set to the task ID.


### Task dimensions

**Required**:

*   `id`: can be optional used with a list with exactly one value to select a
    specific bot.
*   `pool`: **must** be a list with exactly one value.


### Task tags

Task tags describe context about why the task was triggered. It can provide
metadata like `asan:1` to state that the test being run was built with Address
Sanitizer or `purpose:pre-commit` to differentiate between pre-commit and
post-commit tasks. The tags can then be leveraged in the Web UI to search for
specific tasks.

The tags are in `key:value` format but other than that, they are free form and
user chosen. A small subset of the tags have predefined meaning:

*   `allow_milo:1`: Tells Swarming Web UI to enable the [LUCI
    Milo](https://chromium.googlesource.com/infra/luci/luci-go/+/main/milo/)
    annotation processing support.
*   `source_revision`: if present, it specifies the SCM revision related to the
    task. This allows the UI to link to the relevant revision.
*   `source_repo`: if present, it is a url to the hosted SCM related to the
    task, with a %s where the revision should be placed. This allows the UI
    to link to the relevant revision.

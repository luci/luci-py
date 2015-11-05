# User guide

Trigger tasks and get results


## Introduction

This pages describe the CLI python client that uses the Swarming server REST
API. The server REST API can also be used directly if desired but it is not
documented here (TODO), please read the sources in the meantime.

`swarming.py` is the client side script to manage Swarming tasks at the command
line.

**Warning:** This doc is bound to become out of date. Here's one weird trick:
  - "`swarming.py help`" gives you all the help you need so only a quick
    overview is given here:


## Running a task synchronously

If you just want to run something remotely, you can use the `run` command. It is
going to block until the command has completely run remotely.

```
swarming.py run --swarming <host> --isolate-server <isolate_host> <isolated|hash>
```

The `<hash>` is what `isolate.py archive` gave you. See IsolatedUserGuide for
more information. A path to a `.isolated` file will work too.


## Describing a task

Swarming tasks are normally running a isolated tree directly via run_isolated. A
task is described by two sets of parameters;

  - The request metadata, e.g. who triggered it, when, tags, etc.
  - The request data, e.g. what command, what dimensions, environment variables
    if any, etc.

The dimensions are important. For example be clear upfront if you assume an
Intel processor, the OS distribution version, e.g. Windows-7-SP1 vs
Windows-Vista-SP2.


### Task idempotency

It's important to be very careful with the request data, as if the data is
deterministic (including the files themselves) it is possible to use the
`--idempotent` flag. This flag tells the server to **skip** the task if the
exact same command was run previously and succeeded. This basically means that
if you run the test twice and it succeeded, the second request is served the
results from the first request. _This saves a lot of time and infrastructure
usage._

For a task to be idempotent, it must depend on nothing else than the task
description which includes:

    - Isolated files mapped in.
    - Dimensions are uniquely describe the type of bot required; exact OS
      version, any other important detail that can affect the task output.

Other things of note are:

    - No access to any remote service. This include HTTP(S), DNS lookup, etc. No
      file can be 'downloaded' or 'uploaded' by the task. They must be mapped
      in, content addressed, up front. Results must be inside
      `${ISOLATED_OUTDIR}``.
      - This is also important from a performance PoV since run_isolate.py keeps
        a local content addressed cache.
    - No dependency on the time of the day or any other side-signal.

If any of the rule above does not hold, the task must *not* be marked as
idempotent since it is not reproducible by definition.


### Task behavior

Swarming is designed against internal Google test distribution mechanism. As
such, it has a few assumptions baked in. A task shall:

    - Open input files as read-only, never for write.
    - Write files only to these two locations:
      - The OS-specific temporary directory, e.g. `/tmp` or `%TEMP%` file files
        that are irrelevant after the task execution.
      - `${ISOLATED_OUTDIR}` for files that are the output of this task.


## Running a task asynchronously

The buildbot slaves uses `trigger` + `collect`, so they can do multiple things
simultaneously. The general idea is that you trigger all the tests you want to
run immediately, then collect the results.


### Triggering

Triggers a task and exits without waiting for it:
```
swarming.py trigger --swarming <host> --isolate-server <isolate_host> --task <name> <hash>
```

  - `<name>` is the name you want to give to the task, like "`base_unittests`".
  - `<hash>` is an `.isolated` hash.

Run `help trigger` for more information.


### Collecting results

Collects results for a previously triggered task. The results can be collected
multiple times without problem until they are expired on the server. This means
you can collect again data run from a job triggered via `run`.

```
swarming.py collect --swarming <host> <name>
```


## Querying bot states

`swarming.py query` returns state about the known bots. More APIs will be added,
like returning tasks, once needed by someone. In the meantime the web frontend
shows these.


## More info

The client tools are all self-documenting. Use "`swarming.py help`" for more
information.

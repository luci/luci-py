# Life of a bot

Swarming bot behavior has been deeply influenced by the primary author's
[maruel](https://github.com/maruel) work on buildbot and his previous failed
attempts to replace it over the span of several years. So its design was
effectively sketched out of anti-patterns observations.


## Anti patterns

Here's the laundry list of anti-patterns observed from out
[buildbot](https://buildbot.net/) slaves and other similar systems
([Jenkins](https://jenkins-ci.org/) and several other CI systems falls in the
same bucket) and how these were addressed. Don't take this as
_Jenkins/buildbot/etc is bad_, it's about redesigning the core functionality
from observed failure modes. The primary goal is to reduce maintenance work for
large (thousands of bots) fleet as we experienced it. As you can see, the list
is fairly large.

  - Bot management
    - Slave code made of multiple files (notable exception is Jenkins).
      - The Swarming bot is self-contained as a single executable.
    - Bot side configuration like config files deployed to the slave is used.
      Examples include the URL of the server used put in a file or local
      password file used to access third party remote services.
      - The Swarming bot has no configuration data except the executable itself
        which _embeds_ the configuration. All the configuration is done on the
        server, then incorporated inside the bot's zip.
      - A colleague is working on secret management.
    - Lack of self-awareness.
      - The Swarming bot knows by definition what its server is, since it is
        part of the bot zip itself. This removes the need to specify any one-off
        URL on the bot as a configuration data.
    - Lack of self-diagnosis.
      - The Swarming bot is able to do a health check and self-quarantine
        itself, effectively telling the server to *not* hand tasks to it.
    - Bot version fleet management, it's hard to know which slave runs which
      version of the code.
      - The Swarming bot version is the digest (SHA-1) of the content of its
        code and it is reported to the server so there's no ambiguity.
    - State management, the slave could create and delete local files.
      - The Swarming bot has almost no state. The _checkout_ is deleted after
        every single task.
    - Slaves required many python libraries or other third parties to be
      preinstalled.
      - The Swarming bot contains all the python libraries that are needed.
        The code is designed to not depend on OS specific libraries (like
        pywin32) and use raw calls instead to reduce the footprint.
  - API and deployment
    - Bot API versioning. Updating slaves and master (server) has to be done
      synchronously and/or the bot API has to be versioned.
      - The Swarming bot is always accessing the server via the versioned URL,
        e.g.  123-dot-app.appspot.com.
    - Building an executable to run on the slave.
      - Since it's all interpreted code and the server generates the zip on the
        fly, there's nothing to compile.
    - SCM/package management usage for distribution using deb/rpm, checking out
      a git repository or any other installation mean.
      - The Swarming bot code is downloaded as a single HTTPS GET from the
        server.
    - Slaves used unencrypted communication with no server verification.
      - Bot only use HTTPS with a valid TLS certificate.
    - The server is extensible and knows how to build.
      - The server knows nothing, it doesn't know what a build is, this removes
        all the need for bot<->server state communication.
  - Resilience
    - Unreliable code, slaves having an exception or any kind of internal error
      would cause it to shut down.
      - The Swarming bot sustains fairly broken bot code via internal
        compartmentalization, only the code up to the initial poll is critical,
        the rest can crash, the bot will survive up to the point of getting a
        new version.
      - The Swarming bot keeps multiple copies of itself on the host and
        alternates between file name while updating itself.
    - Complex slave side logic, for example it knows how to checkout via git.
      - The Swarming bot is as dumb as possible. No logic exists: it can only
        run a single command and returns its output. This reduces the risk of a
        logic error.
    - Long lived TCP connections.
      - Every bot connections are meant to be executed immediately and not last.
        There is no hanging connection which would break down for various
        reasons (e.g. RST packets).
    - APIs that cannot be retried, it has to succeeds on the first try.
      - All the bot API strive to be idempotent; that is, each call can be
        safely retried without side effect. For example, appending output always
        specify the current offset so retrying this request won't cause output
        corruption.


## Bot executable

The whole design is based on python's native zip support for executable via
`__main__.py`, it permits a completely self-contained executable that can do
everything via subcommands. The Swarming bot doesn't use any library that uses C
extension so the same swarming_bot.zip file can run on either Intel or ARM
architecture unchanged.


### Internal code layout

The zip is a subset of the files inside
[//appengine/swarming/swarming_bot/](../swarming_bot/), with symlinks resolved
and a few files ignored (mainly unit tests). The files are stored at their
respective relative location. The list of imported files is inside
[whitelist](../server/bot_archive.py). The code contains 3 broad sections split
as directories:

  - [api/](../swarming_bot/api/) contains the generic functions for OS
    abstraction and utilities that are accessible to
    [bot_config.py](../swarming_bot/config/bot_config.py). It is really meant to
    be used as a stdlib for bot_config.py.
  - [bot_code/](../swarming_bot/bot_code/) contains the bot logic. The main guts
    is [bot_main.py](../swarming_bot/bot_code/bot_main.py) for the bot process
    and [task_runner.py](../swarming_bot/bot_code/task_runner.py) for the
    process handling a single task.
  - [config/](../swarming_bot/config/) contains the files that are meant to be
    overidden. In practice, only
    [bot_config.py](../swarming_bot/config/bot_config.py) and
    [config.json](../swarming_bot/config/config.json) are kept there but
    replaced with server provided versions.


### Creation

The bot code is generated by the server itself. Boostrapping a bot is not meant
to be sourced by any other mean like side-loading a zip from another bot. It has
to be downloaded from the server.

Literally, doing a HTTP GET to `/bot_code` returns the complete self-contained
bot executable that is immediately usable as-is. It is as simple as that.


### Server tainting

To define the _property_ of the bot by the server, the server embeds a file
named [config.json](../swarming_bot/config/config.json) that records the server
version name used to generate the bot and the URL used to fetch the bot code.
Most importantly, [config.json](../swarming_bot/config/config.json) contains the
*URL of server*, so the bot knows which server to contact for execution.

The URL listed is the hostname that was used to do the HTTP GET request. That
is, if a user does `https://foo.com/bot_bot` and another does
`https://foo.com:443/bot_code`, the generated zips will be different and will
have different digests.


### Versioning

Except the very first bootstrap, all `swarming_bot.zip` download requests are
done via a versioned URL, which includes the expected digest (SHA-1) of the bot.
It has the form `/swarming/api/v1/bot/bot_code/<sha1 digest>` If there is a
digest mismatch, an error is reported to the server administrators.

Everytime the bot polls the server for more tasks, it sends along the request
its digest. If there's a mismath, the server tells the bot to self-update.
Updating is all done purely by digest, so there's no "forward" or "rolling
back", all changes are equal and content addressed.

This removes a lot of the stress of "rolling back a bad server upgrade", since
all changes are not backward or forward, they are content addressed.


## Configuration

Since configuration is all done on the server, the server need to store all
the configuration files that may be deployed to the bots. In practice, there is
only one configuration file that is embedded inside the bot,
[bot_config.py](../swarming_bot/config/bot_config.py).  This file is then
injected inside the zip every time a zip is requested.


### Hooks

[bot_config.py](../swarming_bot/config/bot_config.py) purely implements
callbacks, it is pure executable code without any predefined data form. These
callbacks are called at specific predefined times during the bot's lifetime and
are classified by a few categories via their prefix:

   - `on_*` are hooks that are called on events based on the task with obvious
     names: `on_bot_startup`, `on_bot_shutdown`, `on_before_task`,
     `on_after_task`.
   - `get_*` are functions that returns the bot's self-defined identity. These
     are reported to the server and fall in two categories: `get_dimensions`
     which returns the properties that are used as part of the task scheduler
     bot selection and `get_state` which is purely informational data about the
     state of the bot.
   - `setup_bot` is the only hook that is expected to modify its host. It is
     called on the initial startup and just before shutdown, to ensure the bot
     configured the host well so it will return properly upon host restart.


## Bootstrap helper

A [bootstrap.py](../swarming_bot/config/bootstrap.py) helper script can be added
to the server, which can be run ala `curl | bash` like mechanism but based on
python instead. It is useful to prepare the bot's environment even before the
bot is created. The reason python was used instead of bash is primarily due to
simplify the bootstrapping of Windows bots.

While it is hosted inside [config/](../swarming_bot/config/), it is not part of
the swarming bot zip.

*TODO(maruel):* bootstrap.py should be moved elsewhere for clarity.


## Behavior

When the bot starts, `swarming_bot.zip` immediately copies itself to
`swarming_bot.1.zip` and restarts itself. Then it dispatches the [internal
handler](../swarming_bot/__main__.py) based on the command line. The bot
supports many utility subcommands for diagnostics, including a `shell`
subcommand that can be used to query the internal bot state and `config` to dump
its `config.json`.


### bot_main.py

The primary use case of swarming_bot.zip is to defer to
[bot_main.py](../swarming_bot/bot_code/bot_main.py), which handles the
communication with the server. It polls the API and dispatch the 4 supported
server replies:


#### update

`update` is sent when there's a mismatch between what version the server expects
and what the bot declares as its digest. In general it is because the default
server version changed.

The bot duplicates itself as swarming_bot.1.zip and swarming_bot.2.zip with
alterning versions. This aleviates issues with partial download or broken bot
code. It requests the new bot code via its digest. The way the bot restarts
itself is slightly OS specfic, it exec() itself on POSIX systems but calls a
subprocess on Windows.


#### run

`run` is the command to run a task. The bot starts task_runner which handles the
request from there.

Run is the primary purpose of the bot; the server tells the bot to run a task.
To reduce the likelihood of bugs, bot_main starts a child process task_runner to
handle the task. task_runner contains all the logic to download, execute and
report back the task results. task_runner implements the timeouts. In addition,
bot_main implements its own timeout in case task_runner would fail in any way.

Inside the manifest describing the task to run, the server adds the versioned
URL of the server to use for the lifetime of the task, so that the bot is not
affected by default server version change while the bot is running. So while
config.json declares a server URL like `https://foo.com`, the manifest will tell
the bot to send all requests related to the task to `https://1234-dot-foo.com`.
This removes the need to version the bot API at all.

Only once a task is executed successfully that swarming_bot.zip is updated with
the current code. This assures that the bot code that is used as the default
version is good enough to go as far as updating then running a task
successsfully.


#### restart

`restart` is sent when the server decides the bot ran for too long and the host
should restart itself. The bot really just restarts the host and waits for
SIGTERM.


#### terminate

`terminate` is sent when `swarming.py terminate` was used to request the bot to
orderly shut itself down. The bot simply exits.


### task_runner.py

[task_runner.py](../swarming_bot/bot_code/task_runner.py) can run two types of
task: raw command or isolated task.

For raw command, it runs the command as is and streams the stdout out. Using
this assumes the bot state is known and that the command is preinstalled.

For isolated task, task_runner defers the actual task execution to
[run_isolated.py](../../../client/run_isolated.py).  task_runner knows how to
translate the isolated Swarming task to run_isolated command line arguments.

In any case, task_runner creates a temporary directory `work` to use as the
current working directory when running the child process.


### Isolated task

[run_isolated.py](../../../client/run_isolated.py) doesn't know about Swarming
at all, it only knows about the Isolate server. task_runner doesn't know about
isolate, it only knows how to stream stdout back to the Swarming server.

This clear separation of tasks permits much simpler and focused code.
task_runner purely handle the Swarming task management while run_isolated purely
handles the cache management and isolated file processing, including uploading
results back.

This also reduces the knowledge of Isolate as much as possible from the Swarming
bot. Only task_runner knows how to translate the isolated Swarming task to
run_isolated command line arguments.

The process tree ends up like this:

    bot_main -> task_runner -> run_isolated -> child_process


#### Isolated cache

What makes isolated testing efficient is the high cache hit rate. To ensure high
cache hit rate, a local content addressed read only cache must outlive each
task. task_runner tells run_isolated to use the same local cache directory at
each task that is outside the `work` directory.

While it is preferable to keep `cache` directory, it is safe to delete, it will
be recreated as needed from the isolate server.


### Event reporting

Meaningful events like when the bot starts, shutdown or updates are reported to
the server as bot events, so they can be listed on its page.


### Error reporting

Errors on the bot are reported to the server via a specific bot API; any
exception in the hooks or in the bot owns code will be sent to the server. A
process wide exception handler is also set to catch underwise unhandled
exceptions.

It is also possible to post errors manually directly from the bot_config.py file
via the Bot object that is passed as a parameter to every bot_config.py
callback.

Errors are reported as events but are also included in the server error
reports.


## Bot Directory layout

The base directory containing swarming_bot.zip doesn't contains a limited number
of files and directories, **EVERYTHING NOT LISTED THERE IS DELETED ON STARTUP**:

  - `*-cacert.pem` are certificate files to verify the SSL certificates. Sadly
    the python libraries used enforce the program to write this file to disk.
  - `cipd_cache/` is a version cache for CIPD packages.
  - `isolated_cache/` is the run_isolated cache. Deleting it causes the next
    task to download all the files instead of reusing whatever was previously
    downloaded.
  - `logs/` is the logs for all the processes. Deleting it is fine, the
    directory will be recreated.
  - `swarming.lck` is a lock file to prevent the bot from starting twice on the
    same host. If you want to run multiple bots on a single host, use multiple
    directories.
  - `swarming_bot.zip` is the _LKGBC_ (Last Known Good Bot Code), it is reset
    after an upgrade and a successful task execution.
  - `swarming_bot.1.zip` and `swarming_bot.2.zip` are the two 'partitions' used
    when the bot is running and self-updating.
  - `work/` is the temporary working directory created for each task then
    deleted. By definition it only exists for the lifetime of a single task, so
    it is deleted on bot startup if found.


### logs/

All logs are put in a subdirectory named `logs`. The logs are rotated by the
processes themselves. The logs are as follow:

  - `bot_config.log`: it is a misnomer, it's the log of the
    [__main__.py](../swarming_bot/__main__.py) process, before shelling out to
    bot_main.
  - `run_isolated.log`: logs output by
    [run_isolated.py](../../../client/run_isolated.py).
  - `swarming_bot.log`: logs output by
    [bot_main.py](../swarming_bot/bot_code/bot_main.py).
  - `task_runner.log`: logs output by
    [task_runner.py](../swarming_bot/bot_code/task_runner.py).
  - `task_runner_stdout.log`: stdout output from
    [task_runner.py](../swarming_bot/bot_code/task_runner.py). task_runner is
    not supposed to do any output so it can be analysed here for any internal
    failure that would cause a dump to stderr.

Logs are currently not upstreamed to the server.


## Time out and killing

Swarming supports two kind of timeouts: _hard_ and _I/O_ timeout.

  - hard timeout is the maximum amount of time a task can run in seconds.
  - I/O timeout is the maximum amount of time a task can be silent, that is, it
    doesn't write anything to stdout or stderr. This is used to detect hung task
    earlier than with the hard timeout.

Enforcement falls on different process:

  - [run_isolated.py](../../../client/run_isolated.py) implements hard timeout,
    which excludes the overhead of download and upload.
  - [task_runner.py](../swarming_bot/bot_code/task_runner.py) implements hard
    timeout for raw task. For isolated task, it let run_isolated enforce it.
  - [task_runner.py](../swarming_bot/bot_code/task_runner.py) implements I/O
    timeout, since it's the one handling the task's process output.
  - [bot_main.py](../swarming_bot/bot_code/bot_main.py) implements a last ditch
    timeout, in case something really wrong happens. When it triggers, this is
    considered an internal failure.


### Graceful termination, aka the SIGTERM and SIGKILL dance

When any process (task_runner, run_isolated or the task process being run) needs
to be terminated, either due to bot_main receiving a SIGTERM (normally due to
the host shutting down), SIGINT (Someone pressed Ctrl-C at the terminal) or due
a timeout that expired, the code tries to do an ordered shutdown:

  - A SIGTERM is sent. On Windows, CTRL_BREAK_EVENT is used.
  - A grace period is given to the child process to clean itself up and shut
    down. The default is usually 30 seconds but could be configured.
  - If the grace period expired and the child process still hasn't exited, a
    SIGKILL is sent. On Windows, TerminateProcess() is used.
  - The parent process waits for the child process to fully die since these
    signals are asynchronous on all OSes w.r.t. process termination.

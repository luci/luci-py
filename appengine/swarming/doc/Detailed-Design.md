# Detailed design

Lightweight distribution server with a legion of dumb bots.

**One line description**: Running all tests concurrently, not be impacted by network or device flakiness.


## Overview

### Server

The server runs on [AppEngine](https://developers.google.com/appengine/) and is
the only communication point for any client or bot that wants to use Swarming.
Clients send task requests to the server and receive a task id. It's up to the
client to poll the server to know when a task is done.

The server uses its DB to store the tasks, the bots' state and stdout from the
tasks. It exposes the web frontend UI, client JSON REST API and bot JSON API. It
uses OAuth2 (or optionally IP whitelisting) to authenticate clients.

Task requests have a set of dimensions associated to it. It's the server that
matches the request's dimensions to find a bot which has the same set of
dimensions.

The server serializes all its state in the DB. There is **no** continuously
running thread. This makes upgrading the server and rolling back trivial. When
the bot code is affected by the server upgrade, the bots seamlessly upgrade
after their currently running task completes.


### APIs

The communication of tasks between clients and the server, or the server and the
bots, is done through JSON REST API over HTTPS.

**All the JSON REST APIs are idempotent**. Retrying them in case of error is
always safe. This permits transparent retry in case of failure.

All APIs require authentication when relevant. Multiple access group levels can
be defined.

The bot JSON REST APIs and client JSON REST APIs are clearly separated;
`/swarming/api/v1/bot/...` and `/swarming/api/v1/client/...`.


### Server configuration

The server has a few major configuration points;
  - Authentication, usually deletaged to the auth_service instance.
  - [bootstrap.py](../swarming_bot/bootstrap.py) which permits automatic
    single-command bot bootstrapping.
  - [bot_config.py](../swarming_bot/bot_config.py)
    which permits Swarming server specific global configuration. It can hook
    what the bots do after running a task, their dimensions, etc. See the file
    itself for the APIs. The Bot interface is provided by
    [bot.py](../swarming/swarming_bot/bot.py).


### Tasks

Each task is represented by a TaskRequest and a TaskProperties described in
task_request.py. The TaskRequest represents the meta data about a task, who,
when, expiration timestamp, etc. The TaskProperties contains the actual details
to run a task, commands, environment variables, execution timeout, etc. This
separation makes it possible to dedupe the task requests when the exact same
`.isolated` file is ran multiple times, so that task-deduplication can be
eventually implemented.

A task also has a TaskResultSummary to describe its pending state and a tiny
TaskToRun entity for the actual scheduling. They are respectively defined in
task_result.py and task_to_run.py.

The task ID is the milliseconds since epoch plus low order random bits and the
last byte set to 0. The last byte is used to differentiate between each try.


#### Priority FIFO task queues

The server implements a Priority FIFO queue based on the creation timestamp of
request. The priority is a 0-255 value with lower is higher priority. The
priority enforces ordering, higher priority (lower value) tasks are run first.
Then tasks *with the same priority* are run in FIFO order.

Technically speaking, it's possible to do more elastic priority scheduling, like
old pending requests have their priority slowly increasing over time but the
code to implement this was not committed since there was no immediate need.


#### Assignment

When a bot polls the server for work, the server assigns the first available
matching task available.

Matching is done via the dimensions of the request vs the dimensions of the bot.
The bot must have all the dimensions listed on the request to be given the task.
For example, it could be "os=Windows-Vista-SP2; gpu=15ad:0405".

To make the proces efficient, the dimensions are MD5 hashed and only the first
32 bits are saved so integer comparison can be used. This greatly reduce the
size of the hot TaskToRun entities used for task scheduling and the amount of
memory necessary on the frontend server.

Once a bot is assigned to a task, a TaskRunResult is created. If the task is
actually a retry, multiple TaskRunResult can be created for a single
TaskRequest.


#### Task execution

During execution, the bot streams back the stdout and a heartbeat over HTTPS
requests every 10 seconds. This works around stable long-lived network
connectivity, as a failing HTTPS POST will simply be retried.


#### Task success

Swarming distributes tasks but it doesn't care much about the task exit code.
The only trick we use is that bots currently automatically reboot when the task
executed fails on desktop OSes. It fixes classes of issues especially on
Windows. Obviously this is disabled when multiple bots run on a single host
(TODO).


#### Orphaned task

If a task stops being updated by its bot after 5 minutes, a cron job will abort
the task with BOT_DIED. This condition is **masked by retrying the task
transparently on the next available bot**. Only **one** retry is permitted to
not overflow the infrastructure with potentially broken tasks.

If any part of the scheduling, execution or processing of results fails, this is
considered an infrastructure failure.


### Task deduplication

If a task is marked as idempotent, e.g. `--idempotent` is used, the client
certifies that the task do not have side effects. This means that running the
task twice shall return the same results (pending flakiness).

The way it works internally is by calculating the SHA256 of !TaskProperties when
marked as idempotent. When a !TaskResultSummary succeeds that was also
idempotent, it sets a property to tell that its values can be reused.

When a new request comes in, it looks for a !TaskResultSummary that has
properties_hash set. If it finds one, the results are reused as-is and served to
the client immediately, without ever scheduling a task.

**Efficient task deduplication requires a deterministic build and no side
effects in the tasks themselves**. On the other hand, it can results is large
infrastructure savings.


### Caveats of running on AppEngine

   - Reliability. The main caveat of running on AppEngine is that it is ~99.99%
     stable. A simple task scheduling services that is running on a single host
     would never have to care about this. This forces the code and client to be
     *extremely defensive*.
   - No "process" or "thread" makes simple things difficult; message passing has
     to be DB based, cannot be only in-memory. Have to use memcache instead of
     in-memory lookup, which causes some overhead.
   - No long lived TCP connections makes it hard to have push based design.
   - DB operations scale horizontally but are vertically slow.
   - It's pretty rare that MySQL or Postgres would save half of the entities in
     a DB.put_multi() call. AppEngine does this all the time.
   - Transactions have to be avoided as much as possible. This forces the DB
     schema to be of a particular style.
   - Increased latency due to polling based design.

We accepted these caveats as we found the benefits outweighed, and by far, the
caveats. The main issue has been coding defensively up-front, which represented
a sunk cost in coding time.


### Handling flakiness

Running on AppEngine forced Swarming to make every subsystem to support
flakiness;

   - The server tolerates DB failure. In this case, it usually returns HTTP 500.
   - The client and the bot handles HTTP 500 by automatically retrying with
     exponential backoff. This is fine because the REST APIs are safe to retry.
   - No long lived TCP connection is ever created, so a network switch reset or
     flakiness network conditions are transparently handled.
   - The task scheduler handles flaky bots by retrying the task when the bot
     stops sending heartbeats.


## Bot

Each Swarming bot is intended to be extremely dumb and replaceable. These
workers have a very limited understanding of the system and access the server
via a JSON API. Each bot polls the server for a task. If the server hands a
task, the bot runs the associated commands and then pipe the output back to the
server. Once done, it starts polling again.


### Bootstrapping

Only two basic assumptions are:

   - The bot must be able to access the server through HTTPS.
   - python 2.7 must be installed.

The bot's code is served directly from the server as a self-packaged
`swarming_bot.zip`. The server generates it on the fly and embeds its own URL in
it. The server can also optionally have a custom bootstrap.py to further
automate the bot bootstraping process.


### Self updating

The bot keeps itself up to date with what the server provides.

   - At each poll, the bot hands to the server the SHA256 of the contents of
     swarming_bot.zip. If it mismatches what the server expects, it is told to
     auto-update;
     - The bot downloads the new bot code to swarming_bot.2.zip or
       swarming_bot.1.zip, depending on the currently running version and
       alternates between both names.
   - swarming_bot.zip is generated by the server and includes 2 generated files:
      - bot_config.py is user-configurable and contains hooks to be run on bot
        startup, shutdown and also before and after task execution.
      - config.json contains the URL of the server itself.
   - When a bot runs a task, it locks itself into the server version it started
     the task with. This permits to do breaking bot API change safely. This
     implies two side-effects:
      - A server version must not be deleted on AppEngine until all bot locked
        into this version completed their task. It's normally below one hour.
      - A server shouldn't be updated in-place, in particular if it modifies the
        bot API. Use a new server version name when changing the server or bot
        code.

Since the bot version calculation is done solely by the hash, the bot will also
roll back to earlier versions if the server is rolled back. All the bot's code
is inside the zip, this greatly reduces issues like a partial update, update
failure when there's no free space available, etc.

The bot also keeps a LKGBC copy (Last Known Good Bot Code):

   - Upon startup, if the bot was executed via swarming_bot.zip;
     - It copies itself to swarming_bot.1.zip and starts itself back, e.g.
       execv().
   - After successfully running a task, it looks if swarming_bot.zip is not the
     same version as the currently running version, if so;
     - It copies itself (swarming.1.zip or swarming.2.zip) back to
       swarming_bot.zip. This permits that at the next VM reboot, the most
       recent LKGBC version will be used right away.

The bot code has been tested on Linux, Mac and Windows, Chrome OS' crouton and
Raspbian.


### Bot dimensions

The bot publishes a dictionary of *dimensions*, which is a dict(key,
list(values)), where each value can have multiple values. For example, a Windows
XP bot would have 'os': ['Windows-XP-SP3']('Windows',). This permits broad or
scoped selection of bot type.

For desktop OSes, it's about the OS and hardware properties. For devices, it's
about the device, not about the host driving it.

These "dimensions" are used to associate tasks with the bots. See below.


### Device Bot

For bots that represent a device (Android, iOS, !ChromeBook), multiple bots can
run on a single host (TODO), *one bot per connected device to the host*. Usually
via USB.

In the USB case, a prototype recipe to create
[udev](http://en.wikipedia.org/wiki/Udev) rules to fire up the bot upon
connection is included. The general idea is to reduce sysadmin overhead to its
minimum, configure the host once, then connect devices. No need to touch the
host again afterward. The server tolerates devices going Missing In Action or
the host rebooting, forcibly killing the on-going tasks. The server will retry
these task in this case, since it is an *infrastructure failure*.

The only sysadmin overhead remaining is to look for dead devices once in a while
via the client tools or server monitoring functionality.


## Client

Clients trigger tasks and requests results via a JSON REST API. It is not
possible for the client to access bots directly, no interactivity is provided by
design. The client code gives similar access to the Web UI.


### Requesting a task

When a client wishes to run something on swarming, they can use the REST API or
use the client script `swarming.py trigger`. It's a simple HTTPS POST with the
TaskRequest and TaskProperties serialized as JSON.

The format is described in `task_request.py` until the [API rewrite is
done](https://code.google.com/p/swarming/issues/detail?id=179).


### Task -> Bot assignment

The bot selection process is inveryed. It's the bot that polls for tasks. It
looks at all the products of all the `dimensions` it has and look at the oldest
task with highest priority that has `dimensions` which are also defined on the
bot.


## Stats

Statistics are generated at a minute resolution, with details like the number of
active bots, tasks, average pending time for the tasks triggered, etc. Details
per dimensions are also saved. The stats are generated directly from the logs by
a cron job in an immutable way. This means that once details are saved for a
minute, they are not updated afterward. This simplifies the DB load. Aggregate
views of hourly and daily resolutions are automatically generated via a cron
job.


## Security

APIs are XSRF token protected. AppEngine presents a valid SSL certificate that
is verified by both the bot and the client.


### Authentication

   - Web users are authenticated via AppEngine's native flow.
   - Client users are authenticated via OAuth2 flow.
   - Bots using the REST APIs can optionally be whitelisted by IPs.
   - The ACLs can be federated through a third party AppEngine instance. Design
     doc is at AuthDesign.


### Access control and Federation

The access control groups are optionally federated (TODO: Add specific page
about it) via services/auth_service via a master-replica model. This presents a
coherent view on all the associated services.


## Testing Plan

Swarming is tested by python tests in 4 ways:
   - Load tests, which was run in the few thousands bots and clients range.
   - Smoke tests before committing.
   - Unit tests.
   - Canarying on the chromium infrastructure, to ensure the code works before
     deploying to prod. CI live at
     http://build.chromium.org/p/chromium.swarm/waterfall

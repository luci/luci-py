# `LUCI_CONTEXT`

`LUCI_CONTEXT` is a generic way for LUCI services to pass contextual
information to each other. It has a very simple protocol:
  * Application writes a JSON file out (usually a temp file)
  * Application sets `LUCI_CONTEXT` environment variable to point to that file.
  * Subprocesses are expected to read contents from `LUCI_CONTEXT` in whole or
    part.
  * If any subprocess needs to add/modify information in the context, it copies
    the existing `LUCI_CONTEXT` entirely, then makes its modifications on the
    copy before writing it out to a different file (and updating the envvar
    appropriately.)

The `LUCI_CONTEXT` JSON file is always a JSON object (e.g. `{...}`), and
applications are cooperative in terms of the top-level keys (all known keys for
`LUCI_CONTEXT` and their meaning should be documented in this file). Every
top-level key also corresponds to a JSON object (never a primitive), to avoid
the temptation to pollute the top-level namespace with multiple
related-but-not-grouped data items.

No implementation should make the assumption that it knows the full set of keys
and/or schemas (hence the 'copy-and-modify' portion of the protocol).

Parents should keep any `LUCI_CONTEXT` files they write out alive for the
subprocess to read them (>= observable lifetime of the subprocess). If
a subprocess intends to outlive its parent, it MUST make its own copy of the
`LUCI_CONTEXT` file.

Example contents:

```json
{
  "local_auth": {
    "rpc_port": 10000,
    "secret": "aGVsbG8gd29ybGQK",
    ...
  },
  "swarming": {
    "secret_bytes": "cmFkaWNhbGx5IGNvb2wgc2VjcmV0IHN0dWZmCg=="
  },
  "luciexe": {
    "cache_dir": "/b/s/w/ir/cache"
  },
  "deadline": {
    "soft_deadline": 1600883265.1039423,
    "grace_period": 30
  }
}
```

# Library support
There is an easy-to-use library for accessing the contents of `LUCI_CONTEXT`, as
well as producing new contexts, located
[here][./libs/luci_context/luci_context.py].

# Known keys

For precision, the known keys should be documented with a block of protobuf
which, when encoded in jsonpb, result in the expected values. Implementations
will typically treat `LUCI_CONTEXT` as pure JSON, but we'd like to make the
implementation more rigorous in the future (hence the strict schema
descriptions). Currently implementing `LUCI_CONTEXT` in terms of actual
protobufs would be onerous, given the way that this repo is deployed and used.

It's assumed that all of the field names in the proto snippets below EXACTLY
correspond to their encoded JSON forms. When encoding in golang, this would be
equivalent to specifying the 'OrigName' parameter in the Marshaller.

## `local_auth`

Local auth specifies where subprocesses can obtain OAuth2 tokens to use when
calling other services. It is a reference to a local RPC port, along with
some configuration of what this RPC service (called "local auth service") can
provide.

```proto
message LocalAuth {
  message Account {
    string id = 1;
    string email = 2;
  }

  int rpc_port = 1;
  bytes secret = 2;

  repeated Account accounts = 3;
  string default_account_id = 4;
}
```

...

The returned tokens MUST have expiration duration longer than 150 sec. Clients
of the protocol rely on this.

...

The email may be a special string `"-"` which means tokens produced by the auth
server are not associated with any particular known email. This may happen when
using tokens that don't have `userinfo.email` OAuth scope.

...

TODO(vadimsh): Finish this.


## `swarming`

This section describes data passed down from the
[swarming service](../appengine/swarming) to scripts running within swarming.

```proto
message Swarming {
  // The user-supplied secret bytes specified for the task, if any. This can be
  // used to pass application or task-specific secret keys, JSON, etc. from the
  // task triggerer directly to the task. The bytes will not appear on any
  // swarming UI, or be visible to any users of the swarming service.
  byte secret_bytes = 1;
}
```

## `luciexe`

This section describes data passed from a `luciexe` host (e.g. Buildbucket's
agent in swarming).

```
message LUCIExe {
  // The absolute path of the base cache directory. This directory MAY be on the
  // same filesystem as CWD (but is not guaranteed to be). The available caches
  // are described in Buildbucket as CacheEntry messages.
  string cache_dir = 1;
}
```

## `realm`

This section describes data passed from LUCI Realms integration.

```proto
message Realm {
  // Realm name of the task.
  // e.g. infra:ci
  string name = 1;
}
```

## `resultdb`

This section describes data passed from ResultDB integrations.

```proto
message ResultDB {
  string hostname = 1; // e.g. results.api.cr.dev

  message Invocation {
    string name = 1;         // e.g. "invocations/build:1234567890"
    string update_token = 2; // required in all mutation requests
  }

  // The invocation in the current context.
  // For example, in a Buildbucket build context, it is the build's invocation.
  //
  // This is the recommended way to propagate invocation name and update token
  // to subprocesses.
  Invocation current_invocation = 1;
}
```

## `result_sink`

This section describes the ResultSink available in the environment.

```proto
message ResultSink {
  // TCP address (e.g. "localhost:62115") where a ResultSink pRPC server is hosted.
  string address = 1;

  // secret string required in all ResultSink requests in HTTP header
  // `Authorization: ResultSink <auth-token>`
  string auth_token = 2;
}
```

## `deadline`

The Deadline represents an externally-imposed termination criteria for the
process observing the `LUCI_CONTEXT`.

Additionally, this contains `grace_period` which can be used to communicate how
long the external process will allow for clean up once it sends
SIGTERM/Ctrl-Break.

Intermediate applications MUST NOT increase `soft_deadline` or `grace_period`.

If the entire Deadline is missing from `LUCI_CONTEXT`, it should be assumed to
be:
    {soft_deadline: infinity, grace_period: 30}

Intermediate applications can 'reserve' portions of `soft_deadline` and
`grace_period` by reducing them and then enforcing the reduced times.

*** note
**WARNING:** Reducing `soft_deadline` may adversely affect the parent process's
ability to accurately assess if `soft_deadline` was exceeded. This could affect
reporting indicators such as 'timeout occurred', because the child process may
terminate itself before the parent can send a signal and mark that it has done
so.

Most applications SHOULD only reserve time from `grace_period`. Those reserving
from `soft_deadline` should take care to ensure that timeout status will still
be accurately communicated to their parent process, if that's important for
the application.
***

```
message Deadline {
  // The soft deadline for execution for this context as a 'float' unix
  // timestamp (seconds past unix epoch). This is the same as python's
  // `time.time()` representation.
  //
  // If this value is set, processes SHOULD rely on their parent process
  // to send SIGTERM/Ctrl-Break at this time.
  //
  // Parent processes adjusting or setting `soft_deadline` MUST enforce it by
  // sending SIGTERM/Ctrl-Break as close to this time as possible, followed
  // by SIGKILL/Terminate after `grace_period` additional time.
  //
  // If `soft_deadline` is 0 consider there to be no stated deadline (i.e.
  // infinite).
  //
  // Processes reading this value can use it to determine if a timeout will
  // actually be honored; i.e. if the user asks for 30s to run a process, but
  // soft_deadline indicates an end in 10s, your program can react accordingly
  // (print warning, adjust user-requested timeout, refuse to run user's
  // process, etc.).
  //
  // Processes can also observe this value in conjunction with
  // receiving a signal (i.e. I got a signal after `soft_deadline` then I'm likely
  // in a timeout state).
  double soft_deadline = 1 [json_name = "soft_deadline"];

  // The amount of time (in fractional seconds), processes in this context have
  // time to react to a SIGTERM/Ctrl-Break before being SIGKILL/Terminated.
  //
  // If an intermediate process has a lot of cleanup work to do after its child
  // quits (e.g. flushing stats/writing output files/etc.) it SHOULD reduce this
  // value for the child process by an appropriate margin.
  double grace_period = 2 [json_name = "grace_period"];
}
```

## `buildbucket`

This section describes data to update a buildbucket build.

```proto
message Buildbucket {
  // Buildbucket host name.
  // E.g. cr-buildbucket.appspot.com.
  string hostname = 1 [json_name = "hostname"];

  // Token to use to schedule child builds for this build.
  string schedule_build_token = 2 [json_name = "schedule_build_token"];
}
```

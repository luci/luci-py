# Auth Service

Auth Service manages and distributes data and configuration used for
authorization decisions performed by services in a LUCI cluster. It is part
of the control plane, and as such it is **not directly involved** in authorizing
every request to LUCI. Auth Service merely tells other services how to do it,
and they do it themselves.

There's one Auth Service per LUCI cluster. When a new LUCI service joins the
cluster it is configured with the location of Auth Service, and Auth Service is
configured with the identity of the new service. This is one time setup. During
this initial bootstrap the new service receives the full snapshot of all
authorization configuration, which it can cache locally and start using right
away. Then the new service can either register a web hook to be called by Auth
Service when configuration changes, or it can start polling Auth Service for
updates itself.

Whenever authorization configuration changes (e.g. a group is updated), Auth
Service prepares a new snapshot of all configuration, makes it available to
all polling clients, and pushes it to all registered web hooks until they
acknowledge it.

If Auth Service goes down, everything remains operational, except the
authorization configuration becomes essentially "frozen" until Auth Service
comes back online and resumes distributing it.

[TOC]

## Configuration distributed by Auth Service (aka AuthDB)

This section describes what exactly is "data and configuration used for
authorization decisions".

See AuthDB message in
[replication.proto](../components/components/auth/proto/replication.proto) for
all details.

### Groups graph

Groups are how the lowest layer of ACLs is expressed in LUCI, e.g. a service
may authorize some action to members of some group. More complex access control
rules use groups as building blocks.

Each group has a name (global to the LUCI cluster), a list of identity strings
it includes directly (e.g. `user:alice@example.com`), a list of nested groups,
and a list of glob-like patterns to match against identity strings
(e.g. `user:*@example.com`).

An identity string encodes a principal that performs an action. It's the result
of authentication stage. It looks like `<type>:<id>` and can represent:
  * `user:<email>` - Google Accounts (end users and service accounts).
  * `anonymous:anonymous` - callers that didn't provide any credentials.
  * `project:<project>` - a LUCI service acting in a context of some LUCI
    project when calling some other LUCI service. **Work in progress**.
  * `bot:<hostname>` - used only by Swarming, individual bots pulling tasks.
  * `bot:whitelisted-ip` - callers authenticated exclusively through an IP
    allowlist. **Deprecated**.
  * `service:<app-id>` - GAE application authenticated via
    `X-Appengine-Inbound-Appid` header. **Deprecated**.

Note that various UIs and configs may omit `user:` prefix, it is implied if no
other prefix is provided. For example, `*@example.com` in UI actually means
`user:*@example.com`.


### IP allowlists

An IP allowlist as a named set of IPv4 and IPv6 addresses. They are primarily
used in Swarming when authorizing RPCs from bots. IP allowlists are defined in
`ip_allowlist.cfg` configuration file.


### OAuth client ID allowlist

This is a list of Google [OAuth client IDs] recognized by the LUCI cluster. It
lists various OAuth clients (standalone binaries, web apps, AppScripts, etc.)
that are allowed to send end-user OAuth access tokens to LUCI. OAuth client ID
allowlist is defined in `oauth.cfg` configuration file.

[OAuth client IDs]: https://www.oauth.com/oauth2-servers/client-registration/client-id-secret/


### Security configuration for internal LUCI RPCs.

These are various bits of configuration (defined partially in `oauth.cfg` and in
`security.cfg` config files) that are centrally distributed to services in
a LUCI cluster. Used to establish mutual trust between them.


## API surfaces

### Groups API

This is a REST API to examine and modify group graph. It is used primarily by
Auth Service's own web frontend (i.e. it is mostly used by humans). It is
documented [right there](https://chrome-infra-auth.appspot.com/auth/api).
Alternatively, read the
[source code](../components/components/auth/ui/rest_api.py). This API is
appropriate for modifying groups and for ad-hoc checks when debugging
access errors (and both these tasks can be done through the web UI, so there's
rarely a need to use this API directly).

Services that care about availability **must not** use this API for
authorization checks. It has no performance or availability guarantees. If you
use this API, and your service goes down because Auth Service is down, it is
**your fault**.

Instead services should use AuthDB replication (normally through a LUCI client
library such as [components.auth] and [go.chromium.org/luci/server/auth]) to
obtain and keep up-to-date the snapshot of all groups, and use it locally
without hitting Auth Service on every request. See next couple sections for more
information.

[components.auth]: ../components/components/auth
[go.chromium.org/luci/server]: https://godoc.org/go.chromium.org/luci/server
[go.chromium.org/luci/server/auth]: https://godoc.org/go.chromium.org/luci/server/auth


### AuthDB replication

All authorization configuration internally is stored in a single Cloud Datastore
entity group. This entity group has a revision number associated with it, which
is incremented with every transactional change (e.g. when groups are updated).

For every revision Auth Service creates a consistent snapshot of AuthDB at this
particular revision, signs it, uploads it to a preconfigured Google Storage
bucket, starts serving it over `/auth_service/api/v1/authdb/revisions/...`
endpoint, sends a PubSub notification to a preconfigured PubSub topic, and
finally uploads the snapshot (via POST HTTP request) to all registered web hooks
until all of them acknowledge it.

It is distributed in a such diverse way for backward compatibility with variety
of AuthDB client implementations (in historical order):
  * Web hooks are how Python GAE services consume AuthDB via [components.auth]
    client library.
  * PubSub and `/auth_service/api/v1/authdb/revisions/...` endpoint is how
    Go GAE services consume AuthDB via [go.chromium.org/luci/server/auth]
    client.
  * `/auth_service/api/v1/authdb/revisions/...` is also polled by Gerrit `cria/`
    groups plugin.
  * Google Storage dump is consumed by Go GKE services (that don't have
    non-ephemeral storage to cache AuthDB in). This is also done via
    [go.chromium.org/luci/server/auth] client configured by
    [go.chromium.org/luci/server].


### Hooking up a LUCI service to receive AuthDB updates {#linking}

In all cases the goal is to let Auth Service know the identity of a new service
and let the new service know the location of Auth Service. How this looks
depends on where the new service is running and what client library it uses.

Python GAE services should use [components.auth] library. Once the service is
deployed, perform this one-time setup:
  * If you are member of `administrators` group, open
    `https://<auth-service>.appspot.com/auth/services` in a browser, put app ID
    of the new service in `Add service` section and click `Generate linking
    URL`.
  * If you are not a member of `administrators` group, ask an administrator to
    do it on your behalf and share the resulting link with you.
  * If you are GAE admin of the new service, follow the link, it will ask for
    confirmation. Confirm. If it succeeds, you are done.
  * If you are not a GAE admin, ask the admin to visit the link.
  * During this process the new service receives an initial snapshot of AuthDB
    and registers a web hook to be called when AuthDB changes.

Go GAE services should use [go.chromium.org/luci/server/auth] and hooking them
to an Auth Service looks different:
  * Make sure Google Cloud Pub/Sub API is enabled in the cloud project that
    contains the new service.
  * Add GAE service account (`<appid>@appspot.gserviceaccount.com`) of the
    new service to `auth-trusted-services` group (or ask an administrator to do
    it for you).
  * As a GAE admin, open `https://<appid>.appspot.com/admin/portal/auth_service`
    and put `https://<auth-service>.appspot.com` into `Auth Service URL` field.
  * Click `Save Settings`. If it succeeds, you are done.
  * During this process the new service receives an initial snapshot of AuthDB,
    creates a new PubSub subscription, asks Auth Service to grant it access to
    the AuthDB notification PubSub topic and subscribes to this topic, so it
    knows when to refetch AuthDB from `/.../authdb/revisions/...` endpoint.

Go GKE/GCE services should use [go.chromium.org/luci/server]. There's no notion
of "GAE admin" for them, and no dynamic settings. Instead the configuration is
done through command line flags:
  * Add the service account of the new service to `auth-trusted-services` group
    (or ask an administrator to do it for you).
  * Pass `-auth-service-host <auth-service>.appspot.com` flag when launching the
    server binary.
  * If it starts and responds to `/healthz` checks, you are done.
  * Beneath the surface `-auth-service-host` is used to derive a Google Storage
    path to look for AuthDB snapshots (it can also be provided directly via
    `-auth-db-dump`). When server starts it tries to fetch AuthDB from there.
    On permission errors it asks Auth Service to grant it access to the AuthDB
    dump and tries again. This normally happens only on the first run. On
    subsequent runs, the new service doesn't send any RPCs to Auth Service
    **at all** and just periodically polls Google Storage bucket. Note that
    AuthDB is cached only in memory, but this is fine since we assume it is
    always possible to fetch it from Google Storage (i.e. Google Storage becomes
    a hard dependency, which has very high availability).


## Initial setup

...


## External dependencies

Auth Service depends on following services:
  * App Engine standard: the serving environment.
  * Cloud Datastore: storing the state (including groups graph).
  * Cloud PubSub: sending AuthDB update notifications to authorized clients.
  * Cloud Storage: saving AuthDB dumps to be consumed by authorized clients.
  * Cloud IAM: managing PubSub ACLs to allow authorized clients to subscribe.
  * LUCI Config: receiving own configuration files.

# Configuration service

  - Stores and imports config files from repositories, such as Gitiles.
  - Provides read-only access to config files and encapsulates their location.
  - Stores a registry of projects that use LUCI services.


## Quick examples

### Service config example
Auth service admins keep client id whitelist and configuration of group import
from externa sources. They can store these configs as files in Gitiles.
Config service can be configured to import them from Gitiles to
to `services/auth` config set. Auth service can use config component to
access its own configs.

As a result, Auth services has the following for free

  - convenient configuration viewing and editing by humans
  - change review and history
  - config ACLs


### Project config example

Project chromium is a tenant of Swarming service. Swarming is
chromum-independent therefore it does not contain configuration for chromium,
but chromium needs to supply its config to swarming. Chromium configs can be
stored in chromium repository and be imported into `projects/chromium` config
set. `projects.cfg` in `services/luci-config` config set contains a list of
projects served by LUCI services. For each project, Swarming uses config
component to read `swarming.cfg` from `projects/<project id>` config set.


## Terminology

  - **service**: project-independent (in particular, chromium-independent)
    multi-tenant reusable software. Examples: swarming, isolate, auth.
  - **project**: a tenant of a service. Examples: chromium, v8, skia.
  - **ref**: a git ref in a project repo.
  - **config set**: a versioned collection of config files. Config sets have
    names, for example: `services/chrome-infra-auth`, `projects/chromium`,
    `projects/chromium/refs/heads/master`. Config sets encapsulate location of
    files. Config service API accepts config sets instead of repository URLs.
    `services/luci-config:projects.cfg` means `projects.cfg` file in
    `services/luci-config` config set.

## Types of configs

There are three types of configs:

  1. Service configs. A service may have a global project-independent config.
    Example: auth service has a whiltelist of oauth2 client ids.
    These configs are generally not interesting to project maintainers.

    Service configs live in `services/<service_id>` config sets. For GAE apps,
    `service_id` is an app id.
    Examples: `services/luci-config`, `services/chrome-infra-auth`.
    A service typically reads config files in its own config set.

    `services/<service_id>` is always accessible to
    &lt;service-id&gt;.appspot.com.

    `services/luci-config:projects.cfg` is a project registry. It contains
    unique project ids (chromium, v8, skia) and location of project configs.
    This list is available through get_projects() API. This is how projects are
    discovered by services.

  2. Project configs. Project-wide branch-independent configs for services.
    This is what a project as a tenant tells a service about itself. Examples:

    - project metadata: project name, project description, mailing list,
      owner email, team auth group, wiki link, etc.
    - list of project refs.
    - cron jobs: when and what project tasks to run.

    Project configs live in `projects/<project_id>` config set. Services
    discover projects through `get_projects()` and request a config from
    `projects/<project_id>` config set. For instance, cron service reads
    `projects/<project_id>:cron.cfg` for each project in the registry.

  3. Ref configs. These are repository/branch-specific configs in a project.
    Examples:

    - list of builds that have to pass for a CL to be committed.
    - list of builder names that can close the tree if failed.
    - Code review info: type (rietveld, gerrit, etc), URL and
      codereview-specific details.

    Ref configs live in `projects/<project_id>/<ref_name>` config
    set, where `<ref_name>` always starts with `refs/`.


## GAE component

config component can be used by a GAE app to read configs.


## Config import

Configs are continuously imported from external sources to the datastore by
config service backend.
[Read more](https://github.com/luci/luci-py/wiki/Config-service:-config-import)

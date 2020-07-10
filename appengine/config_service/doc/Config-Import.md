# Config service: config file import

Configs are continuously imported from external sources to the datastore by
config server backend.  Requests to read a config never cause requests to
external sources, so failure of external sources cannot cause service outage.


## Gitiles import

A config set is mapped to a location in Gitiles, expressed as a
Gitiles-formatted URL. Example:

  ` projects/v8` -> https://chromium.googlesource.com/v8/v8/+/infra/config

means that files in `infra/config` branch of v8/v8 project will be accessible
in `projects/v8` config set.

1.  Service configs are imported from a single repository in Gitiles.
    The URL to the repository (`services_config_location`) is stored in the
    datastore and set once during GAE app setup through Admin API.

    Each child directory at `services_config_location` is treated as
    `<service_id>`. `services/<service_id>` is mapped to
    `<service_config_location>/<service_id>`.

2. `projects/<project_id>` config set mapping is specified in the
   `services/luci-config:projects.cfg`.


Note that services don't need to know location of configs, as long as they know
config set names.

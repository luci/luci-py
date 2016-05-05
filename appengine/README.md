appengine/
==========

This directory contains the AppEngine services needed for a LUCI infrastructure.

Services
--------

  - [auth_service/](auth_service) Authentication Server. It provides centralized
    group management and group database replication across services.
  - [config_service/](config_service) is a project configuration distribution
    server that supports importing from repositories.
  - [isolate/](isolate) Isolate Server is a Content-Addressed Cache running on
    AppEngine backed by Cloud Storage.
  - [machine_provider/](machine_provider) Machine provider is a service to
    lease VMs from a pool to users.
  - [swarming/](swarming) Swarming Server is a task distribution engine for
    highly hetegeneous fleet at high scale.

Supporting code
---------------

  - [components/](components) contains the modules shared by all services in
    this repository. This includes the embeddable part of auth_service to act as
    a client for auth_service, ereporter2, machine_provider, tooling for testing
    and deployment.
  - [third_party/](third_party) contains shared third parties. Services using
    these should symlink the packages inside the root server directory so it
    becomes available in sys.path.
  - [third_party_local/](third_party_local) constains testing or tooling related
    third parties that are not meant to be ever used on a AppEngine server.


Tooling
-------

All services can be managed with `./tools/gae`, including running locally or
pushing a new version. Use `./tools/gae help` for an up to date list of commands
available.

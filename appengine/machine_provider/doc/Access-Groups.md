# Overview of the access controls

## Introduction

This is the list of the access control groups. By default in a fresh instance
all groups are empty and no one can do anything. Add relevant users and IPs in
the following groups.


### machine-provider-catalog-administrators

Members of this group can perform the following for any backend service:

*   Add new machines.
*   Query the catalog for machines.
*   Update any CatalogEntry.
*   Modify the capacity of machines in the catalog.
*   Delete exsting machines.


### machine-provider-NAME-backend

The `NAME` identifier represents the name of the corresponding backend, such
as `gce`.

Members of this group can perform the following for backend service `NAME`:

*   Add new machines.
*   Query the catalog for machines.
*   Update any CatalogEntry.
*   Modify the capacity of machines in the catalog.
*   Delete exsting machines.


### Lease requests and releases

The user must be logged in to perform lease request and release operations.

GCE Backend
-----------

A GCE backend for Machine Provider.

GCE Backend is composed of a series of cron jobs. Each cron runs independently
of the others and performs idempotent operations. Many of the jobs are only
eventually consistent, converging on the desired state over multiple calls.

Setup
-----
Deploy an instance of the app, enable Pub/Sub and Compute Engine.

In Pub/Sub, create a topic with the same name as
pubsub.get\_machine\_provider\_topic(), and a pull subscription with the same
name as pubsub.get\_machine\_provider\_subscription(). On the topic, authorize
the Machine Provider's default service account as a publisher, e.g.
machine-provider@appspot.gserviceaccount.com.

Store the following three entities in the datastore:

config.Configuration:
Most of this entity will be updated by the app, but the config\_set field needs
to be set to the name of the config set to fetch from the config service.

components.config.common.ConfigSettings:
Set service\_hostname to the address of the config service, e.g.
luci-config.appspot.com. All others fields are unused.

components.machine\_provider.utils.MachineProviderConfiguration:
Set instance\_url to the URL for the Machine Provider service, e.g.
https://machine-provider.appspot.com.

Config
------
The configuration defines the desired instance templates, and the desired
instance group managers created from those templates. Configured in the config
service as templates.cfg and managers.cfg respectively.

import-config
-------------

Responsible for importing the current GCE configuration from the config service.
When a new config is detected, the local copy of the config in the datastore is
updated.

process-config
--------------

Responsible for creating an entity structure corresponding to the config.

A models.InstanceTemplate is created for each template defined in templates.cfg
which has a globally unique base\_name. Any reuse of base\_name, even when the
configured project is different, is considered to be another revision of the
same instance template. A revision of an instance template is one set of fields
for it, and a corresponding models.InstanceTemplateRevision is created. Each
time a template in templates.cfg with the same name has its other fields changed
a new models.InstanceTemplateRevision is created for it.

A models.InstanceGroupManager is created for each manager defined in
managers.cfg which refers to an existing template by template\_base\_name. At
most one instance group manager in each zone may exist for each template.

create-instance-templates
-------------------------

Ensures a GCE instance template exists for each template in the config. Updates
models.InstanceTemplateRevisions with the URL of the created instance template.

create-instance-group-managers
------------------------------

Ensures a GCE instance group manager exists for each manager in the config.
Updates models.InstanceGroupManagers with the URL of the created instance group
manager. Waits for an instance template to exist for the template the manager
is configured to use before attempting to create the instance group manager.

fetch-instances
---------------

Fetches the list of instances created by each instance group manager, creating
a models.Instance for each one. Waits for the instance group manager to exist
before attempting to fetch the list of instances.

catalog-instances
-----------------

Adds instances to the Machine Provider catalog. Any instance not cataloged and
not pending deletion is added to the catalog.

process-pubsub-messages
-----------------------

Handles Pub/Sub communication from the Machine Provider. Polls the pull
subscription for new messages regarding machines and operates accordingly. If
a machine is SUBSCRIBED, schedules an operation to update its metadata with the
subscription details. If a machine is RECLAIMED, sets it to pending deletion.

compress-instance-metadata-updates
----------------------------------

Compresses all pending\_metadata\_updates for a models.Instance into one single
update and sets it as the active\_metadata\_update if there isn't already one.

update-instance-metadata
------------------------

Looks for an active\_metadata\_update in a models.Instance and creates the GCE
zone operation to apply it. Updates models.Instance.active\_metadata\_update
with the URL of the scheduled operation.

check-instance-metadata-operations
----------------------------------

Looks for an active\_metadata\_update with a url in a models.Instance and checks
the status of the operation. If succeeded, active\_metadata\_update is cleared,
otherwise only the url is cleared and the operation is moved to the front of
the pending\_metadata\_updates.

delete-instances-pending-deletion
---------------------------------

Deletes GCE instances for each models.Instance with pending\_deletion set.


resize-instance-groups
----------------------

Embiggens a GCE managed instance group whose size has fallen below the minimum
configured size.


remove-cataloged-instances
--------------------------

Removes each models.Instance from the Machine Provider catalog that wasn't
created from an instance template currently referenced in the config.

delete-drained-instances
------------------------

Deletes GCE instances for each models.Instance that was removed from the
Machine Provider catalog for being created from an instance template not
currently referenced in the config.

delete-instance-group-managers
------------------------------

Deletes GCE instance group managers that aren't found in the config and have no
instances created from them.

delete-instance-templates
-------------------------

Deletes GCE instance templates that aren't found in the config and have no
instance group managers configured to use them.

cleanup-entities
----------------

Deletes entities that are no longer needed.

# GCE Backend

A GCE backend for Machine Provider.

GCE Backend is composed of a series of cron jobs. Each cron runs independently
of the others and performs idempotent operations. Many of the jobs are only
eventually consistent, converging on the desired state over multiple calls.


## Setting up

*   Visit http://console.cloud.google.com and create a project. Replace
    `<appid>` below with your project id.
*   Visit Google Cloud Console, IAM & Admin, click Add Member and add someone
    else so you can safely be hit by a bus.
*   Upload the code with: `./tools/gae upl -x -A <appid>`
*   Enable Pub/Sub and Compute Engine.
*   In Pub/Sub, create a topic with the same name as
    pubsub.get\_machine\_provider\_topic(), and a pull subscription with the
    same name as pubsub.get\_machine\_provider\_subscription(). On the topic,
    authorize the Machine Provider's default service account as a publisher,
    e.g. machine-provider@appspot.gserviceaccount.com.
*   Use the ConfigApi to set the ConfigSettings datastore entity to the
    address of the config service, e.g. luci-config.appspot.com.
*   Configure the Machine Provider with the
    [config service](https://github.com/luci/luci-py/blob/master/appengine/gce-backend/proto/config.proto)
    or the `instance\_url` property of the
    components.machine\_provider.utils.MachineProviderConfiguration entity in
    the datastore.
*   TODO(smut): Simplify the above.


TODO(smut): Move the following into doc/ as applicable.


## Config

The configuration defines the desired instance templates, and the desired
instance group managers created from those templates. Configured in the config
service as templates.cfg and managers.cfg respectively.


## import-config

Responsible for importing the current GCE configuration from the config service.
When a new config is detected, the local copy of the config in the datastore is
updated. This task ensures that the valid manager and template configurations
are updated synchronously.


## process-config

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


## create-instance-templates

Ensures a GCE instance template exists for each template in the config. Updates
models.InstanceTemplateRevisions with the URL of the created instance template.


## create-instance-group-managers

Ensures a GCE instance group manager exists for each manager in the config.
Updates models.InstanceGroupManagers with the URL of the created instance group
manager. Waits for an instance template to exist for the template the manager
is configured to use before attempting to create the instance group manager.


## fetch-instances

Fetches the list of instances created by each instance group manager, creating
a models.Instance for each one. Waits for the instance group manager to exist
before attempting to fetch the list of instances.


## catalog-instances

Adds instances to the Machine Provider catalog. Any instance not cataloged and
not pending deletion is added to the catalog.


## process-pubsub-messages

Handles Pub/Sub communication from the Machine Provider. Polls the pull
subscription for new messages regarding machines and operates accordingly. If
a machine is SUBSCRIBED, schedules an operation to update its metadata with the
subscription details. If a machine is RECLAIMED, sets it to pending deletion.

## schedule-metadata-tasks

Looks for an active\_metadata\_update with a url in a models.Instance and checks
the status of the operation. If succeeded, active\_metadata\_update is cleared,
otherwise only the url is cleared and the operation is moved to the front of
the pending\_metadata\_updates. If there is an active\_metadata\_update without
a url, creates the GCE zone operation to apply the metadata update. If there is
no active\_metadata\_update, compresses all pending\_metadata\_updates into a
single metadata update and sets it as the active\_metadata\_update.


## delete-instances-pending-deletion

Deletes GCE instances for each models.Instance with pending\_deletion set.


## resize-instance-groups

Embiggens a GCE managed instance group whose size has fallen below the minimum
configured size.


## remove-cataloged-instances

Removes each models.Instance from the Machine Provider catalog that wasn't
created from an instance template currently referenced in the config.


## delete-drained-instances

Deletes GCE instances for each models.Instance that was removed from the
Machine Provider catalog for being created from an instance template not
currently referenced in the config.


## delete-instance-group-managers

Deletes GCE instance group managers that aren't found in the config and have no
instances created from them.


## delete-instance-templates

Deletes GCE instance templates that aren't found in the config and have no
instance group managers configured to use them.


## cleanup-entities

Deletes entities that are no longer needed.

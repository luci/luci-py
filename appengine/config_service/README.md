# Configuration service

*   Stores and imports config files from repositories, such as Gitiles.
*   Provides read-only access to config files and encapsulates their location.
*   Stores a registry of LUCI services.
*   Stores a registry of projects that use LUCI services.

[Documentation](doc)


## Setting up

*   Visit http://console.cloud.google.com and create a project. Replace
    `<appid>` below with your project id.
*   Visit Google Cloud Console, IAM & Admin, click Add Member and add someone
    else so you can safely be hit by a bus.
*   Upload the code with: `./tools/gae upl -x -A <appid>`
*   Set the import location and type using the Administration API's
    `globalConfig` setting call:
    *    "_https://apis-explorer.appspot.com/apis-explorer/?base=https://\<appid\>.appspot.com/_ah/api#p/admin/v1/admin.globalConfig_"
    *   `services_config_location` specifies the source location.
    *   `services_config_storage_type` specifies the source type
         (e.g. GITILES).
*   If you plan to use an [auth_service](../auth_service),
    *   Make sure it is setup already.
    *   [Follow instructions
        here](../auth_service#linking-other-services-to-auth_service).
*   _else_
    *   Visit "_https://\<appid\>.appspot.com/auth/bootstrap_" and click
        `Proceed`.
*   TODO(nodir): Add more.

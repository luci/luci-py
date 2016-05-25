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
*   TODO(nodir): Add more.

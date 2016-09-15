# Machine Provider

Services that manages leasing VMs to clients based on desired characteristics.


## Setting up

*   Visit http://console.cloud.google.com and create a project. Replace
    `<appid>` below with your project id.
*   Visit Google Cloud Console,
    *   IAM & Admin, click `Add Member` and add someone else so you can safely
        be hit by a bus.
    *   IAM & Admin, change the role for `App Engine default service account`
        from `Editor` to `Owner`.
    *   Pub/Sub, click `Enable API`.
*   Upload the code with: `./tools/gae upl -x -A <appid>`
*   If you plan to use a [config service](../config_service),
    *   Make sure it is setup already.
    *   [Follow instruction
        here](../config_service/doc#linking-to-the-config-service).
*   If you plan to use an [auth_service](../auth_service),
    *   Make sure it is setup already.
    *   [Follow instructions
        here](../auth_service#linking-isolate-or-swarming-to-auth_service).
*   _else_
    *   Visit "_https://\<appid\>.appspot.com/auth/bootstrap_" and click
        `Proceed`.
*   TODO(smut): Add more.

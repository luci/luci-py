# Authentication Service

An AppEngine service used to import and manage ACL groups. It is to be used in
conjunction with the [auth component](../components/components/auth) to embed
replicated DB.

The authentication server provides a central control panel to declare every ACL
group and the whitelisted IPs. For example, which user has administrative
access, which can request tasks, which IP addresses can host bots, etc.

Each service have the authencation component embedded and will use the
standalone version by default. Using a central authentication service permits
not having to duplicate the ACLs, which is useful for larger scale
installations. For one-off experimentation, this is not strictly necessary.

[Documentation](doc/)


## Setting up

*   Visit http://console.cloud.google.com and create a project. Replace
    `<appid>` below with your project id.
*   Visit Google Cloud Console,
    *   IAM & Admin, click `Add Member` and add someone else so you can safely
        be hit by a bus.
    *   IAM & Admin, change the role for `App Engine default service account`
        from `Editor` to `Owner`.
    *   Pub/Sub, click `Enable API`.
        *   Click `Create a topic`.
        *   Name it "_auth-db-changed_", click `Create`.
*   Upload the code with: `./tools/gae upl -x -A <appid>`
    *   The very first upload may fail, try a second time.
*   Visit https://\<appid\>.appspot.com/auth/bootstrap and click Proceed.
*   Visit Google Cloud Console,
    *   API Manager, Credentials, OAuth consent screen:
        *   Type something in 'Product name shown to users'.
    *   API Manager, Credentials, click Create credentials:
        *   Choose `OAuth client ID`.
        *   Choose `Web application`, use name `service`, use `Authorized
            redirect URIs`,
            "_https://\<appid\>.appspot.com/auth/openid/callback_", click
            `Create`.
        *   In a new tab, visit https://\<appid\>.appspot.com/_ah/api/explorer,
            click `auth API`, click `auth.configure_openid`:
            *   Click `Request body`, set `client_id` and `client_secret` to the
                values from the previous tab, use for request_uri
                "_https://\<appid\>.appspot.com/auth/openid/callback_".
        *   TODO(vadimsh): Make UI to simplify this flow, e.g. visit
            https://\<appid\>.appspot.com/auth/oauth_config and add the service
            private data.
*   Wait up to 5 minutes.
*   Visit "_https://\<appid\>.appspot.com_" and make sure you can access the
    service before connecting [Isolate](../isolate) and [Swarming](../swarming)
    to this instance.
*   If you plan to use a [config service](../config_service),
    *   Make sure it is setup already.
    *   [Follow instruction
        here](../components/components/config/#linking-to-the-config-service).


### Linking other services to auth_service

*   Make sure your app is fully working.
*   Visit https://\<authid\>.appspot.com where \<authid\> is the auth_service
    instance to link with.
*   Type your \<appid\> in GAE application id and click Generate linking URL,
    where \<appid\> is the service being linked to the auth_service.
*   Click the link in the UI.
*   Click the red Switch button, understanding that any previous ACL
    configuration on this instance is lost.

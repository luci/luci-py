# Isolate Server

An AppEngine service to efficiently cache large set of large files over the
internet with high level of duplication. The cache is content addressed and uses
[Cloud Storage](https://cloud.google.com/storage/) as its backing store.

Isolate enables sending _temporary_ files around. It is a pure cache, _files_
will be deleted.

Isolate can be used standalone when only files need to be transfered but no task
scheduler is needed.

[Documentation](doc)


## Setting up

*   Visit http://console.cloud.google.com and create a project. Replace
    `<appid>` below with your project id.
*   Visit Google Cloud Console, IAM & Admin, click Add Member and add someone
    else so you can safely be hit by a bus.
*   Visit Google Cloud Console, IAM & Admin, Service accounts, click Create
    service account:
    *   Name it "_server@\<appid\>.iam.gserviceaccount.com_".
    *   Check "_Furnish a new private key_" and select "_P12_".
        *   TODO(vadimsh): switch to JSON keys.
    *   Click Create.
*   Visit Google Cloud Console, Storage, Create bucket with named with the same
    <appid>. Do not use any pre-created bucket, they won't work.
    *   Click on Browser on the left.
    *   Click the 3 dots on the right of your new bucket and select Edit bucket
        permission.
    *   Click Add item, chose _User_,
        "_server@\<appid\>.iam.gserviceaccount.com_", _Writer_.
    *   Click Save.
*   Upload the code with: `./tools/gae upl -x -A <appid>`
*   Visit https://\<appid\>.appspot.com/auth/bootstrap and click Proceed.
*   Visit https://\<appid\>.appspot.com/auth/groups:
    *   Create [access groups](doc/Access-Groups.md) as relevant. Visit the "_IP
        Whitelists_" tab and add bot external IP addresses if needed.
*   Visit https://\<appid\>.appspot.com/restricted/config
    *   Set "API access client email address" to
        "_server@\<appid\>.iam.gserviceaccount.com_".
    *   Follow the on-screen instructions to generate the base64 encoded DER
        private key.
    *   Click Submit.
*   Tweak settings:
    *   Visit Google Cloud Console, App Engine, Memcache, click "_Change_":
        *   Chose "_Dedicated_".
        *   Set the cache to Dedicated 5Gb.
        *   Wait a day of steady state usage.
        *   Set the limit to be lower than the value read at "Total cache size"
            in "Memcache Viewer".
    *   Visit Google Cloud Console, App Engine, Settings, click "_Edit_":
    *   Set Google login Cookie expiration to: 2 weeks, click Save.
*   Optionally [link with
    auth_service](../auth_service#linking-isolate-or-swarming-to-auth_service).
    Otherwise, you need to setup an oauth2 client token.

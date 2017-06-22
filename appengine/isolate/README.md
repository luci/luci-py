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
*   Visit Google Cloud Console
    *   IAM & Admin, click `Add Member` and add someone else so you can safely
        be hit by a bus.
    *   IAM & Admin, Service accounts, click `Create service account`:
        *   Name it "_server@\<appid\>.iam.gserviceaccount.com_".
        *   Check `Furnish a new private key` and select `P12`.
            *   TODO(vadimsh): switch to JSON keys.
        *   Click `Create`.
    *   Storage, click `Create bucket`, name it with the same <appid>. Do not
        use any pre-created bucket, they won't work.
        *   Click on Browser on the left.
        *   Click the 3 dots on the right of your new bucket and select `Edit
            bucket permissions`.
        *   Enter the user
            "_server@\<appid\>.iam.gserviceaccount.com_"
            in the `Add members` field and select `Storage Legacy Bucket Writer`
            and `Storage Legacy Object Reader` from the `Storage Legacy` group.
        *   Click `Add`.
    *   Pub/Sub, click `Enable API`.
*   Upload the code with: `./tools/gae upl -x -A <appid>`
*   If you plan to use an [auth_service](../auth_service),
    *   Make sure it is setup already.
    *   [Follow instructions
        here](../auth_service#linking-other-services-to-auth_service).
*   _else_
    *   Visit "_https://\<appid\>.appspot.com/auth/bootstrap_" and click
        Proceed.
*   Visit "_https://\<appid\>.appspot.com/auth/groups_":
    *   Create [access groups](doc/Access-Groups.md) as relevant. Visit the "_IP
        Whitelists_" tab and add bot external IP addresses if needed.
*   Visit "_https://\<appid\>.appspot.com/restricted/config_"
    *   Set "API access client email address" to
        "_server@\<appid\>.iam.gserviceaccount.com_".
    *   Follow the on-screen instructions to generate the base64 encoded DER
        private key.
    *   Click Submit.
*   Tweak settings:
    *   Visit Google Cloud Console
        *   App Engine, Memcache, click `Change`:
            *   Chose `Dedicated`.
            *   Set the cache to Dedicated 5Gb.
            *   Wait a day of steady state usage.
            *   Set the limit to be lower than the value read at "Total cache
                size" in "Memcache Viewer".
    *   App Engine, Settings, click `Edit`:
        *   Set Google login Cookie expiration to: 2 weeks, click Save.

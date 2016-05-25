# Swarming

An AppEngine service to do task scheduling on highly hetegeneous fleets at
medium (10000s) scale. It is focused to survive network with low reliability,
high latency while still having low bot maintenance, and no server maintenance
at all since it's running on AppEngine.

Swarming is purely a task scheduler service.

[Documentation](doc)


## Setting up

*   Visit http://console.cloud.google.com and create a project. Replace
    `<appid>` below with your project id.
*   Visit Google Cloud Console, IAM & Admin, click Add Member and add someone
    else so you can safely be hit by a bus.
*   Upload the code with: `./tools/gae upl -x -A <appid>`
*   Visit https://\<appid\>.appspot.com/auth/bootstrap and click Proceed.
*   Visit https://\<appid\>.appspot.com/auth/groups:
    *   Create [access groups](doc/Access-Groups.md) as relevant. Visit the "_IP
        Whitelists_" tab and add bot external IP addresses if needed.
*   Configure [bot_config.py](swarming_bot/config/bot_config.py) and
    [bootstrap.py](swarming_bot/config/bootstrap.py) as desired. Both are
    optional.
*   Visit https://\<appid\>.appspot.com and follow the instructions to start a
    bot.
*   Visit https://\<appid\>.appspot.com/restricted/bots to ensure the bot is
    alive.
*   Run one of the [examples in the client code](../../client/example).
*   Tweak settings:
    *   Visit Google Cloud Console, App Engine, Memcache, click "_Change_":
        *   Choose "_Dedicated_".
        *   Set the cache to Dedicated 1Gb.
    *   Visit Google Cloud Console, App Engine, Settings, click "_Edit_":
        *   Set Google login Cookie expiration to: 2 weeks, click Save.
*   Optionally [link with
    auth_service](../auth_service#linking-isolate-or-swarming-to-auth_service).
    Otherwise, you need to setup an oauth2 client token.


## Running locally

You can run a swarming+isolate local setup with:

    ./tools/start_servers.py

Then run a bot with:

    ./tools/start_bot.py http://localhost:9050

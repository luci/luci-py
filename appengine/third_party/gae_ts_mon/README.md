# Setting up timeseries monitoring on App Engine.

1.  Symlink this directory into your appengine app.

        cd infra/appengine/myapp
        ln -s ../../appengine_module/gae_ts_mon .

1.  Add the scheduled task to your `cron.yaml` file.  Create it if you don't
    have one already.

        cron:
        - description: Send ts_mon metrics
          url: /internal/cron/ts_mon/send
          schedule: every 1 minutes

1.  Include the URL handler for that scheduled task in your `app.yaml` file.

        includes:
        - gae_ts_mon  # handles /internal/cron/ts_mon/send

1.  Initialize the library in your request handler.

        import gae_ts_mon

        [...]

        app = webapp2.WSGIApplication(my_handlers)
        gae_ts_mon.initialize(app)

    You must do this in every top-level request handler that's listed in your
    app.yaml to ensure metrics are registered no matter which type of request
    an instance receives first.

1.  Give your app's service account permission to send metrics to the API.  You
    can find the name of your service account on the `Permissions` page of your
    project in the cloud console - it'll look something like
    `app-id@appspot.gserviceaccount.com`.  Add it as a "Publisher" of the
    "monacq" PubSub topic in the
    [chrome-infra-mon-pubsub project](https://pantheon.corp.google.com/project/chrome-infra-mon-pubsub/cloudpubsub/topicList)
    by selecting it from the list and clicking "Permissions". If you see an
    error "You do not have viewing permissions for the selected resource.", then
    please ask pgervais@chromium.org (AMER) or sergiyb@chromium.org (EMEA) to do
    it for you.

1.  You also need to enable the Google Cloud Pub/Sub API for your project if
    it's not enabled already.

You're done!  You can now use ts_mon metrics exactly as you normally would using
the infra_libs.ts_mon module. Here's a quick example, but see the
[timeseries monitoring docs](https://chrome-internal.googlesource.com/infra/infra_internal/+/master/doc/ts_mon.md)
for more information.

    from infra_libs import ts_mon

    class MyHandler(webapp2.RequestHandler):
      goats_teleported = ts_mon.CounterMetric('goats/teleported')

      def get(self):
        count = goat_teleporter.teleport()
        goats_teleported.increment(count)

        self.response.write('Teleported %d goats this time' % count)


## Appengine Modules

Multiple Appengine modules are fully supported - the module name will appear in
as `job_name` field in metrics when they are exported.

The scheduled task only needs to run in one module.

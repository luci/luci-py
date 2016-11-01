# config/

`config` is a library that provides centralized configuration management
functionality for webapp2 and Cloud Endpoints apps. Acts as a client
for [config_service](../../../config_service).

The library can work in two modes: remote and fs. In remote mode the library
fetches configs from a configuration service. In fs mode it uses file system
as a source of configs. The library defaults to fs mode if URL of the config
service is uknown.

## Remote mode

Remote mode allows updating configuration without re-deploying your application.
Unless `store_last_good=True` argument is passed (see below), `get*` functions
make a roundtrip to the config service and should not be used in the serving
code path.

Setup:

  - Include `endpoints` library in app.yaml.
  - Export `config.ConfigApi` Cloud Endpoints service.
  - Open the service with API Explorer, replace `<host>` with the hostname of
    your app:

    `https://apis-explorer.appspot.com/apis-explorer/?base=https://<host>/_ah/api#p/config/v1/config.settings`

    In the request body specify auth service's hostname (e.g.
    `luci-config.appspot.com`) and account (e.g.
    `user:luci-config@appspot.gserviceaccount.com`)
  - Tell the configure service that your service may fetch your configs by
    adding your service to `services.cfg` of the config service
  - Create a directory `<appid>` in the config service's config root directory
    and put global configuration files there.

### Storing last good config

Most `get*` functions have `store_last_good` parameter. Pass `True` to avoid
doing a roundtrip to the config service. Instead they are fetched from the
datastore and a cron job updates the config files in the background.

Setup:

  - Add to your `app.yaml` to enable the config component:

    ```
    includes:
    - components/config

    libraries:
    - name: jinja2
      version: "2.6"
    - name: webapp2
      version: "2.5.2"
    - name: webob
      version: "1.2.3"
    ```

  - Include `../third_party/` in `sys.path`

## FS mode

In this mode configuration files are read from the file system. It is convenient
for local development or when using a config service is an overkill.

The config files are read from
`<app root>/<config_set>/CONFIGS/<config file path>`, for example if your app
is `foo.appspot.com` and your global config file is called `settings.cfg`, it
will be read from `<app root>/services/foo/CONFIGS/settings.cfg`.

When the app is uploaded to the real AppEngine, the configs from your local
checkout are uploaded too. To update them, reupload your app.


## Config validation

Configuration service supports config validation at the import time.
For each file in a revision, the config service contacts the apps that claim
they can validate the file and if any of the apps returns an error, the whole
revision is rejected.

Use `config.validation` module to specify which config files your app can
validate and how to validate them. See the module docstring for more info.

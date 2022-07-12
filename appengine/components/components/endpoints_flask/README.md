# endpoints_flask/

`endpoints_flask` is a package which implements Cloud Endpoints v1 over
Flask routes by acting as an adapter. This is a copy of the `endpoints_webapp2` directory that has been modified to run using Flask instead.

## Usage

The adapter is a drop-in replacement. Simply replace your calls to
`endpoints.api_server` with calls to `endpoints_flask.api_server`.
You will need to update your app configuration in as well.

### Before

```py
import endpoints

def get_routes():
  return endpoints.api_server([
    MyService,
    MyOtherService,
  ])
```

### After

```py
import endpoints_flask

def get_routes():
  return endpoints_flask.api_server([
    MyService,
    MyOtherService,
  ])
```

Using the adapter creates discovery routes for all your services. The default
base path is `/api` (cf. `/_ah/spi` on Cloud Endpoints v1 and `/_ah/api` on
Cloud Endpoints v2) so you'll need to update `app.yaml` accordingly. You can
change the base path by supplying a `base_path` keyword argument to
`endpoints_flask.api_server`.

```py
return endpoints_flask.api_server([MyService], base_path='/custom/path')
```

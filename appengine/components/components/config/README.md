config/
=====

`config` is a library that provides centralized configuration management
functionality for webapp2 and Cloud Endpoints apps. Acts as a client
for [config_service](../../../config_service).

### To use it in your service

  - Add to your `app.yaml` to enable the config component:

```
includes:
- components/config

libraries:
- name: endpoints
  version: "1.0"
- name: jinja2
  version: "2.6"
- name: webapp2
  version: "2.5.2"
- name: webob
  version: "1.2.3"
```

  - Add to your `main.py`:

```
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(BASE_DIR, 'components', 'third_party'))
```

  - Update required configuration details in the config_service import
    location:
    - `services.cfg` for the config_service requires configuration for
      each <appid>.
    - a corresponding folder for each service specified by <appid>
      contains configuration unique to that service.
    - more details on configurations read by the config_service can be found
      in [config_service/doc](../../../config_service/doc).

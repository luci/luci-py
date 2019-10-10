#!/usr/bin/env python3
# Copyright 2019 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.
"""The sole purpose of this file is to import other sources files and make sure
they work in the python3 App Engine Standard environment.
"""

#  pylint: disable=unused-import

from flask import Flask

import concurrent.futures
from google.auth.transport import requests
from google.cloud import bigquery
from google.cloud import datastore
from google.cloud import ndb

# Can be imported:
from components import cipd
from components import decorators
from components import natsort
from components import protoutil

# Can't be imported:
#from components import auth
#from components import config
#from components import datastore_utils
#from components import endpoints_webapp2
#from components import ereporter2
#from components import gce
#from components import gerrit
#from components import gitiles
#from components import net
#from components import prpc
#from components import pubsub
#from components import stats_framework
#from components import template
#from components import utils

# TODO(maruel): Connect to the datastore.
#bigquery_client = bigquery.Client()
#datastore_client = datastore.Client()
#ndb_client = ndb.Client()

# If `entrypoint` is not defined in app.yaml, App Engine will look for an app
# called `app` in `main.py`.
app = Flask(__name__)


@app.route('/')
def hello():
  return 'Please try again!'


@app.route('/_ah/warmup')
def warmup():
  # Handle your warmup logic here, e.g. set up a database connection pool.
  return '', 200, {}


if __name__ == '__main__':
  # This is used when running locally only. When deploying to Google App Engine,
  # a webserver process such as Gunicorn will serve the app. This can be
  # configured by adding an `entrypoint` to app.yaml.
  app.run(host='127.0.0.1', port=8080, debug=True)
